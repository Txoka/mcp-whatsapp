package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const projectionSchema = `
CREATE TABLE IF NOT EXISTS projection_clock(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL);
INSERT OR IGNORE INTO projection_clock VALUES(1,0);
CREATE TABLE IF NOT EXISTS projection_events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,event_key TEXT NOT NULL UNIQUE,
 chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,kind TEXT NOT NULL,observed_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS message_body_state(chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,known BOOLEAN NOT NULL,PRIMARY KEY(chat_jid,message_id));
INSERT OR IGNORE INTO message_body_state(chat_jid,message_id,known)
 SELECT chat_jid,id,NOT(COALESCE(media_type,'') IN ('image','video','document') AND COALESCE(content,'')='') FROM messages WHERE id IS NOT NULL AND chat_jid IS NOT NULL;
CREATE TABLE IF NOT EXISTS message_conflicts(chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,PRIMARY KEY(chat_jid,message_id));
CREATE TABLE IF NOT EXISTS message_revisions (
 chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,revision_ms INTEGER NOT NULL,
 payload TEXT NOT NULL,PRIMARY KEY(chat_jid,message_id));
`

// MessageEvent is the normalized input to the transactional message projection.
// Event timestamps and message creation timestamps are deliberately separate.
type MessageEvent struct {
	Kind                                string
	Message                             Message
	ChatName                            string
	RevisionMS                          int64
	Incoming                            bool
	PollOptions                         []string
	MediaKey, FileSHA256, FileEncSHA256 []byte
	FileLength                          uint64
	ContentUnknown                      bool
	LegacySender                        string
}
type revisionPayload struct {
	Content     string
	MentionJIDs []string
}
type ApplyResult struct {
	Inserted, Updated, Suppressed int
	Watermark                     int64
}

type ChatSnapshot struct {
	JID, Name       string
	LastMessageTime time.Time
	UnreadCount     *uint32
	MarkedUnread    *bool
}

func (s *Store) ApplyMessages(ctx context.Context, events []MessageEvent) (ApplyResult, error) {
	return s.ApplyHistory(ctx, events, nil)
}
func (s *Store) applyPrepared(ctx context.Context, input projectionInput, inboxID string) (ApplyResult, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	events, chats := input.Events, input.Chats
	var result ApplyResult
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	for _, chat := range chats {
		if _, err = tx.ExecContext(ctx, `INSERT INTO chats(jid,name,last_message_time) VALUES(?,?,?) ON CONFLICT(jid) DO UPDATE SET name=COALESCE(NULLIF(chats.name,''),excluded.name),last_message_time=CASE WHEN chats.last_message_time IS NULL OR excluded.last_message_time>chats.last_message_time THEN excluded.last_message_time ELSE chats.last_message_time END`, chat.JID, chat.Name, chat.LastMessageTime.UTC()); err != nil {
			return ApplyResult{}, err
		}
		if chat.UnreadCount != nil || chat.MarkedUnread != nil {
			var count any
			if chat.UnreadCount != nil {
				count = int(*chat.UnreadCount)
			}
			marked := chat.MarkedUnread != nil && *chat.MarkedUnread
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO chat_read_state(chat_jid,unread_count,marked_unread,source) VALUES(?,?,?,'history')", chat.JID, count, marked); err != nil {
				return ApplyResult{}, err
			}
		}
	}
	for _, event := range events {
		if err = s.applyMessage(ctx, tx, event, &result); err != nil {
			return ApplyResult{}, err
		}
	}
	if err = tx.QueryRowContext(ctx, "SELECT version+1 FROM projection_clock WHERE id=1").Scan(&result.Watermark); err != nil {
		return ApplyResult{}, err
	}
	if input.OperationID != "" {
		if _, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='complete',payload='' WHERE id=?", input.OperationID); err != nil {
			return ApplyResult{}, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE operations SET state='applied',detail='accepted outgoing intent projected locally',updated_at=? WHERE id=?", time.Now().UnixMilli(), input.OperationID); err != nil {
			return ApplyResult{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE projection_inbox SET state='applied',payload='',watermark=? WHERE id=?", result.Watermark, inboxID); err != nil {
		return ApplyResult{}, err
	}
	if err = s.commitUpdate(tx); err != nil {
		return ApplyResult{}, err
	}
	return result, nil
}
func (s *Store) applyMessage(ctx context.Context, tx *sql.Tx, e MessageEvent, result *ApplyResult) error {
	m := e.Message
	if m.ChatJID == "" || m.ID == "" {
		return fmt.Errorf("message event requires exact chat and message IDs")
	}
	if e.Kind == "" {
		e.Kind = "message"
	}
	if e.Kind != "message" && e.Kind != "edit" && e.Kind != "revoke" && e.Kind != "delete_me" && e.Kind != "read_chat" && e.Kind != "read_ids" && e.Kind != "poll_vote" {
		return fmt.Errorf("unsupported message event %q", e.Kind)
	}
	if e.Kind == "message" && m.Content == "" && m.MediaType == "" {
		return nil
	}
	// Include payload and revision in the event identity; duplicates are stable
	// across transport retries and history/live delivery. Source is not a revision.
	m.Timestamp = m.Timestamp.UTC()
	identity, err := json.Marshal(struct {
		Kind     string
		Message  Message
		Revision int64
		Mentions []string
		ChatName string
	}{e.Kind, m, e.RevisionMS, m.MentionJIDs, e.ChatName})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%x", sha256.Sum256(identity))
	inserted, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO projection_events(event_key,chat_jid,message_id,kind,observed_at) VALUES(?,?,?,?,?)", key, m.ChatJID, m.ID, e.Kind, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	n, err := inserted.RowsAffected()
	if err != nil {
		return err
	}
	// Even duplicate message bodies can add missing sender/media metadata. The
	// projection rules below determine whether any message data may change.
	_ = n
	if e.Kind == "read_chat" {
		var state readChatEvent
		if err := json.Unmarshal([]byte(m.Content), &state); err != nil {
			return err
		}
		return observeChatReadTx(ctx, tx, m.ChatJID, state.Read, state.Through, state.Action)
	}
	if e.Kind == "read_ids" {
		var ids []string
		if err := json.Unmarshal([]byte(m.Content), &ids); err != nil {
			return err
		}
		return observeReadIDsTx(ctx, tx, m.ChatJID, ids)
	}
	if e.Kind == "revoke" || e.Kind == "delete_me" {
		if err := deleteMessageTx(ctx, tx, m.ChatJID, m.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "UPDATE operations SET state='applied',detail='observed matching deletion',updated_at=? WHERE kind=? AND chat_jid=? AND message_id=?", time.Now().UnixMilli(), e.Kind, m.ChatJID, m.ID)
		if err == nil {
			_, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='complete',payload='' WHERE id IN (SELECT id FROM operations WHERE state='applied' AND kind=? AND chat_jid=? AND message_id=?)", e.Kind, m.ChatJID, m.ID)
		}
		return err
	}
	var deleted bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM deleted_messages WHERE chat_jid=? AND message_id=?)", m.ChatJID, m.ID).Scan(&deleted); err != nil {
		return err
	}
	if deleted {
		result.Suppressed++
		return nil
	}
	if e.Kind == "poll_vote" {
		return applyPollVoteTx(ctx, tx, m)
	}
	var existing bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM messages WHERE chat_jid=? AND id=?)", m.ChatJID, m.ID).Scan(&existing); err != nil {
		return err
	}
	var revision int64
	var pending string
	err = tx.QueryRowContext(ctx, "SELECT revision_ms,payload FROM message_revisions WHERE chat_jid=? AND message_id=?", m.ChatJID, m.ID).Scan(&revision, &pending)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	mutation := messageProjection{ctx: ctx, tx: tx, event: e, result: result, existing: existing, revision: revision, pending: pending}
	if e.Kind == "edit" {
		return mutation.applyEdit()
	}
	return mutation.applyCreation()
}

type messageProjection struct {
	ctx      context.Context
	tx       *sql.Tx
	event    MessageEvent
	result   *ApplyResult
	existing bool
	revision int64
	pending  string
}

func (p messageProjection) applyEdit() error {
	ctx, tx, e, result, existing, revision, pending := p.ctx, p.tx, p.event, p.result, p.existing, p.revision, p.pending
	m := e.Message
	var err error

	if e.RevisionMS <= 0 {
		return fmt.Errorf("edit requires a protocol revision timestamp")
	}
	payload, err := json.Marshal(revisionPayload{m.Content, m.MentionJIDs})
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE operations SET state='applied',detail='observed matching edit',updated_at=? WHERE kind='edit' AND chat_jid=? AND message_id=? AND revision_ms=?", time.Now().UnixMilli(), m.ChatJID, m.ID, e.RevisionMS); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='complete',payload='' WHERE id IN (SELECT id FROM operations WHERE state='applied' AND kind='edit' AND chat_jid=? AND message_id=? AND revision_ms=?)", m.ChatJID, m.ID, e.RevisionMS); err != nil {
		return err
	}

	if revision <= e.RevisionMS {
		if _, err := tx.ExecContext(ctx, "DELETE FROM message_conflicts WHERE chat_jid=? AND message_id=?", m.ChatJID, m.ID); err != nil {
			return err
		}
	}
	if revision <= e.RevisionMS {
		if _, err = tx.ExecContext(ctx, "INSERT INTO message_body_state(chat_jid,message_id,known) VALUES(?,?,1) ON CONFLICT(chat_jid,message_id) DO UPDATE SET known=1", m.ChatJID, m.ID); err != nil {
			return err
		}
	}

	if revision > e.RevisionMS {
		result.Suppressed++
		return nil
	}
	if revision == e.RevisionMS && pending != "" {
		var prior revisionPayload
		if err = json.Unmarshal([]byte(pending), &prior); err != nil {
			return err
		}
		if prior.Content != m.Content {
			return fmt.Errorf("conflicting edit payloads at the same revision")
		}
		result.Suppressed++
		return nil
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO message_revisions(chat_jid,message_id,revision_ms,payload) VALUES(?,?,?,?) ON CONFLICT(chat_jid,message_id) DO UPDATE SET revision_ms=excluded.revision_ms,payload=excluded.payload", m.ChatJID, m.ID, e.RevisionMS, string(payload)); err != nil {
		return err
	}

	if !existing {
		return nil
	} // materialize only after the original exists
	if _, err = tx.ExecContext(ctx, "UPDATE messages SET content=? WHERE chat_jid=? AND id=?", m.Content, m.ChatJID, m.ID); err != nil {
		return err
	}
	if err = replaceMentions(ctx, tx, m); err != nil {
		return err
	}
	result.Updated++
	return nil
}
func (p messageProjection) applyCreation() error {
	ctx, tx, e, result, existing, revision, pending := p.ctx, p.tx, p.event, p.result, p.existing, p.revision, p.pending
	m := e.Message
	var err error

	if existing && revision == 0 && !e.ContentUnknown {
		var known bool
		err := tx.QueryRowContext(ctx, "SELECT known FROM message_body_state WHERE chat_jid=? AND message_id=?", m.ChatJID, m.ID).Scan(&known)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if !known {
			if _, err = tx.ExecContext(ctx, "UPDATE messages SET content=? WHERE chat_jid=? AND id=?", m.Content, m.ChatJID, m.ID); err != nil {
				return err
			}
			if err = replaceMentions(ctx, tx, m); err != nil {
				return err
			}
		}
	}
	if existing && revision == 0 && !e.ContentUnknown {
		var body string
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(content,'') FROM messages WHERE chat_jid=? AND id=?", m.ChatJID, m.ID).Scan(&body); err != nil {
			return err
		}
		if body != m.Content {
			if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO message_conflicts(chat_jid,message_id) VALUES(?,?)", m.ChatJID, m.ID); err != nil {
				return err
			}
		}

	}

	if m.Timestamp.IsZero() {
		return fmt.Errorf("message creation timestamp required")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO chats(jid,name,last_message_time) VALUES(?,?,?) ON CONFLICT(jid) DO UPDATE SET name=COALESCE(NULLIF(excluded.name,''),chats.name),last_message_time=CASE WHEN chats.last_message_time IS NULL OR excluded.last_message_time>chats.last_message_time THEN excluded.last_message_time ELSE chats.last_message_time END`, m.ChatJID, e.ChatName, m.Timestamp.UTC()); err != nil {
		return err
	}
	// Creation replay enriches metadata, but never replaces an observed body.
	// This protects existing edited/legacy rows when revision provenance is absent.
	_, err = tx.ExecContext(ctx, `INSERT INTO messages(id,chat_jid,sender,content,timestamp,is_from_me,media_type,filename,url,direct_path,media_key,file_sha256,file_enc_sha256,file_length)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id,chat_jid) DO UPDATE SET
 sender=CASE WHEN messages.sender IS NULL OR messages.sender='' OR (instr(messages.sender,'@')=0 AND messages.sender=? AND instr(excluded.sender,'@')>0) THEN excluded.sender ELSE messages.sender END,
 media_key=COALESCE(messages.media_key,excluded.media_key),file_sha256=COALESCE(messages.file_sha256,excluded.file_sha256),file_enc_sha256=COALESCE(messages.file_enc_sha256,excluded.file_enc_sha256),
 direct_path=COALESCE(NULLIF(messages.direct_path,''),excluded.direct_path),url=COALESCE(NULLIF(messages.url,''),excluded.url)`,
		m.ID, m.ChatJID, m.Sender, m.Content, m.Timestamp.UTC(), m.IsFromMe, m.MediaType, m.Filename, m.URL, m.DirectPath, e.MediaKey, e.FileSHA256, e.FileEncSHA256, e.FileLength, e.LegacySender)
	if err != nil {
		return err
	}
	known := !e.ContentUnknown || revision > 0
	if _, err = tx.ExecContext(ctx, "INSERT INTO message_body_state(chat_jid,message_id,known) VALUES(?,?,?) ON CONFLICT(chat_jid,message_id) DO UPDATE SET known=MAX(known,excluded.known)", m.ChatJID, m.ID, known); err != nil {
		return err
	}
	if !existing {
		result.Inserted++
		if revision > 0 {
			var edited revisionPayload
			if err = json.Unmarshal([]byte(pending), &edited); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "UPDATE messages SET content=? WHERE chat_jid=? AND id=?", edited.Content, m.ChatJID, m.ID); err != nil {
				return err
			}
			m.Content = edited.Content
			m.MentionJIDs = edited.MentionJIDs
		}
		if err = replaceMentions(ctx, tx, m); err != nil {
			return err
		}
	} else if revision == 0 && len(m.MentionJIDs) > 0 {
		raw, _ := json.Marshal(m.MentionJIDs)
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO message_mentions(chat_jid,message_id,jids_json) VALUES(?,?,?)", m.ChatJID, m.ID, string(raw)); err != nil {
			return err
		}
	}
	if len(e.PollOptions) > 0 {
		raw, _ := json.Marshal(e.PollOptions)
		if _, err = tx.ExecContext(ctx, "UPDATE messages SET poll_options_json=COALESCE(poll_options_json,?) WHERE chat_jid=? AND id=?", string(raw), m.ChatJID, m.ID); err != nil {
			return err
		}
	}
	if m.IsFromMe {
		if _, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='complete',payload='',server_time=? WHERE id=? AND chat_jid=?", m.Timestamp.UTC(), m.ID, m.ChatJID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE operations SET state='applied',detail='outgoing message projected',updated_at=? WHERE kind='send' AND chat_jid=? AND message_id=?", time.Now().UnixMilli(), m.ChatJID, m.ID); err != nil {
			return err
		}
	}
	if e.Incoming && !existing && !m.IsFromMe {
		return observeIncomingTx(ctx, tx, m)
	}
	return nil
}

func replaceMentions(ctx context.Context, tx *sql.Tx, m Message) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM message_mentions WHERE chat_jid=? AND message_id=?", m.ChatJID, m.ID); err != nil {
		return err
	}
	if len(m.MentionJIDs) == 0 {
		return nil
	}
	raw, err := json.Marshal(m.MentionJIDs)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO message_mentions(chat_jid,message_id,jids_json) VALUES(?,?,?)", m.ChatJID, m.ID, string(raw))
	return err
}
func deleteMessageTx(ctx context.Context, tx *sql.Tx, chat, id string) error {
	var unread int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE chat_jid=? AND id=? AND "+knownUnreadSQL, chat, id).Scan(&unread); err != nil {
		return err
	}
	queries := []struct {
		sql  string
		args []any
	}{
		{"INSERT OR IGNORE INTO deleted_messages(chat_jid,message_id) VALUES(?,?)", []any{chat, id}},
		{"DELETE FROM messages WHERE chat_jid=? AND id=?", []any{chat, id}},
		{"DELETE FROM message_mentions WHERE chat_jid=? AND message_id=?", []any{chat, id}},
		{"DELETE FROM message_read_state WHERE chat_jid=? AND message_id=?", []any{chat, id}},
		{"DELETE FROM poll_votes WHERE poll_chat_jid=? AND poll_message_id=?", []any{chat, id}},
		{"DELETE FROM message_revisions WHERE chat_jid=? AND message_id=?", []any{chat, id}},
		{"DELETE FROM message_conflicts WHERE chat_jid=? AND message_id=?", []any{chat, id}},
		{"DELETE FROM message_body_state WHERE chat_jid=? AND message_id=?", []any{chat, id}},
		{"DELETE FROM local_updates WHERE chat_jid=? AND message_id=? AND kind='message'", []any{chat, id}},
		{"UPDATE chat_read_state SET unread_count=MAX(0,unread_count-?) WHERE chat_jid=? AND unread_count IS NOT NULL", []any{unread, chat}},
		{"UPDATE chats SET last_message_time=(SELECT MAX(timestamp) FROM messages WHERE chat_jid=?) WHERE jid=?", []any{chat, chat}},
	}
	for _, q := range queries {
		if _, err := tx.ExecContext(ctx, q.sql, q.args...); err != nil {
			return err
		}
	}
	return nil
}
