package store

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const updatesSchema = `
CREATE TABLE IF NOT EXISTS chat_read_state (
 chat_jid TEXT PRIMARY KEY, unread_count INTEGER, marked_unread BOOLEAN NOT NULL DEFAULT 0,
 read_through INTEGER, action_time INTEGER NOT NULL DEFAULT 0, source TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS message_read_state (
 chat_jid TEXT NOT NULL, message_id TEXT NOT NULL, unread BOOLEAN NOT NULL,
 PRIMARY KEY(chat_jid,message_id));
CREATE TABLE IF NOT EXISTS local_updates (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, chat_jid TEXT NOT NULL,
 message_id TEXT NOT NULL, created_at INTEGER NOT NULL, UNIQUE(kind,chat_jid,message_id));
CREATE INDEX IF NOT EXISTS local_updates_chat ON local_updates(chat_jid,seq);
CREATE TABLE IF NOT EXISTS local_metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT OR IGNORE INTO local_metadata VALUES('updates_epoch',lower(hex(randomblob(8))));
`

// knownUnreadSQL never infers unread state from "incoming" alone. A synced
// read watermark or an explicitly observed unread identity is required.
const knownUnreadSQL = `messages.is_from_me=0 AND (
 EXISTS(SELECT 1 FROM message_read_state r WHERE r.chat_jid=messages.chat_jid AND r.message_id=messages.id AND r.unread=1)
 OR (EXISTS(SELECT 1 FROM chat_read_state s WHERE s.chat_jid=messages.chat_jid AND s.read_through IS NOT NULL AND unixepoch(messages.timestamp)>s.read_through)
 AND NOT EXISTS(SELECT 1 FROM message_read_state r WHERE r.chat_jid=messages.chat_jid AND r.message_id=messages.id AND r.unread=0)))`

type UnreadChat struct {
	JID           string `json:"chat_jid"`
	Name          string `json:"name"`
	Count         *int   `json:"unread_count"`
	KnownMessages int    `json:"known_unread_messages"`
	MarkedUnread  bool   `json:"marked_unread"`
	Status        string `json:"count_status"`
}
type UnreadOverview struct {
	HasMore      bool         `json:"has_more"`
	Chats        []UnreadChat `json:"chats"`
	UnknownChats int          `json:"chats_without_complete_counts"`
	Note         string       `json:"note"`
}

func (s *Store) UnreadChats(ctx context.Context, chat string, limit int) (UnreadOverview, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT c.jid,c.name,r.unread_count,COALESCE(r.marked_unread,0),
 (SELECT count(*) FROM messages WHERE messages.chat_jid=c.jid AND `+knownUnreadSQL+`)
 FROM chats c LEFT JOIN chat_read_state r ON r.chat_jid=c.jid WHERE (?='' OR c.jid=?) ORDER BY c.last_message_time DESC,c.jid`, chat, chat)
	if err != nil {
		return UnreadOverview{}, err
	}
	result := UnreadOverview{Chats: []UnreadChat{}, Note: "Counts can be unknown or partial until read state arrives. Known unread message identities may cover only part of a phone counter. Reading here does not mark anything read."}
	for rows.Next() {
		var item UnreadChat
		var count sql.NullInt64
		if err = rows.Scan(&item.JID, &item.Name, &count, &item.MarkedUnread, &item.KnownMessages); err != nil {
			rows.Close()
			return result, err
		}
		item.Status = "partial"
		if count.Valid {
			n := int(count.Int64)
			item.Count = &n
			item.Status = "synced_count"
		} else {
			result.UnknownChats++
		}
		if count.Int64 > 0 || item.KnownMessages > 0 || item.MarkedUnread {
			if len(result.Chats) < limit {
				result.Chats = append(result.Chats, item)
			} else {
				result.HasMore = true
			}
		}
	}
	err = rows.Err()
	rows.Close()
	for i := range result.Chats {
		result.Chats[i].Name = s.ChatDisplayName(result.Chats[i].JID, result.Chats[i].Name)
	}
	return result, err
}
func (s *Store) HasMessage(chat, id string) bool {
	var exists bool
	s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM messages WHERE chat_jid=? AND id=?)", chat, id).Scan(&exists)
	return exists
}
func (s *Store) SeedUnread(chat string, count *uint32, marked bool) error {
	var value any
	if count != nil {
		value = int(*count)
	}
	_, err := s.db.Exec(`INSERT OR IGNORE INTO chat_read_state(chat_jid,unread_count,marked_unread,source) VALUES(?,?,?,'history')`, chat, value, marked)
	return err
}
func (s *Store) ObserveIncoming(ctx context.Context, m Message) error {
	if m.IsFromMe {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO local_updates(kind,chat_jid,message_id,created_at) VALUES('message',?,?,?)`, m.ChatJID, m.ID, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return s.commitUpdate(tx)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_read_state(chat_jid,message_id,unread)
 VALUES(?,?,NOT EXISTS(SELECT 1 FROM chat_read_state WHERE chat_jid=? AND read_through>=?))
 ON CONFLICT(chat_jid,message_id) DO NOTHING`, m.ChatJID, m.ID, m.ChatJID, m.Timestamp.Unix())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO chat_read_state(chat_jid,unread_count,source) VALUES(?,NULL,'live')
 ON CONFLICT(chat_jid) DO UPDATE SET unread_count=CASE WHEN EXISTS(SELECT 1 FROM message_read_state WHERE chat_jid=? AND message_id=? AND unread=1) THEN unread_count+1 ELSE unread_count END`, m.ChatJID, m.ChatJID, m.ID)
	if err != nil {
		return err
	}
	return s.commitUpdate(tx)
}
func (s *Store) ObserveChatRead(ctx context.Context, chat string, read bool, through, action time.Time) error {
	if action.IsZero() {
		if through.IsZero() {
			return fmt.Errorf("read marker has no timestamp")
		}
		action = through
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO chats(jid,name,last_message_time) VALUES(?,'',?)", chat, time.Unix(0, 0).UTC()); err != nil {
		return err
	}
	var previous int64
	err = tx.QueryRowContext(ctx, "SELECT action_time FROM chat_read_state WHERE chat_jid=?", chat).Scan(&previous)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if previous > action.UnixMilli() {
		return nil
	}
	if read {
		if through.IsZero() {
			through = action
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO chat_read_state(chat_jid,unread_count,marked_unread,read_through,action_time,source) VALUES(?,NULL,0,?,?,'read_watermark')
 ON CONFLICT(chat_jid) DO UPDATE SET unread_count=NULL,marked_unread=0,read_through=MAX(COALESCE(chat_read_state.read_through,0),excluded.read_through),action_time=excluded.action_time,source='read_watermark'`, chat, through.Unix(), action.UnixMilli())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE message_read_state SET unread=0 WHERE chat_jid=? AND message_id IN (SELECT id FROM messages WHERE chat_jid=? AND unixepoch(timestamp)<=?)`, chat, chat, through.Unix())
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO chat_read_state(chat_jid,marked_unread,action_time,source) VALUES(?,1,?,'manual_marker') ON CONFLICT(chat_jid) DO UPDATE SET marked_unread=1,action_time=excluded.action_time`, chat, action.UnixMilli())
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO local_updates(kind,chat_jid,message_id,created_at) VALUES('read_state',?,?,?)`, chat, fmt.Sprintf("%d:%t", action.UnixMilli(), read), time.Now().UnixMilli())
	if err != nil {
		return err
	}
	return s.commitUpdate(tx)
}
func (s *Store) ObserveReadIDs(ctx context.Context, chat string, ids []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range ids {
		_, err = tx.ExecContext(ctx, `INSERT INTO message_read_state(chat_jid,message_id,unread) VALUES(?,?,0) ON CONFLICT(chat_jid,message_id) DO UPDATE SET unread=0`, chat, id)
		if err != nil {
			return err
		}
	}
	// Receipts do not necessarily carry every unread identity, so do not invent
	// a new phone counter by subtracting IDs whose prior state was unknown.
	_, err = tx.ExecContext(ctx, "UPDATE chat_read_state SET unread_count=NULL WHERE chat_jid=?", chat)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO local_updates(kind,chat_jid,message_id,created_at) VALUES('read_state',?,?,?)`, chat, strings.Join(ids, ","), time.Now().UnixMilli())
	if err != nil {
		return err
	}
	return s.commitUpdate(tx)
}

type LocalUpdate struct {
	Mentions   []Mention `json:"mentions,omitempty"`
	MentionsMe bool      `json:"mentions_me,omitempty"`
	Author     string    `json:"author,omitempty"`
	Time       string    `json:"time,omitempty"`
	Seq        int64     `json:"-"`
	Kind       string    `json:"type"`
	ChatJID    string    `json:"chat_jid"`
	MessageID  string    `json:"-"`
	Chat       string    `json:"chat"`
	Text       string    `json:"text,omitempty"`
}
type WaitResult struct {
	Connected *bool         `json:"connected,omitempty"`
	Status    string        `json:"status"`
	Cursor    string        `json:"cursor"`
	Updates   []LocalUpdate `json:"updates"`
}

func (s *Store) updateHead(ctx context.Context) (string, int64, error) {
	var epoch string
	var seq int64
	err := s.db.QueryRowContext(ctx, `SELECT value,(SELECT COALESCE(MAX(seq),0) FROM local_updates) FROM local_metadata WHERE key='updates_epoch'`).Scan(&epoch, &seq)
	return epoch, seq, err
}

// InfiniteWait keeps one tool call pending until data arrives or its caller cancels.
const InfiniteWait time.Duration = -1

func (s *Store) updateSignal() <-chan struct{} {
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	if s.updateWake == nil {
		s.updateWake = make(chan struct{})
	}
	return s.updateWake
}
func (s *Store) commitUpdate(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return err
	}
	s.updateMu.Lock()
	defer s.updateMu.Unlock()
	if s.updateWake != nil {
		close(s.updateWake)
	}
	s.updateWake = make(chan struct{})
	return nil
}

func (s *Store) WaitUpdates(ctx context.Context, chat, kind, cursor string, timeout time.Duration) (WaitResult, error) {
	epoch, head, err := s.updateHead(ctx)
	if err != nil {
		return WaitResult{}, err
	}
	after := head
	if cursor != "" {
		parts := strings.Split(cursor, ".")
		if len(parts) != 2 || parts[0] != epoch {
			return WaitResult{}, fmt.Errorf("cursor: invalid or belongs to another cache")
		}
		after, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || after < 0 || after > head {
			return WaitResult{}, fmt.Errorf("cursor: invalid position")
		}
	}
	if timeout < 0 && timeout != InfiniteWait {
		return WaitResult{}, fmt.Errorf("invalid wait duration")
	}
	waitCtx, cancel := context.WithCancel(ctx)
	if timeout != InfiniteWait {
		cancel()
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	result := WaitResult{Status: "timeout", Updates: []LocalUpdate{}}
	for {
		wake := s.updateSignal()
		_, head, err = s.updateHead(ctx)
		if err != nil {
			return result, err
		}
		rows, err := s.db.QueryContext(ctx, `SELECT seq,kind,chat_jid,message_id FROM local_updates WHERE seq>? AND seq<=? AND (?='' OR chat_jid=?) AND (?='any' OR kind=?) ORDER BY seq LIMIT 100`, after, head, chat, chat, kind, kind)
		if err != nil {
			return result, err
		}
		for rows.Next() {
			var u LocalUpdate
			if err = rows.Scan(&u.Seq, &u.Kind, &u.ChatJID, &u.MessageID); err != nil {
				rows.Close()
				return result, err
			}
			result.Updates = append(result.Updates, u)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return result, err
		}
		if len(result.Updates) > 0 {
			last := result.Updates[len(result.Updates)-1].Seq
			for i := range result.Updates {
				u := &result.Updates[i]
				u.Chat = s.ChatDisplayName(u.ChatJID, s.FindChatName(u.ChatJID))
				if u.Kind == "message" {
					message, e := s.GetMessageContext(ctx, u.MessageID, 0, 0, u.ChatJID)
					if e == nil {
						u.Author = s.GetSenderName(ctx, message.Message.Sender)
						u.Time = message.Message.Timestamp.In(time.Local).Format("2006-01-02 15:04 MST")
						u.Text, u.Mentions, u.MentionsMe = s.RenderMentions(ctx, message.Message, false)
						if message.Message.MediaType != "" {
							u.Text = "[" + message.Message.MediaType + "] " + u.Text
						}
					}
				}
			}
			result.Status = "updates"
			result.Cursor = fmt.Sprintf("%s.%d", epoch, last)
			return result, nil
		}
		after = head
		result.Cursor = fmt.Sprintf("%s.%d", epoch, after)
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			return result, nil
		case <-wake:
		}
	}
}
