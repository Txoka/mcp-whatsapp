package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// listMessageColumns is the 8-column projection shared by ListMessages and
// queryContextWindow.
const contentStatusSQL = "CASE WHEN EXISTS(SELECT 1 FROM message_conflicts conflict WHERE conflict.chat_jid=messages.chat_jid AND conflict.message_id=messages.id) THEN 'conflicting' WHEN EXISTS(SELECT 1 FROM message_body_state body WHERE body.chat_jid=messages.chat_jid AND body.message_id=messages.id AND body.known=0) THEN 'unknown' ELSE 'known' END"

const listMessageColumns = "messages.timestamp, messages.sender, chats.name, CASE WHEN EXISTS(SELECT 1 FROM message_conflicts conflict WHERE conflict.chat_jid=messages.chat_jid AND conflict.message_id=messages.id) THEN '[content unavailable: conflicting unversioned copies]' ELSE messages.content END, messages.is_from_me, chats.jid, messages.id, messages.media_type" + ", " + contentStatusSQL

// ListMessagesParams holds the filters accepted by ListMessages. Zero values
// mean "unset" except for Limit, which defaults to 20 when <= 0.
type ListMessagesParams struct {
	UnreadOnly     bool
	CursorTime     time.Time
	CursorChat     string
	CursorID       string
	After          time.Time
	Before         time.Time
	SenderPhone    string
	ChatJID        string
	Query          string
	Limit          int
	IncludeContext bool
	ContextBefore  int
	ContextAfter   int
}

// ListMessages returns messages matching the given filters, optionally expanded
// with surrounding context messages when IncludeContext is true.
func (s *Store) ListMessages(ctx context.Context, params ListMessagesParams) ([]Message, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 20
	}

	var (
		whereClauses []string
		args         []any
	)

	if params.UnreadOnly {
		whereClauses = append(whereClauses, knownUnreadSQL)
	}
	if !params.CursorTime.IsZero() {
		whereClauses = append(whereClauses, "(julianday(messages.timestamp) < julianday(?) OR (julianday(messages.timestamp) = julianday(?) AND (messages.chat_jid > ? OR (messages.chat_jid = ? AND messages.id < ?))))")
		args = append(args, params.CursorTime, params.CursorTime, params.CursorChat, params.CursorChat, params.CursorID)
	}
	if !params.After.IsZero() {
		whereClauses = append(whereClauses, "julianday(messages.timestamp) > julianday(?)")
		args = append(args, params.After.UTC())
	}
	if !params.Before.IsZero() {
		whereClauses = append(whereClauses, "julianday(messages.timestamp) < julianday(?)")
		args = append(args, params.Before.UTC())
	}
	if params.SenderPhone != "" {
		// Live/history ingestion stores bare users, while imported rows may
		// contain full JIDs. Match exact representations, never substrings.
		resolved := s.ResolveLIDToJID(params.SenderPhone)
		whereClauses = append(whereClauses, "messages.sender IN (?, ?, ?, ?, ?)")
		args = append(args, params.SenderPhone, jidUser(params.SenderPhone), resolved, jidUser(resolved), jidUser(resolved)+"@s.whatsapp.net")
	}
	if params.ChatJID != "" {
		whereClauses = append(whereClauses, "messages.chat_jid = ?")
		args = append(args, params.ChatJID)
	}
	if params.Query != "" {
		clauses := append([]string{}, whereClauses...)
		clauses = append(clauses, "EXISTS(SELECT 1 FROM message_conflicts conflict WHERE conflict.chat_jid=messages.chat_jid AND conflict.message_id=messages.id)")
		var conflicts int
		if err := s.reader(ctx).QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE "+strings.Join(clauses, " AND "), args...).Scan(&conflicts); err != nil {
			return nil, err
		}
		if conflicts > 0 {
			return nil, fmt.Errorf("search coverage unknown: conflicting unversioned message content in the requested range")
		}
	}
	if params.Query != "" {
		whereClauses = append(whereClauses, "LOWER(messages.content) LIKE LOWER(?)")
		args = append(args, "%"+params.Query+"%")
	}

	var b strings.Builder
	b.WriteString("SELECT " + listMessageColumns + " FROM messages")
	b.WriteString(" JOIN chats ON messages.chat_jid = chats.jid")
	if len(whereClauses) > 0 {
		b.WriteString(" WHERE ")
		b.WriteString(strings.Join(whereClauses, " AND "))
	}
	b.WriteString(" ORDER BY julianday(messages.timestamp) DESC, messages.chat_jid ASC, messages.id DESC")
	b.WriteString(" LIMIT ?")
	args = append(args, limit)

	rows, err := s.reader(ctx).QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var result []Message
	for rows.Next() {
		m, err := s.scanListMessageRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !params.IncludeContext || len(result) == 0 {
		return result, nil
	}

	before := params.ContextBefore
	if before <= 0 {
		before = 1
	}
	after := params.ContextAfter
	if after <= 0 {
		after = 1
	}

	// Context windows overlap. Keep one row per (chat, message), then restore
	// the same newest-first order as the unexpanded query. Scope each window
	// to its match's chat, even if another chat has the same message ID.
	var expanded []Message
	seen := make(map[[2]string]bool)
	appendUnique := func(messages []Message) {
		for _, message := range messages {
			key := [2]string{message.ChatJID, message.ID}
			if !seen[key] {
				seen[key] = true
				expanded = append(expanded, message)
			}
		}
	}
	for _, m := range result {
		prior, err := s.queryContextWindow(ctx, m.ChatJID, m.Timestamp, m.ID, before, false)
		if err != nil {
			return nil, err
		}
		next, err := s.queryContextWindow(ctx, m.ChatJID, m.Timestamp, m.ID, after, true)
		if err != nil {
			return nil, err
		}
		appendUnique(prior)
		appendUnique([]Message{m})
		appendUnique(next)
	}
	sort.Slice(expanded, func(i, j int) bool {
		if !expanded[i].Timestamp.Equal(expanded[j].Timestamp) {
			return expanded[i].Timestamp.After(expanded[j].Timestamp)
		}
		if expanded[i].ChatJID != expanded[j].ChatJID {
			return expanded[i].ChatJID < expanded[j].ChatJID
		}
		return expanded[i].ID > expanded[j].ID
	})
	return expanded, nil
}

// ListChats returns chats matching query. sortBy is "last_active" (default) or "name".
func (s *Store) ListChats(ctx context.Context, query string, limit, page int, includeLastMessage bool, sortBy string) ([]Chat, error) {
	if limit <= 0 {
		limit = 20
	}
	if page < 0 {
		page = 0
	}

	var b strings.Builder
	b.WriteString(chatSelectSQL("chats", includeLastMessage))

	var args []any
	if query != "" {
		b.WriteString(" WHERE (LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
		args = append(args, "%"+query+"%", "%"+query+"%")
	}

	if sortBy == "name" {
		b.WriteString(" ORDER BY chats.name, chats.jid")
	} else {
		b.WriteString(" ORDER BY chats.last_message_time DESC, chats.jid")
	}

	b.WriteString(" LIMIT ? OFFSET ?")
	args = append(args, limit, page*limit)

	rows, err := s.reader(ctx).QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list chats: %w", err)
	}
	defer rows.Close()

	var result []Chat
	for rows.Next() {
		c, err := scanChatRow(rows)
		if err != nil {
			return nil, err
		}
		c.Name = s.ChatDisplayName(c.JID, c.Name)
		c.PhoneNumber = s.ResolveJIDToPhone(c.JID)
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// GetChat returns a single chat by JID. Returns (nil, nil) when not found.
func (s *Store) GetChat(ctx context.Context, jid string, includeLastMessage bool) (*Chat, error) {
	var b strings.Builder
	b.WriteString(chatSelectSQL("c", includeLastMessage))
	b.WriteString(" WHERE c.jid = ?")

	row := s.reader(ctx).QueryRowContext(ctx, b.String(), jid)
	c, err := scanChatRow(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get chat: %w", err)
	}
	c.Name = s.ChatDisplayName(c.JID, c.Name)
	c.PhoneNumber = s.ResolveJIDToPhone(c.JID)
	return &c, nil
}

// SearchContacts does a LIKE search over chats (excluding groups) and the
// synced whatsmeow_contacts address book, and returns up to 50 contacts
// ordered by name, jid. Address-book names win over chat names, which for
// 1:1 chats are often just the bare phone number.
func (s *Store) SearchContacts(ctx context.Context, query string) ([]Contact, error) {
	pattern := "%" + query + "%"
	rows, err := s.reader(ctx).QueryContext(ctx, `
		SELECT DISTINCT jid, name
		FROM chats
		WHERE (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
		  AND jid NOT LIKE '%@g.us'
		  AND jid NOT LIKE '%@lid'
		ORDER BY name, jid
		LIMIT 50`, pattern, pattern)
	if err != nil {
		return nil, fmt.Errorf("search contacts: %w", err)
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		var jid string
		var name sql.NullString
		if err := rows.Scan(&jid, &name); err != nil {
			return nil, err
		}
		result = append(result, Contact{
			PhoneNumber: jidUser(jid),
			Name:        name.String,
			JID:         jid,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Merge in address-book matches (issue #21: saved names live in
	// whatsmeow_contacts, not chats).
	names := s.searchContactNames(pattern)
	seen := make(map[string]bool, len(result))
	for i, c := range result {
		seen[c.JID] = true
		if n := names[c.JID]; n != "" {
			result[i].Name = n
		}
	}
	for jid, name := range names {
		if seen[jid] {
			continue
		}
		result = append(result, Contact{PhoneNumber: jidUser(jid), Name: name, JID: jid})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].JID < result[j].JID
	})
	if len(result) > 50 {
		result = result[:50]
	}
	return result, nil
}

// GetMessageContext returns the target message together with before/after
// windows from the same chat.
func (s *Store) GetMessageContext(ctx context.Context, messageID string, before, after int, chatFilter ...string) (*MessageContext, error) {
	filter := ""
	if len(chatFilter) > 0 {
		filter = chatFilter[0]
	}
	var count int
	if err := s.reader(ctx).QueryRowContext(ctx, "SELECT COUNT(*) FROM messages WHERE id=? AND (?='' OR chat_jid=?)", messageID, filter, filter).Scan(&count); err != nil {
		return nil, err
	}
	if count > 1 {
		return nil, fmt.Errorf("message ID is ambiguous; provide chat_jid")
	}
	row := s.reader(ctx).QueryRowContext(ctx, `
		SELECT messages.timestamp, messages.sender, chats.name, CASE WHEN EXISTS(SELECT 1 FROM message_conflicts conflict WHERE conflict.chat_jid=messages.chat_jid AND conflict.message_id=messages.id) THEN '[content unavailable: conflicting unversioned copies]' ELSE messages.content END, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type, `+contentStatusSQL+`
		FROM messages
		JOIN chats ON messages.chat_jid = chats.jid
		WHERE messages.id = ? AND (? = '' OR messages.chat_jid = ?)`, messageID, filter, filter)

	var (
		ts            time.Time
		sender        string
		chatName      sql.NullString
		content       sql.NullString
		isFromMe      bool
		chatJID       string
		id            string
		msgChat       string
		mediaType     sql.NullString
		contentStatus string
	)
	if err := row.Scan(&ts, &sender, &chatName, &content, &isFromMe, &chatJID, &id, &msgChat, &mediaType, &contentStatus); err != nil {
		if err == sql.ErrNoRows {
			return nil, &MissingMessageError{ID: messageID}
		}
		return nil, fmt.Errorf("get message context: %w", err)
	}

	target := Message{
		ContentStatus: contentStatus,
		ID:            id,
		ChatJID:       chatJID,
		Sender:        sender,
		Content:       content.String,
		Timestamp:     ts,
		IsFromMe:      isFromMe,
		MediaType:     mediaType.String,
		ChatName:      chatName.String,
	}
	target.IsGroup = strings.HasSuffix(target.ChatJID, "@g.us")
	target.SenderPhone = s.resolveSenderPhone(target.Sender)

	beforeMsgs, err := s.queryContextWindow(ctx, msgChat, ts, id, before, false)
	if err != nil {
		return nil, err
	}
	afterMsgs, err := s.queryContextWindow(ctx, msgChat, ts, id, after, true)
	if err != nil {
		return nil, err
	}

	return &MessageContext{
		Message: target,
		Before:  beforeMsgs,
		After:   afterMsgs,
	}, nil
}

// GetSenderName resolves a sender JID to a display name, preferring the
// synced address book (whatsmeow_contacts) over the chats table — 1:1 chat
// names are often just the bare phone number. Falls back to the input value
// if no name is found.
func (s *Store) GetSenderName(ctx context.Context, senderJID string) string {
	if senderJID == "" || strings.HasSuffix(senderJID, "@g.us") {
		return "Unknown participant"
	}
	candidates := []string{senderJID}
	if !strings.Contains(senderJID, "@") {
		candidates = append(candidates, senderJID+"@s.whatsapp.net", senderJID+"@lid")
	}
	for _, jid := range candidates {
		if name := s.ContactName(jid); name != "" {
			return name
		}
		resolved := s.ResolveLIDToJID(jid)
		if name := s.chatNameByJID(ctx, resolved); name != "" {
			return name
		}
	}
	if strings.HasSuffix(senderJID, "@lid") {
		return "Unknown participant"
	}
	return senderJID
}

// chatNameByJID looks up chats.name for an exact jid match. Returns "" if
// not found or the stored name is empty.
func (s *Store) chatNameByJID(ctx context.Context, jid string) string {
	var name sql.NullString
	if err := s.reader(ctx).QueryRowContext(ctx,
		"SELECT name FROM chats WHERE jid = ? LIMIT 1", jid).Scan(&name); err == nil {
		return name.String
	}
	return ""
}

// queryContextWindow fetches `limit` messages in chat either strictly before
// (ascending=false) or strictly after (ascending=true) the anchor timestamp.
func (s *Store) queryContextWindow(ctx context.Context, chatJID string, anchor time.Time, anchorID string, limit int, ascending bool) ([]Message, error) {
	op := "<"
	order := "DESC"
	if ascending {
		op = ">"
		order = "ASC"
	}
	q := fmt.Sprintf(`
		SELECT `+listMessageColumns+`
		FROM messages
		JOIN chats ON messages.chat_jid = chats.jid
		WHERE messages.chat_jid = ? AND (messages.timestamp %s ? OR (messages.timestamp = ? AND messages.id %s ?))
		ORDER BY messages.timestamp %s, messages.id %s
		LIMIT ?`, op, op, order, order)

	rows, err := s.reader(ctx).QueryContext(ctx, q, chatJID, anchor.UTC(), anchor.UTC(), anchorID, limit)
	if err != nil {
		return nil, fmt.Errorf("context window: %w", err)
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		m, err := s.scanListMessageRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// scanListMessageRow scans the 8-column projection used by list_messages and
// the context window queries.
func (s *Store) scanListMessageRow(row interface {
	Scan(dest ...any) error
}) (Message, error) {
	var (
		ts            time.Time
		sender        string
		chatName      sql.NullString
		content       sql.NullString
		isFromMe      bool
		chatJID       string
		id            string
		mediaType     sql.NullString
		contentStatus string
	)
	if err := row.Scan(&ts, &sender, &chatName, &content, &isFromMe, &chatJID, &id, &mediaType, &contentStatus); err != nil {
		return Message{}, err
	}
	m := Message{
		ContentStatus: contentStatus,
		ID:            id,
		ChatJID:       chatJID,
		Sender:        sender,
		Content:       content.String,
		Timestamp:     ts,
		IsFromMe:      isFromMe,
		MediaType:     mediaType.String,
		ChatName:      chatName.String,
	}
	m.IsGroup = strings.HasSuffix(m.ChatJID, "@g.us")
	m.SenderPhone = s.resolveSenderPhone(m.Sender)
	return m, nil
}

// resolveSenderPhone returns a full phone (bare user part) for a message
// sender. The stored sender may be a LID user part (group participants, and
// DMs whose LID mapping was unknown when the message arrived), so try the
// LID map; a phone-number sender simply misses and is returned unchanged.
func (s *Store) resolveSenderPhone(sender string) string {
	if sender == "" || sender == "me" {
		return sender
	}
	resolved := s.ResolveLIDToJID(sender + "@lid")
	if u := jidUser(resolved); u != "" {
		return u
	}
	return sender
}

// chatSelectSQL returns the shared 6-column chat projection (jid, name,
// last_message_time, plus the latest message's content/sender/is_from_me
// when includeLastMessage) used by both ListChats and GetChat. alias
// qualifies the chat column references; pass "chats" for an unaliased FROM
// clause (ListChats) or a short alias like "c" when the caller appends its
// own WHERE clause on that alias (GetChat).
func chatSelectSQL(alias string, includeLastMessage bool) string {
	from := "chats"
	if alias != "chats" {
		from = "chats " + alias
	}
	if !includeLastMessage {
		return fmt.Sprintf(`SELECT
		%s.jid,
		%s.name,
		%s.last_message_time,
		NULL,
		NULL,
		NULL
	FROM %s`, alias, alias, alias, from)
	}
	return fmt.Sprintf(`SELECT
		%s.jid,
		%s.name,
		%s.last_message_time,
		CASE WHEN EXISTS(SELECT 1 FROM message_conflicts conflict WHERE conflict.chat_jid=latest_msg.chat_jid AND conflict.message_id=latest_msg.id) THEN '[content unavailable: conflicting unversioned copies]' ELSE latest_msg.content END,
		latest_msg.sender,
		latest_msg.is_from_me
	FROM %s
		LEFT JOIN messages AS latest_msg ON %s.jid = latest_msg.chat_jid
		AND latest_msg.rowid = (
			SELECT rowid FROM messages
			WHERE chat_jid = %s.jid
			ORDER BY timestamp DESC,id DESC LIMIT 1
		)`, alias, alias, alias, from, alias, alias)
}

// scanChatRow scans the 6-column projection used by ListChats and GetChat.
func scanChatRow(row interface {
	Scan(dest ...any) error
}) (Chat, error) {
	var (
		jid          string
		name         sql.NullString
		lastTime     sql.NullTime
		lastMessage  sql.NullString
		lastSender   sql.NullString
		lastIsFromMe sql.NullBool
	)
	if err := row.Scan(&jid, &name, &lastTime, &lastMessage, &lastSender, &lastIsFromMe); err != nil {
		return Chat{}, err
	}
	c := Chat{
		JID:          jid,
		Name:         name.String,
		LastMessage:  lastMessage.String,
		LastSender:   lastSender.String,
		LastIsFromMe: lastIsFromMe.Bool,
	}
	if lastTime.Valid {
		c.LastMessageTime = lastTime.Time
	}
	c.IsGroup = strings.HasSuffix(c.JID, "@g.us")
	return c, nil
}

type MissingMessageError struct{ ID string }

func (e *MissingMessageError) Error() string {
	return fmt.Sprintf("message with ID %s not found", e.ID)
}
func (e *MissingMessageError) Unwrap() error { return sql.ErrNoRows }
