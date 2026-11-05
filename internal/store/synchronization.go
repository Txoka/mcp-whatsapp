package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const synchronizationSchema = `
CREATE TABLE IF NOT EXISTS history_requests(
 id TEXT PRIMARY KEY,chat_jid TEXT NOT NULL,anchor_id TEXT NOT NULL,state TEXT NOT NULL,
 lower_time TIMESTAMP,upper_time TIMESTAMP,lower_id TEXT,upper_id TEXT,lower_from_me BOOLEAN NOT NULL DEFAULT 0,
 boundary TEXT NOT NULL DEFAULT 'unknown',watermark INTEGER,started_at INTEGER NOT NULL,finished_at INTEGER);
CREATE INDEX IF NOT EXISTS history_requests_chat ON history_requests(chat_jid,started_at);
CREATE TABLE IF NOT EXISTS history_members(request_id TEXT NOT NULL,message_id TEXT NOT NULL,PRIMARY KEY(request_id,message_id));
CREATE TABLE IF NOT EXISTS operations(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,
 state TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '',revision_ms INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL);
`

type HistoryAnchor struct {
	ID        string
	Timestamp time.Time
	IsFromMe  bool
}
type HistoryCoverage struct {
	Lower, Upper                         time.Time
	LowerID, UpperID, Boundary, AnchorID string
	Watermark                            int64
	LowerFromMe                          bool
}

func (s *Store) StartHistory(ctx context.Context, id, chat string, anchor HistoryAnchor) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO history_requests(id,chat_jid,anchor_id,upper_time,upper_id,state,started_at) VALUES(?,?,?,?,?,'pending',?)", id, chat, anchor.ID, anchor.Timestamp.UTC(), anchor.ID, time.Now().UnixMilli())
	return err
}
func (s *Store) FinishHistory(ctx context.Context, id, state string, c HistoryCoverage, members ...string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "UPDATE history_requests SET state=?,lower_time=?,lower_id=?,lower_from_me=?,boundary=?,watermark=?,finished_at=? WHERE id=?", state, c.Lower.UTC(), c.LowerID, c.LowerFromMe, c.Boundary, c.Watermark, time.Now().UnixMilli(), id)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO history_members(request_id,message_id) SELECT id,anchor_id FROM history_requests WHERE id=?", id); err != nil {
		return err
	}
	for _, member := range members {
		if member == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO history_members(request_id,message_id) VALUES(?,?)", id, member); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) HistoryCoverage(ctx context.Context, chat, anchor string) (HistoryCoverage, bool, error) {
	var c HistoryCoverage
	err := s.db.QueryRowContext(ctx, `SELECT lower_time,upper_time,lower_id,upper_id,boundary,anchor_id,watermark FROM history_requests WHERE chat_jid=? AND anchor_id=? AND state='complete' ORDER BY finished_at DESC LIMIT 1`, chat, anchor).Scan(&c.Lower, &c.Upper, &c.LowerID, &c.UpperID, &c.Boundary, &c.AnchorID, &c.Watermark)
	if err == sql.ErrNoRows {
		return c, false, nil
	}
	return c, err == nil, err
}
func (s *Store) RecoverInterrupted(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "UPDATE history_requests SET state='interrupted',finished_at=? WHERE state='pending'", time.Now().UnixMilli()); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, "UPDATE operations SET state='unknown',detail='service interrupted while submission was pending',updated_at=? WHERE state='submitting'", time.Now().UnixMilli())
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "UPDATE outgoing_intents SET state='unknown' WHERE id IN (SELECT id FROM operations WHERE state='unknown') AND state NOT IN ('accepted','complete')")
	return err
}

type Operation struct {
	ID, Kind, Chat, MessageID, State, Detail string
	RevisionMS                               int64
}

// ReserveOperation is an atomic claim. A previously uncertain operation is
// never implicitly retried, including after a restart.
func (s *Store) ReserveOperation(ctx context.Context, op Operation) (Operation, bool, error) {
	result, err := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO operations(id,kind,chat_jid,message_id,state,revision_ms,updated_at) VALUES(?,?,?,?,'prepared',?,?)", op.ID, op.Kind, op.Chat, op.MessageID, op.RevisionMS, time.Now().UnixMilli())
	if err != nil {
		return op, false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return op, false, err
	}
	err = s.db.QueryRowContext(ctx, "SELECT state,detail FROM operations WHERE id=?", op.ID).Scan(&op.State, &op.Detail)
	return op, n == 1, err
}
func (s *Store) Operation(ctx context.Context, id string) (Operation, error) {
	var op Operation
	op.ID = id
	err := s.db.QueryRowContext(ctx, "SELECT kind,chat_jid,message_id,state,detail FROM operations WHERE id=?", id).Scan(&op.Kind, &op.Chat, &op.MessageID, &op.State, &op.Detail)
	return op, err
}
func (s *Store) SetOperation(ctx context.Context, id, state, detail string) error {
	r, err := s.db.ExecContext(ctx, "UPDATE operations SET state=?,detail=?,updated_at=? WHERE id=? AND (state!='applied' OR ?='applied')", state, detail, time.Now().UnixMilli(), id, state)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err == nil && n != 1 {
		op, e := s.Operation(ctx, id)
		if e == nil && op.State == "applied" {
			return nil
		}
		return fmt.Errorf("operation not found or transition rejected")
	}
	return err
}
func (s *Store) BeginSubmission(ctx context.Context, id string) (bool, error) {
	r, err := s.db.ExecContext(ctx, "UPDATE operations SET state='submitting',updated_at=? WHERE id=? AND state IN ('prepared','not_sent')", time.Now().UnixMilli(), id)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n == 1, err
}

// JSON diagnostics intentionally omit message bodies and credentials.
func (s *Store) SyncDiagnostics(ctx context.Context) (string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT state,COUNT(*) FROM operations GROUP BY state")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var state string
		var count int
		if err = rows.Scan(&state, &count); err != nil {
			return "", err
		}
		counts[state] = count
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	b, err := json.Marshal(counts)
	return string(b), err
}

func (s *Store) UncertainOperation(ctx context.Context, kind, chat, message string) (Operation, error) {
	var op Operation
	err := s.db.QueryRowContext(ctx, "SELECT id,state,detail FROM operations WHERE kind=? AND chat_jid=? AND message_id=? AND state IN ('submitting','unknown') ORDER BY updated_at DESC LIMIT 1", kind, chat, message).Scan(&op.ID, &op.State, &op.Detail)
	return op, err
}

// NextHistoryAnchor follows confirmed transfer boundaries, including keys that
// are not visible messages (e.g. deleted rows). It never fabricates a cursor.
func (s *Store) NextHistoryAnchor(ctx context.Context, chat string) (HistoryAnchor, error) {
	if unknown, err := s.UnconfirmedHead(ctx, chat); err != nil {
		return HistoryAnchor{}, err
	} else if unknown {
		var head HistoryAnchor
		head.ID, head.Timestamp, head.IsFromMe, err = s.GetNewestMessage(chat)
		return head, err
	}
	var a HistoryAnchor
	err := s.db.QueryRowContext(ctx, "SELECT lower_id,lower_time,lower_from_me FROM history_requests WHERE chat_jid=? AND state='complete' AND lower_id!='' ORDER BY finished_at DESC,rowid DESC LIMIT 1", chat).Scan(&a.ID, &a.Timestamp, &a.IsFromMe)
	if err == nil {
		return a, nil
	}
	if err != sql.ErrNoRows {
		return a, err
	}
	a.ID, a.Timestamp, a.IsFromMe, err = s.GetNewestMessage(chat)
	return a, err
}
func (s *Store) CoversMessages(ctx context.Context, chat string, messages []Message) (bool, error) {
	if len(messages) == 0 {
		return false, nil
	}
	for _, m := range messages {
		var known bool
		if err := s.reader(ctx).QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM history_members member JOIN history_requests request ON request.id=member.request_id WHERE request.chat_jid=? AND request.state='complete' AND member.message_id=?)", chat, m.ID).Scan(&known); err != nil {
			return false, err
		}
		if !known {
			return false, nil
		}
	}
	upper := messages[0].Timestamp
	lower := messages[len(messages)-1].Timestamp
	rows, err := s.reader(ctx).QueryContext(ctx, "SELECT lower_time,upper_time,lower_id,upper_id FROM history_requests WHERE chat_jid=? AND state='complete' AND (lower_id!='' OR boundary='exhausted') ORDER BY upper_time DESC,lower_time ASC", chat)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	cursor := upper
	for rows.Next() {
		var l, u time.Time
		var lid, uid string
		if err = rows.Scan(&l, &u, &lid, &uid); err != nil {
			return false, err
		}
		if u.Before(cursor) || l.After(cursor) {
			continue
		}
		// Membership is checked above; equal timestamps do not establish
		// subsecond ordering between different WhatsApp message IDs.
		if !l.After(lower) {
			return true, nil
		}

		if l.Before(cursor) {
			cursor = l
		}
	}
	return false, rows.Err()
}
func (s *Store) HistoryBoundary(ctx context.Context, chat string) (string, error) {
	var boundary string
	err := s.db.QueryRowContext(ctx, "SELECT boundary FROM history_requests WHERE chat_jid=? AND state='complete' AND boundary IN ('exhausted','inaccessible') ORDER BY finished_at DESC,rowid DESC LIMIT 1", chat).Scan(&boundary)
	if err == sql.ErrNoRows {
		return "unknown", nil
	}
	return boundary, err
}

func (s *Store) UnconfirmedHead(ctx context.Context, chat string) (bool, error) {
	var unknown bool
	err := s.reader(ctx).QueryRowContext(ctx, `SELECT NOT EXISTS(SELECT 1 FROM history_members member JOIN history_requests request ON request.id=member.request_id WHERE request.chat_jid=messages.chat_jid AND request.state='complete' AND member.message_id=messages.id) FROM messages WHERE chat_jid=? ORDER BY timestamp DESC,id DESC LIMIT 1`, chat).Scan(&unknown)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return unknown, err
}

// CaptionRepairAnchor uses a genuinely newer message key, because on-demand
// history excludes the anchor itself. Never invent an anchor or timestamp.
func (s *Store) CaptionRepairAnchor(ctx context.Context, chat string, after time.Time) (HistoryAnchor, error) {
	var anchor HistoryAnchor
	err := s.reader(ctx).QueryRowContext(ctx, "SELECT id,timestamp,is_from_me FROM messages WHERE chat_jid=? AND timestamp>? ORDER BY timestamp ASC,id ASC LIMIT 1", chat, after.UTC()).Scan(&anchor.ID, &anchor.Timestamp, &anchor.IsFromMe)
	return anchor, err
}
