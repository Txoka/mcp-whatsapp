package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

const outboxSchema = `CREATE TABLE IF NOT EXISTS outgoing_intents(id TEXT PRIMARY KEY,chat_jid TEXT NOT NULL,payload BLOB NOT NULL,state TEXT NOT NULL,server_time TIMESTAMP);`

func (s *Store) CaptureOutgoing(ctx context.Context, id, chat string, events []MessageEvent) error {
	raw, err := json.Marshal(events)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "INSERT OR IGNORE INTO outgoing_intents(id,chat_jid,payload,state) VALUES(?,?,?,'prepared')", id, chat, raw)
	return err
}
func (s *Store) AcceptOutgoing(ctx context.Context, id string, at time.Time) error {
	if at.IsZero() {
		return fmt.Errorf("accepted send lacks a server timestamp; local projection remains pending")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE operations SET state='accepted',updated_at=? WHERE id=? AND state!='applied'", time.Now().UnixMilli(), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='accepted',server_time=? WHERE id=? AND state!='complete'", at.UTC(), id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ReplayOutgoing(ctx context.Context, chat string) error {
	rows, err := s.db.QueryContext(ctx, "SELECT id,payload,server_time FROM outgoing_intents WHERE state='accepted' AND (?='' OR chat_jid=?) ORDER BY rowid", chat, chat)
	if err != nil {
		return err
	}
	type work struct {
		id      string
		payload []byte
		at      sql.NullTime
	}
	var pending []work
	for rows.Next() {
		var w work
		if err = rows.Scan(&w.id, &w.payload, &w.at); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, w)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, w := range pending {
		var events []MessageEvent
		if err = json.Unmarshal(w.payload, &events); err != nil {
			return fmt.Errorf("invalid outgoing intent: %w", err)
		}
		for i := range events {
			if events[i].Kind == "message" || events[i].Kind == "" || events[i].Kind == "poll_vote" {
				if !w.at.Valid || w.at.Time.IsZero() {
					return fmt.Errorf("accepted message lacks a recorded server timestamp")
				}
				events[i].Message.Timestamp = w.at.Time
			}
		}
		if _, err = s.ApplyHistory(ctx, events, nil, w.id); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) OutgoingTimestamp(ctx context.Context, id string) (time.Time, error) {
	var at sql.NullTime
	err := s.db.QueryRowContext(ctx, "SELECT server_time FROM outgoing_intents WHERE id=?", id).Scan(&at)
	return at.Time, err
}

func (s *Store) AcceptMutation(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "UPDATE operations SET state='accepted',updated_at=? WHERE id=? AND state!='applied'", time.Now().UnixMilli(), id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE outgoing_intents SET state='accepted' WHERE id=? AND state!='complete'", id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) UnreconciledMessageWrites(ctx context.Context, chat string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM operations WHERE kind IN ('send','edit','revoke','delete_me') AND state IN ('unknown','submitting','accepted') AND (?='' OR chat_jid=?)", chat, chat).Scan(&count)
	return count, err
}
