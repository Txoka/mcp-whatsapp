package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const inboxSchema = `CREATE TABLE IF NOT EXISTS projection_inbox(id TEXT PRIMARY KEY,payload BLOB NOT NULL,state TEXT NOT NULL,watermark INTEGER NOT NULL DEFAULT 0);`

type projectionInput struct {
	OperationID string
	Events      []MessageEvent
	Chats       []ChatSnapshot
}
type ProjectionError struct {
	ID    string
	Chats []string
	Cause error
}

func (e *ProjectionError) Error() string {
	return fmt.Sprintf("projection %s is incomplete: %v", e.ID, e.Cause)
}
func (e *ProjectionError) Unwrap() error { return e.Cause }
func (e *ProjectionError) Affects(chat string) bool {
	if chat == "" {
		return true
	}
	for _, c := range e.Chats {
		if c == chat {
			return true
		}
	}
	return false
}

type eventAlias MessageEvent

func (e MessageEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Event    eventAlias
		Mentions []string
	}{eventAlias(e), e.Message.MentionJIDs})
}
func (e *MessageEvent) UnmarshalJSON(b []byte) error {
	var v struct {
		Event    eventAlias
		Mentions []string
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*e = MessageEvent(v.Event)
	e.Message.MentionJIDs = v.Mentions
	return nil
}
func (s *Store) ApplyHistory(ctx context.Context, events []MessageEvent, chats []ChatSnapshot, operationID ...string) (ApplyResult, error) {
	input := projectionInput{Events: events, Chats: chats}
	if len(operationID) > 0 {
		input.OperationID = operationID[0]
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return ApplyResult{}, err
	}
	id := fmt.Sprintf("%x", sha256.Sum256(payload))
	scopes := map[string]bool{}
	for _, e := range events {
		scopes[e.Message.ChatJID] = true
	}
	for _, c := range chats {
		scopes[c.JID] = true
	}
	fail := func(err error) (ApplyResult, error) {
		list := []string{}
		for c := range scopes {
			list = append(list, c)
		}
		return ApplyResult{}, &ProjectionError{ID: id, Chats: list, Cause: err}
	}
	if _, err = s.db.ExecContext(ctx, "INSERT OR IGNORE INTO projection_inbox(id,payload,state) VALUES(?,?,'pending')", id, payload); err != nil {
		return fail(err)
	}
	var state string
	var watermark int64
	if err = s.db.QueryRowContext(ctx, "SELECT state,watermark FROM projection_inbox WHERE id=?", id).Scan(&state, &watermark); err != nil {
		return fail(err)
	}
	if state == "applied" {
		return ApplyResult{Suppressed: len(events), Watermark: watermark}, nil
	}
	result, err := s.applyPrepared(ctx, input, id)
	if err != nil {
		return fail(err)
	}
	return result, nil
}

// ReplayPending replays captured input, never a network write. Successful
// projection clears the payload while retaining its digest and watermark.
func (s *Store) ReplayPending(ctx context.Context, chat string) error {
	rows, err := s.db.QueryContext(ctx, "SELECT id,payload FROM projection_inbox WHERE state='pending' ORDER BY rowid")
	if err != nil {
		return err
	}
	type pending struct {
		id      string
		payload []byte
	}
	var work []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.payload); err != nil {
			rows.Close()
			return err
		}
		work = append(work, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range work {
		var in projectionInput
		if err = json.Unmarshal(p.payload, &in); err != nil {
			return fmt.Errorf("invalid captured projection: %w", err)
		}
		relevant := chat == ""
		for _, e := range in.Events {
			relevant = relevant || e.Message.ChatJID == chat
		}
		for _, c := range in.Chats {
			relevant = relevant || c.JID == chat
		}
		if !relevant {
			continue
		}
		if _, err = s.ApplyHistory(ctx, in.Events, in.Chats, in.OperationID); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) ProjectionApplied(ctx context.Context, id string) (bool, error) {
	var state string
	err := s.db.QueryRowContext(ctx, "SELECT state FROM projection_inbox WHERE id=?", id).Scan(&state)
	return state == "applied", err
}
