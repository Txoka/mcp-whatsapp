package client

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"time"
)

var errNotSubmitted = errors.New("mutation not submitted")

// applyTrackedMutation persists the operation before network submission. An
// uncertain earlier operation for this target blocks blind resubmission.
func (c *Client) applyTrackedMutation(ctx context.Context, event store.MessageEvent, submit func(context.Context) error) error {
	chat, id := event.Message.ChatJID, event.Message.ID
	contentKey := ""
	if event.Kind == "edit" {
		contentKey = event.Message.Content
	}
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%s", event.Kind, chat, id, event.RevisionMS, contentKey))))
	if _, err := c.store.UncertainOperation(ctx, event.Kind, chat, id); err == nil {
		return fmt.Errorf("an earlier %s operation for this message has an unknown outcome; reconciliation required", event.Kind)
	} else if err != sql.ErrNoRows {
		return err
	}
	op, _, err := c.store.ReserveOperation(ctx, store.Operation{ID: key, Kind: event.Kind, Chat: chat, MessageID: id, RevisionMS: event.RevisionMS})
	if err != nil {
		return err
	}
	if op.State == "applied" {
		return nil
	}
	if op.State == "accepted" {
		return c.store.ReplayOutgoing(ctx, chat)
	}
	if err = c.store.CaptureOutgoing(ctx, key, chat, []store.MessageEvent{event}); err != nil {
		return err
	}
	claimed, err := c.store.BeginSubmission(ctx, key)
	if err != nil {
		return err
	}
	if !claimed {
		return fmt.Errorf("operation already claimed; do not submit again")
	}
	if err = submit(ctx); err != nil {
		state := "unknown"
		if errors.Is(err, errNotSubmitted) {
			state = "not_sent"
		}
		// Preserve an independently observed terminal event, even if the SDK lost
		// its acknowledgement or failed to finish local app-state reconciliation.
		observed, e := c.store.Operation(context.Background(), key)
		if e == nil && observed.State == "applied" {
			return nil
		}
		saveErr := c.store.SetOperation(context.Background(), key, state, err.Error())
		if saveErr != nil {
			return fmt.Errorf("%s outcome; journal update also failed: %v: %w", state, saveErr, err)
		}
		return fmt.Errorf("%s outcome: %w", state, err)
	}
	if err = c.store.AcceptMutation(context.Background(), key); err != nil {
		return fmt.Errorf("remote update accepted; local outcome journal unavailable: %w", err)
	}
	projectionCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = c.store.ReplayOutgoing(projectionCtx, chat); err != nil {
		return fmt.Errorf("remote update accepted; local projection incomplete: %w", err)
	}
	return nil
}
