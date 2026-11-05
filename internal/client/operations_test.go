package client

import (
	"context"
	"errors"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"strings"
	"testing"
	"time"
)

func TestUnknownDeletionIsNotResubmitted(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s}
	calls := 0
	event := store.MessageEvent{Kind: "delete_me", Message: store.Message{ChatJID: "123@g.us", ID: "m"}}
	submit := func(context.Context) error { calls++; return errors.New("acknowledgement lost") }
	if err = c.applyTrackedMutation(context.Background(), event, submit); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatal("unknown outcome not preserved")
	}
	if err = c.applyTrackedMutation(context.Background(), event, submit); err == nil || calls != 1 {
		t.Fatal("uncertain write retried")
	}
	if err = s.DeleteForMe(context.Background(), event.Message.ChatJID, event.Message.ID); err != nil {
		t.Fatal(err)
	}
	if err = c.applyTrackedMutation(context.Background(), event, submit); err != nil || calls != 1 {
		t.Fatalf("observed outcome not reconciled: %v", err)
	}
}
func TestUnsubmittedFailureCanBeExplicitlyRetried(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s}
	event := store.MessageEvent{Kind: "revoke", Message: store.Message{ChatJID: "123@g.us", ID: "m"}}
	if err = c.applyTrackedMutation(context.Background(), event, func(context.Context) error { return errNotSubmitted }); err == nil {
		t.Fatal("expected preflight failure")
	}
	if err = c.applyTrackedMutation(context.Background(), event, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
func TestReadPolicyRejectsInvalidOperatorConfiguration(t *testing.T) {
	t.Setenv("WHATSAPP_HISTORY_CONCURRENCY", "0")
	if _, err := readPolicyFromEnv(); err == nil {
		t.Fatal("invalid concurrency silently accepted")
	}
}

func TestRecentSyncWindowIsConfigurableAndPositive(t *testing.T) {
	t.Setenv("WHATSAPP_RECENT_SYNC_TIMEOUT", "80ms")
	p, err := readPolicyFromEnv()
	if err != nil || p.RecentSyncTimeout != 80*time.Millisecond {
		t.Fatal("recent sync policy not applied", err)
	}
	for _, bad := range []string{"0s", "-1s", "invalid"} {
		t.Setenv("WHATSAPP_RECENT_SYNC_TIMEOUT", bad)
		if _, err = readPolicyFromEnv(); err == nil {
			t.Fatal("invalid recent sync window accepted", bad)
		}
	}
}
