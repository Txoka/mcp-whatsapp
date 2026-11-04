package mcp

import (
	"context"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"strings"
	"testing"
	"time"
)

type fakeHistory struct {
	s     *store.Store
	calls int
	lid   bool
	fail  bool
}

func (f *fakeHistory) Store() *store.Store { return f.s }
func (f *fakeHistory) RequestHistorySync(ctx context.Context, chat string, ts time.Time, anchor string, count int, lid bool) (string, error) {
	f.calls++
	f.lid = lid
	if f.fail {
		return "", fmt.Errorf("offline")
	}
	return "requested", f.s.StoreMessage(ctx, store.Message{ID: "older", ChatJID: chat, Sender: "123", Content: "older", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, nil, nil, nil, 0)
}
func testHistory(t *testing.T) *fakeHistory {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.StoreChat("123@s.whatsapp.net", "Peer", time.Now()); err != nil {
		t.Fatal(err)
	}
	return &fakeHistory{s: s}
}
func TestAutomaticHistoryLoadingAndCooldown(t *testing.T) {
	f := testHistory(t)
	ctx := context.Background()
	p := store.ListMessagesParams{ChatJID: "123@s.whatsapp.net", Limit: 10}
	h := &historyReader{}
	_, status, _, err := h.read(ctx, f, p, true)
	if err != nil {
		t.Fatal(err)
	}
	if f.calls != 0 || !strings.Contains(status, "Initial history") {
		t.Fatal("empty cache attempted fabricated cursor")
	}
	f.s.StoreMessage(ctx, store.Message{ID: "newer", ChatJID: p.ChatJID, Sender: "123", Content: "newer", Timestamp: time.Now()}, nil, nil, nil, 0)
	messages, status, _, err := h.read(ctx, f, p, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || f.calls != 1 || !strings.Contains(status, "loaded") {
		t.Fatalf("history not transparently loaded: %d %s %d", len(messages), status, f.calls)
	}
	h.read(ctx, f, p, true)
	if f.calls != 1 {
		t.Fatal("cooldown did not prevent repeat sync")
	}
	h.read(ctx, f, p, false)
	if f.calls != 1 {
		t.Fatal("cache-only query requested sync")
	}
	attempt := h.attempts[p.ChatJID]
	attempt.at = time.Now().Add(-time.Minute)
	attempt.anchor = "older"
	h.attempts[p.ChatJID] = attempt
	f.fail = true
	_, status, _, err = h.read(ctx, f, p, true)
	if err != nil {
		t.Fatal(err)
	}
	if f.lid || !strings.Contains(status, "could not be requested") {
		t.Fatal("fallback/error state failed")
	}
}
func TestCursorPagingDoesNotRepeatAfterNewMessage(t *testing.T) {
	f := testHistory(t)
	ctx := context.Background()
	ts := time.Now().UTC()
	chat := "123@s.whatsapp.net"
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := f.s.StoreMessage(ctx, store.Message{ID: id, ChatJID: chat, Sender: "123", Content: "test", Timestamp: ts}, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	p := store.ListMessagesParams{ChatJID: chat, Limit: 2}
	h := &historyReader{}
	first, _, cursor, err := h.read(ctx, f, p, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || cursor == "" {
		t.Fatal("missing first page/cursor")
	}
	f.s.StoreMessage(ctx, store.Message{ID: "new-arrival", ChatJID: chat, Sender: "123", Content: "new", Timestamp: ts.Add(time.Second)}, nil, nil, nil, 0)
	if err = applyCursor(&p, cursor); err != nil {
		t.Fatal(err)
	}
	second, _, _, err := h.read(ctx, f, p, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 || second[0].ID != "b" || second[1].ID != "a" {
		t.Fatalf("cursor repeated/skipped: %+v", second)
	}
	p.ChatJID = "other@g.us"
	if applyCursor(&p, cursor) == nil {
		t.Fatal("cursor accepted different filters")
	}
	if applyCursor(&p, "not-valid") == nil {
		t.Fatal("malformed cursor accepted")
	}
}
