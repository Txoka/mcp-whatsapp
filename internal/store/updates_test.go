package store

import (
	"context"
	"testing"
	"time"
)

func updatesStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func incoming(t *testing.T, s *Store, chat, id string, ts time.Time) Message {
	t.Helper()
	if err := s.StoreChat(chat, "Peer", ts); err != nil {
		t.Fatal(err)
	}
	m := Message{ID: id, ChatJID: chat, Sender: "123", Content: "test", Timestamp: ts}
	if err := s.StoreMessage(context.Background(), m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	return m
}
func TestUnreadStateNeverTreatsAllHistoryAsUnread(t *testing.T) {
	s := updatesStore(t)
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	ts := time.Now().UTC().Truncate(time.Second)
	old := incoming(t, s, chat, "historical", ts.Add(-time.Hour))
	overview, err := s.UnreadChats(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Chats) != 0 || overview.UnknownChats != 1 {
		t.Fatal("historical incoming message assumed unread")
	}
	if err = s.ObserveChatRead(ctx, chat, true, time.Time{}, time.Time{}); err == nil {
		t.Fatal("timestamp-free marker accepted")
	}
	count := uint32(2)
	if err = s.SeedUnread(chat, &count, false); err != nil {
		t.Fatal(err)
	}
	m := incoming(t, s, chat, "live", ts)
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	overview, err = s.UnreadChats(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Chats) != 1 || *overview.Chats[0].Count != 3 || overview.Chats[0].KnownMessages != 1 {
		t.Fatalf("bad count/duplicate handling: %+v", overview)
	}
	messages, err := s.ListMessages(ctx, ListMessagesParams{ChatJID: chat, UnreadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ID != "live" {
		t.Fatal("unknown historical messages leaked into unread filter")
	}
	if err = s.ObserveChatRead(ctx, chat, true, old.Timestamp, ts.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	overview, err = s.UnreadChats(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Chats) != 1 || overview.Chats[0].Count != nil || overview.Chats[0].KnownMessages != 1 {
		t.Fatal("partial watermark reported as complete count")
	}
	if err = s.ObserveReadIDs(ctx, chat, []string{"live"}); err != nil {
		t.Fatal(err)
	}
	messages, err = s.ListMessages(ctx, ListMessagesParams{ChatJID: chat, UnreadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 {
		t.Fatal("read receipt did not remove message")
	}
	if err = s.ObserveChatRead(ctx, chat, false, time.Time{}, ts.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	overview, err = s.UnreadChats(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(overview.Chats) != 1 || !overview.Chats[0].MarkedUnread || overview.Chats[0].KnownMessages != 0 {
		t.Fatal("manual marker confused with unread messages")
	}
	// An older action must not override a newer manual marker.
	if err = s.ObserveChatRead(ctx, chat, true, ts, ts); err != nil {
		t.Fatal(err)
	}
	overview, _ = s.UnreadChats(ctx, "", 100)
	if !overview.Chats[0].MarkedUnread {
		t.Fatal("stale read action overwrote newer marker")
	}
}
func TestWaitUpdatesCursorFilteringAndTimeout(t *testing.T) {
	s := updatesStore(t)
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	start, err := s.WaitUpdates(ctx, "", "message", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if start.Status != "timeout" || start.Cursor == "" {
		t.Fatal("missing initial wait cursor")
	}
	m := incoming(t, s, chat, "one", time.Now())
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, err := s.WaitUpdates(ctx, chat, "message", start.Cursor, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "updates" || len(got.Updates) != 1 || got.Updates[0].Text != "test" {
		t.Fatalf("event missed between calls: %+v", got)
	}
	none, err := s.WaitUpdates(ctx, chat, "message", got.Cursor, 0)
	if err != nil {
		t.Fatal(err)
	}
	if none.Status != "timeout" {
		t.Fatal("cursor replayed event")
	}
	other, err := s.WaitUpdates(ctx, "456@g.us", "any", start.Cursor, 0)
	if err != nil {
		t.Fatal(err)
	}
	if other.Status != "timeout" {
		t.Fatal("chat filter leaked event")
	}
	if _, err = s.WaitUpdates(ctx, "", "any", "foreign.1", 0); err == nil {
		t.Fatal("foreign cursor accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.WaitUpdates(cancelled, "", "any", got.Cursor, time.Second); err == nil {
		t.Fatal("cancelled wait ignored")
	}
}
func TestWaitDoesNotConsumeEventsForOtherWaiters(t *testing.T) {
	s := updatesStore(t)
	ctx := context.Background()
	start, err := s.WaitUpdates(ctx, "", "message", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan WaitResult, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { r, e := s.WaitUpdates(ctx, "", "message", start.Cursor, time.Second); results <- r; errs <- e }()
	}
	m := incoming(t, s, "123@s.whatsapp.net", "concurrent", time.Now())
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r := <-results
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
		if r.Status != "updates" || len(r.Updates) != 1 {
			t.Fatal("concurrent waiter lost event")
		}
	}
}
