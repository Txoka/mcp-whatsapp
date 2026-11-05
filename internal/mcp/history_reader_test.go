package mcp

import (
	"context"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/client"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"strings"
	"testing"
	"time"
)

type fakeHistory struct {
	s        *store.Store
	calls    int
	fail     bool
	readyErr error
	fetch    func(context.Context, string, bool) (client.HistoryBatch, error)
}

func (f *fakeHistory) Store() *store.Store                     { return f.s }
func (f *fakeHistory) WaitReadReady(ctx context.Context) error { return f.readyErr }
func (f *fakeHistory) FetchHistory(ctx context.Context, chat string, lid bool) (client.HistoryBatch, error) {
	f.calls++
	if f.fail {
		return client.HistoryBatch{}, fmt.Errorf("offline")
	}
	anchor, err := f.s.NextHistoryAnchor(ctx, chat)
	if err != nil {
		return client.HistoryBatch{}, err
	}
	request := fmt.Sprint("test-request-", f.calls)
	if err = f.s.StartHistory(ctx, request, chat, anchor); err != nil {
		return client.HistoryBatch{}, err
	}
	var batch client.HistoryBatch
	if f.fetch != nil {
		batch, err = f.fetch(ctx, chat, lid)
	} else {
		err = f.s.StoreMessage(ctx, store.Message{ID: "older", ChatJID: chat, Sender: "123", Content: "older", Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, nil, nil, nil, 0)
		batch.Exhausted = true
	}
	if err != nil {
		return batch, err
	}
	id, ts, me, err := f.s.GetOldestMessage(chat)
	if err != nil {
		return batch, err
	}
	boundary := "unknown"
	if batch.Exhausted {
		boundary = "exhausted"
	}
	if batch.Inaccessible {
		boundary = "inaccessible"
	}
	all, _ := f.s.ListMessages(ctx, store.ListMessagesParams{ChatJID: chat, Limit: 1000})
	ids := []string{}
	for _, m := range all {
		ids = append(ids, m.ID)
	}
	err = f.s.FinishHistory(ctx, request, "complete", store.HistoryCoverage{Lower: ts, LowerID: id, LowerFromMe: me, Boundary: boundary}, ids...)
	return batch, err
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
func TestAutomaticHistoryRequiresConfirmedCompletion(t *testing.T) {
	f := testHistory(t)
	ctx := context.Background()
	p := store.ListMessagesParams{ChatJID: "123@s.whatsapp.net", Limit: 10}
	h := &historyReader{}
	messages, _, _, err := h.read(ctx, f, p)
	if err == nil || len(messages) != 0 || f.calls != 0 {
		t.Fatal("empty cache fabricated a history boundary")
	}
	f.s.StoreMessage(ctx, store.Message{ID: "newer", ChatJID: p.ChatJID, Sender: "123", Content: "newer", Timestamp: time.Now()}, nil, nil, nil, 0)
	messages, status, _, err := h.read(ctx, f, p)
	if err != nil || len(messages) != 2 || f.calls != 1 || !strings.Contains(status, "no older") {
		t.Fatalf("confirmed history not returned: %d %s %v", len(messages), status, err)
	}
	f.readyErr = fmt.Errorf("offline")
	messages, _, _, err = h.read(ctx, f, p)
	if err == nil || len(messages) != 0 {
		t.Fatal("failed sync leaked cached partial messages")
	}
}
func TestHistoryTimeoutDoesNotReturnPartialMessages(t *testing.T) {
	f := testHistory(t)
	chat := "123@s.whatsapp.net"
	f.s.StoreMessage(context.Background(), store.Message{ID: "known", ChatJID: chat, Content: "known", Timestamp: time.Now()}, nil, nil, nil, 0)
	f.fetch = func(ctx context.Context, _ string, _ bool) (client.HistoryBatch, error) {
		<-ctx.Done()
		return client.HistoryBatch{}, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	messages, _, _, err := (&historyReader{}).read(ctx, f, store.ListMessagesParams{ChatJID: chat, Limit: 10})
	if err == nil || len(messages) != 0 {
		t.Fatal("unconfirmed request returned partial history")
	}
}
func TestHistoryContinuesThroughMultipleCompletedBatches(t *testing.T) {
	f := testHistory(t)
	chat := "123@s.whatsapp.net"
	ctx := context.Background()
	now := time.Now()
	f.s.StoreMessage(ctx, store.Message{ID: "known", ChatJID: chat, Content: "known", Timestamp: now}, nil, nil, nil, 0)
	f.fetch = func(ctx context.Context, chat string, _ bool) (client.HistoryBatch, error) {
		return client.HistoryBatch{}, f.s.StoreMessage(ctx, store.Message{ID: fmt.Sprint(f.calls), ChatJID: chat, Content: "older", Timestamp: now.Add(-time.Duration(f.calls) * time.Hour)}, nil, nil, nil, 0)
	}
	messages, _, _, err := (&historyReader{}).read(ctx, f, store.ListMessagesParams{ChatJID: chat, Limit: 3})
	if err != nil || len(messages) != 3 || f.calls != 2 {
		t.Fatalf("stopped early: %d %d %v", len(messages), f.calls, err)
	}
}
func TestFullCacheStillWaitsForCatchup(t *testing.T) {
	f := testHistory(t)
	f.readyErr = fmt.Errorf("catch-up incomplete")
	messages, _, _, err := (&historyReader{}).read(context.Background(), f, store.ListMessagesParams{})
	if err == nil || len(messages) != 0 {
		t.Fatal("catch-up failure hidden")
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
	first, _, cursor, err := h.read(ctx, f, p)
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
	second, _, _, err := h.read(ctx, f, p)
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

func TestConcurrentReadsShareLoadedHistory(t *testing.T) {
	f := testHistory(t)
	chat := "123@s.whatsapp.net"
	ctx := context.Background()
	f.s.StoreMessage(ctx, store.Message{ID: "known", ChatJID: chat, Content: "known", Timestamp: time.Now()}, nil, nil, nil, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	f.fetch = func(ctx context.Context, chat string, _ bool) (client.HistoryBatch, error) {
		close(started)
		<-release
		err := f.s.StoreMessage(ctx, store.Message{ID: "older", ChatJID: chat, Content: "older", Timestamp: time.Now().Add(-time.Hour)}, nil, nil, nil, 0)
		return client.HistoryBatch{Exhausted: true}, err
	}
	h := &historyReader{}
	results := make(chan error, 2)
	read := func() {
		messages, _, _, err := h.read(ctx, f, store.ListMessagesParams{ChatJID: chat, Limit: 2})
		if err == nil && len(messages) != 2 {
			err = fmt.Errorf("partial result")
		}
		results <- err
	}
	go read()
	<-started
	go read()
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("duplicate fetch: %d", f.calls)
	}
}
func TestMessageSchemaHidesSyncPolicy(t *testing.T) {
	schema := NewServer(nil, nil).MCP().ListTools()["list_messages"].Tool.InputSchema
	if _, ok := schema.Properties["auto_sync"]; ok {
		t.Fatal("sync policy exposed")
	}
}

type emptyHistoryTransfer struct{ *fakeHistory }

func (f emptyHistoryTransfer) FetchHistory(ctx context.Context, chat string, lid bool) (client.HistoryBatch, error) {
	f.calls++
	if f.calls > 2 {
		return client.HistoryBatch{}, fmt.Errorf("regression: repeated unchanged frontier")
	}
	anchor, err := f.s.NextHistoryAnchor(ctx, chat)
	if err != nil {
		return client.HistoryBatch{}, err
	}
	id := fmt.Sprint("empty-", f.calls)
	if err = f.s.StartHistory(ctx, id, chat, anchor); err != nil {
		return client.HistoryBatch{}, err
	}
	return client.HistoryBatch{}, f.s.FinishHistory(ctx, id, "complete", store.HistoryCoverage{Boundary: "unknown"})
}
func TestFullUnconfirmedCachePageStopsOnNoProgress(t *testing.T) {
	f := testHistory(t)
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	for i := 0; i < 5; i++ {
		if err := f.s.StoreMessage(ctx, store.Message{ID: fmt.Sprint(i), ChatJID: chat, Content: "cached", Timestamp: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC)}, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	msgs, _, _, err := (&historyReader{}).read(ctx, emptyHistoryTransfer{f}, store.ListMessagesParams{ChatJID: chat, Limit: 5})
	if err == nil || !strings.Contains(err.Error(), "no progress") || f.calls != 2 || len(msgs) != 0 {
		t.Fatalf("unbounded/incorrect read: calls=%d messages=%d error=%v", f.calls, len(msgs), err)
	}
}

func TestCompactCursorResumesAfterInsertionAndAcrossRestart(t *testing.T) {
	f := testHistory(t)
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	for i := 0; i < 4; i++ {
		if err := f.s.StoreMessage(ctx, store.Message{ID: fmt.Sprint(i), ChatJID: chat, Content: "cached", Timestamp: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC)}, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	p := store.ListMessagesParams{Limit: 2}
	h := &historyReader{}
	first, _, raw, err := h.read(ctx, f, p)
	if err != nil || len(first) != 2 {
		t.Fatal(err)
	}
	token, err := f.s.PutBookmark(ctx, "p", filterKey(p), raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) >= len(raw) {
		t.Fatal("cursor was not simplified")
	}
	if err = f.s.StoreMessage(ctx, store.Message{ID: "new-head", ChatJID: chat, Content: "new", Timestamp: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	// Resolve through a separately opened store, as after reconnect/restart.
	reopened, err := store.Open(f.s.Dir())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	resolved, err := reopened.ResolveBookmark(ctx, "p", filterKey(p), token)
	if err != nil {
		t.Fatal(err)
	}
	if err = applyCursor(&p, resolved); err != nil {
		t.Fatal(err)
	}
	next, _, _, err := h.read(ctx, f, p)
	if err != nil || len(next) != 2 || next[0].ID != "1" || next[1].ID != "0" {
		t.Fatalf("insert shifted continuation: %+v %v", next, err)
	}
	p.Page = 1
	if applyCursor(&p, resolved) == nil {
		t.Fatal("accepted page with cursor")
	}
}
