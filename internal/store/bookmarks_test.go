package store

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBookmarksSurviveRestartAndRejectWrongScope(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.PutBookmark(ctx, "p", "original-filters", "private-message-position")
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != 24 || !strings.HasPrefix(token, "p_") {
		t.Fatalf("not compact: %q", token)
	}
	same, err := s.PutBookmark(ctx, "p", "original-filters", "private-message-position")
	if err != nil || same != token {
		t.Fatal("same continuation allocated twice", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, err := s.ResolveBookmark(ctx, "p", "original-filters", token)
	if err != nil || raw != "private-message-position" {
		t.Fatal("bookmark lost on restart", err)
	}
	if _, err = s.ResolveBookmark(ctx, "p", "changed-filters", token); err == nil {
		t.Fatal("changed filters accepted")
	}
	if _, err = s.ResolveBookmark(ctx, "u", "original-filters", token); err == nil {
		t.Fatal("page cursor accepted as update cursor")
	}
	other, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err = other.ResolveBookmark(ctx, "p", "original-filters", token); err == nil {
		t.Fatal("foreign cache accepted")
	}
	for _, legacy := range []string{"", "old-encoded-page-cursor", "deadbeef.42"} {
		got, err := s.ResolveBookmark(ctx, "p", "original-filters", legacy)
		if err != nil || got != legacy {
			t.Fatal("legacy cursor changed", err)
		}
	}
}
func TestConcurrentBookmarksReuseSamePosition(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	tokens := make(chan string, 8)
	for range 8 {
		wg.Go(func() {
			token, e := s.PutBookmark(ctx, "u", "watch", "epoch.10")
			if e != nil {
				t.Error(e)
				return
			}
			tokens <- token
		})
	}
	wg.Wait()
	close(tokens)
	first := ""
	for token := range tokens {
		if first == "" {
			first = token
		}
		if token != first {
			t.Fatal("duplicate handles for same continuation")
		}
	}
	if first == "" {
		t.Fatal("no bookmark persisted")
	}
}

func TestUpdateBookmarkDoesNotMissArrivalBetweenWaits(t *testing.T) {
	s := updatesStore(t)
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	scope := "watch-message-123"
	start, err := s.WaitUpdates(ctx, chat, "message", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.PutBookmark(ctx, "u", scope, start.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	m := incoming(t, s, chat, "between-waits", time.Now())
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	raw, err := s.ResolveBookmark(ctx, "u", scope, token)
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.WaitUpdates(ctx, chat, "message", raw, 0)
	if err != nil || len(result.Updates) != 1 || result.Status != "updates" {
		t.Fatalf("lost arrival: %+v %v", result, err)
	}
	next, err := s.PutBookmark(ctx, "u", scope, result.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = s.ResolveBookmark(ctx, "u", scope, next)
	if err != nil {
		t.Fatal(err)
	}
	result, err = s.WaitUpdates(ctx, chat, "message", raw, 0)
	if err != nil || len(result.Updates) != 0 {
		t.Fatal("replayed consumed event", err)
	}
}
