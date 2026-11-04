package store

import (
	"context"
	"testing"
	"time"
)

func TestLongAndInfiniteWaitCancelWithoutModelPolling(t *testing.T) {
	for _, duration := range []time.Duration{time.Hour, 72 * time.Hour, InfiniteWait} {
		s := updatesStore(t)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := s.WaitUpdates(ctx, "", "message", "", duration); done <- err }()
		select {
		case err := <-done:
			t.Fatalf("long wait ended before cancellation: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		cancel()
		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("want cancellation, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("wait failed to cancel")
		}
	}
}
func TestInfiniteWaitWakesForMessage(t *testing.T) {
	s := updatesStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	initial, err := s.WaitUpdates(ctx, "", "message", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan WaitResult, 1)
	fail := make(chan error, 1)
	go func() { r, e := s.WaitUpdates(ctx, "", "message", initial.Cursor, InfiniteWait); done <- r; fail <- e }()
	m := incoming(t, s, "123@s.whatsapp.net", "wake", time.Now())
	if err = s.ObserveIncoming(ctx, m); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if err = <-fail; err != nil {
		t.Fatal(err)
	}
	if result.Status != "updates" || len(result.Updates) != 1 {
		t.Fatal("indefinite wait missed message")
	}
}
