package client

import (
	"context"
	"errors"
	"go.mau.fi/whatsmeow/appstate"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAppState struct {
	active       atomic.Int32
	max          atomic.Int32
	sends        atomic.Int32
	full         atomic.Int32
	preflightErr error
	sendErr      error
}

func (f *fakeAppState) FetchAppState(_ context.Context, _ appstate.WAPatchName, full, _ bool) error {
	if full {
		f.full.Add(1)
		return nil
	}
	return f.preflightErr
}
func (f *fakeAppState) SendAppState(context.Context, appstate.PatchInfo) error {
	n := f.active.Add(1)
	defer f.active.Add(-1)
	for old := f.max.Load(); n > old; old = f.max.Load() {
		if f.max.CompareAndSwap(old, n) {
			break
		}
	}
	f.sends.Add(1)
	time.Sleep(time.Millisecond)
	return f.sendErr
}
func TestAccountStateWritesSerialized(t *testing.T) {
	var w accountStateWriter
	f := &fakeAppState{}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.send(context.Background(), f, appstate.PatchInfo{}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if f.max.Load() != 1 || f.sends.Load() != 12 {
		t.Fatalf("overlapping updates: max=%d sends=%d", f.max.Load(), f.sends.Load())
	}
}
func TestAccountStateFailureNeverRetriesMutation(t *testing.T) {
	var w accountStateWriter
	f := &fakeAppState{sendErr: appstate.ErrMismatchingLTHash}
	err := w.send(context.Background(), f, appstate.PatchInfo{})
	if err == nil || !strings.Contains(err.Error(), "outcome unknown") || f.sends.Load() != 1 || f.full.Load() != 1 {
		t.Fatalf("unsafe retry or missing recovery: %v", err)
	}
}
func TestAccountStatePreflightFailureDoesNotSend(t *testing.T) {
	var w accountStateWriter
	f := &fakeAppState{preflightErr: errors.New("offline")}
	if w.send(context.Background(), f, appstate.PatchInfo{}) == nil || f.sends.Load() != 0 {
		t.Fatal("sent despite failed preflight")
	}
}
