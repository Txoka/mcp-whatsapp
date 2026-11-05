package client

import (
	"context"
	"errors"
	"fmt"
	"go.mau.fi/whatsmeow/appstate"
	"sync"
)

type appStateAPI interface {
	FetchAppState(context.Context, appstate.WAPatchName, bool, bool) error
	SendAppState(context.Context, appstate.PatchInfo) error
}

// accountStateWriter owns the full version/read -> update -> catch-up sequence.
// whatsmeow serializes fetches internally, but not concurrent SendAppState calls.
type accountStateWriter struct {
	once sync.Once
	lane chan struct{}
}

func (w *accountStateWriter) send(ctx context.Context, api appStateAPI, patch appstate.PatchInfo) error {
	release, err := w.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := api.FetchAppState(ctx, patch.Type, false, false); err != nil {
		if !errors.Is(err, appstate.ErrMismatchingLTHash) {
			return fmt.Errorf("%w: account-state preflight failed: %w", errNotSubmitted, err)
		}
		// Rebuild only the local app-state collection from the server. This does
		// not log out devices or reset the account's state on WhatsApp.
		if err = api.FetchAppState(ctx, patch.Type, true, false); err != nil {
			return fmt.Errorf("%w: account-state recovery failed: %w", errNotSubmitted, err)
		}
	}
	if err := api.SendAppState(ctx, patch); err != nil {
		if errors.Is(err, appstate.ErrMismatchingLTHash) {
			if recoveryErr := api.FetchAppState(ctx, patch.Type, true, false); recoveryErr != nil {
				return fmt.Errorf("update outcome unknown: %w; local state recovery also failed: %v; do not automatically retry", err, recoveryErr)
			}
		}
		return fmt.Errorf("update outcome unknown: %w; do not automatically retry", err)
	}
	return nil
}

func (w *accountStateWriter) acquire(ctx context.Context) (func(), error) {
	w.once.Do(func() { w.lane = make(chan struct{}, 1) })
	select {
	case w.lane <- struct{}{}:
		return func() { <-w.lane }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", errNotSubmitted, ctx.Err())
	}
}
func (w *accountStateWriter) refresh(ctx context.Context, api appStateAPI, name appstate.WAPatchName, needed func() bool) error {
	release, err := w.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if !needed() {
		return nil
	}
	return api.FetchAppState(ctx, name, true, false)
}
