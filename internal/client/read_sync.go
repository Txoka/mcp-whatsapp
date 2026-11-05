package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
)

// ReadTimeout bounds synchronization work, including internal throttling.
const ReadTimeout = 5 * time.Minute

type HistoryBatch struct {
	Exhausted    bool
	Inaccessible bool
}
type historyWaiter struct {
	mu     sync.Mutex
	chat   string
	done   chan struct{}
	result HistoryBatch
	err    error
}
type readSyncState struct {
	mu          sync.Mutex
	wake        chan struct{}
	ready       bool
	active      int
	failure     error
	pending     map[string]*historyWaiter
	collections map[string]error
}

func (s *readSyncState) changedLocked() {
	if s.wake != nil {
		close(s.wake)
	}
	s.wake = make(chan struct{})
}
func (s *readSyncState) connection(ready bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = ready

	s.changedLocked()
}
func (s *readSyncState) processing() func(error) {
	s.mu.Lock()
	s.active++
	s.changedLocked()
	s.mu.Unlock()
	return func(err error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		if err != nil {
			s.failure = err
		}
		s.changedLocked()
	}
}
func (s *readSyncState) wait(ctx context.Context, connected func() bool, scope ...string) error {
	for {
		s.mu.Lock()
		if s.wake == nil {
			s.wake = make(chan struct{})
		}
		ready, active, failure, wake := s.ready, s.active, s.failure, s.wake
		domain := "all"
		if len(scope) > 1 {
			domain = scope[1]
		}
		for name, cause := range s.collections {
			if cause != nil && collectionAffects(name, domain) {
				failure = fmt.Errorf("account collection %s is not synchronized: %w", name, cause)
				break
			}
		}
		s.mu.Unlock()
		if !connected() {
			return errors.New("synchronization unavailable: WhatsApp disconnected; cached results withheld")
		}
		if failure != nil && len(scope) > 0 && scope[0] != "" {
			var projection *store.ProjectionError
			if errors.As(failure, &projection) && !projection.Affects(scope[0]) {
				failure = nil
			}
		}
		if failure != nil {
			return fmt.Errorf("synchronization incomplete: %w; cached results withheld", failure)
		}
		if ready && active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("synchronization incomplete: %w; cached results withheld", ctx.Err())
		case <-wake:
		}
	}
}

// WaitReadReady waits for offline catch-up and any history import already in progress.
// It establishes local catch-up, not an authoritative snapshot of every phone chat.
func (c *Client) WaitReadReady(ctx context.Context) error { return c.WaitReadReadyFor(ctx, "") }
func (c *Client) WaitReadReadyFor(ctx context.Context, chat string, domains ...string) error {
	domain := "all"
	if len(domains) > 0 {
		domain = domains[0]
	}
	if c == nil {
		return errors.New("synchronization unavailable: no WhatsApp client")
	}
	ctx, cancel := context.WithTimeout(ctx, c.ReadPolicy().Timeout)
	defer cancel()
	if err := c.repairFailedCollections(ctx, domain); err != nil {
		return err
	}
	if c.store != nil {
		if err := c.store.ReplayOutgoing(ctx, chat); err != nil {
			return err
		}
		if err := c.store.ReplayPending(ctx, chat); err != nil {
			return err
		}
		c.syncState.mu.Lock()
		failure := c.syncState.failure
		c.syncState.mu.Unlock()
		var projection *store.ProjectionError
		if errors.As(failure, &projection) {
			if applied, err := c.store.ProjectionApplied(ctx, projection.ID); err == nil && applied {
				c.syncState.mu.Lock()
				if c.syncState.failure == failure {
					c.syncState.failure = nil
					c.syncState.changedLocked()
				}
				c.syncState.mu.Unlock()
			}
		}

	}
	if c.store != nil && domain != "contacts" {
		pending, err := c.store.UnreconciledMessageWrites(ctx, chat)
		if err != nil {
			return err
		}
		if pending > 0 {
			return fmt.Errorf("synchronization incomplete: unresolved message-write outcomes affect this scope; cached results withheld")
		}
	}

	return c.syncState.wait(ctx, func() bool { return c.wa != nil && c.wa.IsConnected() }, chat, domain)
}
func (s *readSyncState) register(id, chat string) *historyWaiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = map[string]*historyWaiter{}
	}
	w := &historyWaiter{chat: chat, done: make(chan struct{})}
	s.pending[id] = w
	return w
}
func (s *readSyncState) remove(id string) { s.mu.Lock(); delete(s.pending, id); s.mu.Unlock() }

// completeHistory is called only after all messages in the payload were committed.
// Uncorrelated or non-final chunks cannot satisfy a reader.
func (c *Client) completeHistory(evt *events.HistorySync, importErr error) {
	if evt == nil || evt.Data == nil || evt.Notification == nil || evt.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
		return
	}
	id := evt.Notification.GetPeerDataRequestSessionID()
	if id == "" {
		id = evt.Notification.GetOriginalMessageID()
	}
	c.syncState.mu.Lock()
	w := c.syncState.pending[id]
	if w == nil {
		id = evt.Notification.GetOriginalMessageID()
		w = c.syncState.pending[id]
	}
	c.syncState.mu.Unlock()
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	c.syncState.mu.Lock()
	current := c.syncState.pending[id] == w
	c.syncState.mu.Unlock()
	if !current {
		return
	}
	for _, conv := range evt.Data.Conversations {
		if conv == nil || c.store.ResolveLIDToJID(conv.GetID()) != w.chat {
			continue
		}
		if importErr != nil {
			w.err = importErr
		} else {
			final := conv.GetEndOfHistoryTransfer() || conv.EndOfHistoryTransferType != nil || evt.Data.GetProgress() >= 100 || evt.Notification.GetProgress() >= 100

			if !final {
				return
			}
			if conv.EndOfHistoryTransferType != nil {
				w.result.Exhausted = conv.GetEndOfHistoryTransferType() == waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY
				w.result.Inaccessible = conv.GetEndOfHistoryTransferType() == waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_WITH_MORE_MSG_ON_PRIMARY_BUT_NO_ACCESS
			}
		}
		coverage := store.HistoryCoverage{Boundary: "unknown"}
		if w.result.Exhausted {
			coverage.Boundary = "exhausted"
		}
		if w.result.Inaccessible {
			coverage.Boundary = "inaccessible"
		}
		if err := c.store.DB().QueryRow("SELECT version FROM projection_clock WHERE id=1").Scan(&coverage.Watermark); err != nil {
			w.err = err
		}
		var members []string
		for _, item := range conv.Messages {
			if item == nil || item.Message == nil {
				continue
			}
			t := time.Unix(int64(item.Message.GetMessageTimestamp()), 0)
			if t.Unix() <= 0 {
				continue
			}
			key := item.Message.GetKey().GetID()
			members = append(members, key)
			if coverage.Lower.IsZero() || t.Before(coverage.Lower) || (t.Equal(coverage.Lower) && key < coverage.LowerID) {
				coverage.Lower = t
				coverage.LowerID = key
				coverage.LowerFromMe = item.Message.GetKey().GetFromMe()
			}
			if coverage.Upper.IsZero() || t.After(coverage.Upper) {
				coverage.Upper = t
				coverage.UpperID = key
			}
		}
		if coverage.Boundary == "exhausted" && coverage.Lower.IsZero() {
			coverage.Lower = time.Unix(0, 0)
		}
		state := "complete"
		if w.err != nil {
			state = "failed"
		}
		if err := c.store.FinishHistory(context.Background(), id, state, coverage, members...); err != nil {
			w.err = err
		}
		c.syncState.mu.Lock()
		delete(c.syncState.pending, id)
		c.syncState.mu.Unlock()
		close(w.done)
		return
	}
}

// FetchHistory serializes peer fetches, tracks requests durably, and waits
// for the correlated imported batch. A send acknowledgement is not completion.
func (c *Client) FetchHistory(ctx context.Context, chat string, useLID bool) (HistoryBatch, error) {
	return c.fetchHistoryAt(ctx, chat, useLID, nil)
}
func (c *Client) fetchHistoryAt(ctx context.Context, chat string, useLID bool, requested *store.HistoryAnchor) (HistoryBatch, error) {
	c.historyLaneOnce.Do(func() {
		p := c.ReadPolicy()
		c.historyLane = make(chan struct{}, p.HistoryConcurrency)
		c.historyQueue = make(chan struct{}, p.HistoryQueue)
	})
	select {
	case c.historyQueue <- struct{}{}:
		defer func() { <-c.historyQueue }()
	default:
		return HistoryBatch{}, fmt.Errorf("history queue full; no request submitted")
	}
	select {
	case c.historyLane <- struct{}{}:
		defer func() { <-c.historyLane }()
	case <-ctx.Done():
		return HistoryBatch{}, ctx.Err()
	}
	if err := c.WaitReadReadyFor(ctx, chat); err != nil {
		return HistoryBatch{}, err
	}
	chat = c.store.ResolveLIDToJID(chat)
	if err := c.WaitReadReadyFor(ctx, chat); err != nil {
		return HistoryBatch{}, err
	}
	id := c.wa.GenerateMessageID()
	anchor, err := c.store.NextHistoryAnchor(ctx, chat)
	if requested != nil {
		anchor = *requested
		err = nil
	}
	if err != nil {
		return HistoryBatch{}, err
	}
	if err = c.store.StartHistory(ctx, id, chat, anchor); err != nil {
		return HistoryBatch{}, err
	}
	w := c.syncState.register(id, chat)
	defer c.syncState.remove(id)
	defer func() { // uncompleted requests stay explicitly interrupted/timed out
		c.store.DB().Exec("UPDATE history_requests SET state='unconfirmed',finished_at=? WHERE id=? AND state='pending'", time.Now().UnixMilli(), id)
	}()
	if _, err := c.requestHistorySync(ctx, chat, time.Time{}, AnchorOldest, c.ReadPolicy().HistoryBatch, useLID, id, anchor); err != nil {
		return HistoryBatch{}, err
	}
	select {
	case <-w.done:
		return w.result, w.err
	case <-ctx.Done():
		return HistoryBatch{}, fmt.Errorf("history completion not confirmed: %w", ctx.Err())
	}
}

func (s *readSyncState) recordFailure(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failure = err
	s.changedLocked()
}

func (s *readSyncState) collectionFailure(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.collections == nil {
		s.collections = map[string]error{}
	}
	s.collections[name] = err
	s.changedLocked()
}
func (s *readSyncState) collectionComplete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.collections, name)
	s.changedLocked()
}
func (c *Client) repairFailedCollections(ctx context.Context, domain string) error {
	c.syncState.mu.Lock()
	names := []string{}
	for name := range c.syncState.collections {
		if collectionAffects(name, domain) {
			names = append(names, name)
		}
	}
	c.syncState.mu.Unlock()
	for _, name := range names {
		err := c.accountState.refresh(ctx, c.wa, appstate.WAPatchName(name), func() bool {
			c.syncState.mu.Lock()
			defer c.syncState.mu.Unlock()
			return c.syncState.collections[name] != nil
		})
		if err != nil {
			return fmt.Errorf("account-state synchronization unavailable for %s: %w", name, err)
		}
		c.syncState.collectionComplete(name)
	}
	return nil
}

func collectionAffects(name, domain string) bool {
	if domain == "all" {
		return true
	}
	switch name {
	case "regular_low":
		return domain == "unread"
	case "regular_high":
		return domain != "contacts"
	default:
		return true
	}
}
