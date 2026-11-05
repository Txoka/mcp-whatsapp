package mcp

// This layer owns retrieval policy. Tool handlers do not manage WhatsApp
// anchors, LID fallback, asynchronous delivery, or retry throttling.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/client"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

type historyBackend interface {
	Store() *store.Store
	WaitReadReady(context.Context) error
	FetchHistory(context.Context, string, bool) (client.HistoryBatch, error)
}
type historyReader struct {
	mu    sync.Mutex
	lanes map[string]*readLane
}

type readLane struct {
	token chan struct{}
	users int
}

const snapshotNote = "Synchronized local snapshot, not an authoritative view of the phone. History coverage and remote edits/deletions may be incomplete."

type messageCursor struct {
	Time   time.Time `json:"t"`
	Chat   string    `json:"c"`
	ID     string    `json:"i"`
	Filter string    `json:"f"`
}

func filterKey(p store.ListMessagesParams) string {
	b, _ := json.Marshal([]any{p.ChatJID, p.Query, p.SenderPhone, p.After, p.Before, p.UnreadOnly})
	hash := sha256.Sum256(b)
	return fmt.Sprintf("%x", hash[:8])
}
func applyCursor(p *store.ListMessagesParams, raw string) error {
	if raw == "" {
		return nil
	}
	if len(raw) > 2048 {
		return fmt.Errorf("cursor: invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return fmt.Errorf("cursor: invalid")
	}
	var cursor messageCursor
	if json.Unmarshal(data, &cursor) != nil || cursor.ID == "" || cursor.Chat == "" || cursor.Time.IsZero() || cursor.Filter != filterKey(*p) {
		return fmt.Errorf("cursor: invalid or filters changed")
	}
	p.CursorTime, p.CursorChat, p.CursorID = cursor.Time, cursor.Chat, cursor.ID
	return nil
}
func nextCursor(p store.ListMessagesParams, messages []store.Message) string {
	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}
	if len(messages) < limit {
		return ""
	}
	last := messages[len(messages)-1]
	b, _ := json.Marshal(messageCursor{last.Timestamp, last.ChatJID, last.ID, filterKey(p)})
	return base64.RawURLEncoding.EncodeToString(b)
}

// read uses a short sync window for recent reads and loads older history only
// when the cached page is short. Coverage uncertainty is a warning, not a veto.
func (h *historyReader) read(ctx context.Context, backend historyBackend, p store.ListMessagesParams) ([]store.Message, string, string, error) {
	resultCtx := ctx
	policy := client.DefaultReadPolicy()
	if configured, ok := backend.(interface{ ReadPolicy() client.ReadPolicy }); ok {
		policy = configured.ReadPolicy()
	}
	recent := p.CursorID == "" && p.Before.IsZero() && p.Query == ""
	budget := policy.Timeout
	if recent {
		budget = policy.RecentSyncTimeout
		if budget <= 0 {
			budget = 3 * time.Second
		}
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	base := p
	base.IncludeContext = false
	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}
	finish := func(note string) ([]store.Message, string, string, error) {
		if err := resultCtx.Err(); err != nil {
			return nil, "", "", err
		}
		messages, err := backend.Store().ListMessages(resultCtx, base)
		if err != nil {
			return nil, "", "", err
		}
		status := snapshotNote
		if note != "" {
			status += " Warning: " + note + "."
		}
		if p.ChatJID != "" && len(messages) > 0 {
			covered, err := backend.Store().CoversMessages(resultCtx, p.ChatJID, messages)
			if err != nil {
				return nil, "", "", err
			}
			if !covered {
				status += " Warning: some cached messages were not independently confirmed by history transfers; history may be incomplete."
			}
		}
		if len(messages) < limit {
			status += fmt.Sprintf(" Returned %d cached matches; fewer than the requested %d does not establish that no other messages exist.", len(messages), limit)
		}
		cursor := nextCursor(p, messages)
		if p.IncludeContext {
			messages, err = backend.Store().ListMessages(resultCtx, p)
		}
		return messages, status, cursor, err
	}
	messages, err := backend.Store().ListMessages(resultCtx, base)
	if err != nil {
		return nil, "", "", err
	}
	if p.ChatJID == "" || p.UnreadOnly {
		return finish("")
	}
	if len(messages) >= limit {
		if err := waitForRead(ctx, backend, p.ChatJID); err != nil {
			return finish("synchronization did not finish within the read window; showing the latest committed cache")
		}
		covered, err := backend.Store().CoversMessages(ctx, p.ChatJID, messages)
		if err != nil {
			if ctx.Err() != nil {
				return finish("the synchronization window expired; showing cached messages")
			}
			return nil, "", "", err
		}
		if recent && !covered {
			if refresher, ok := backend.(interface {
				FetchRecentHistory(context.Context, string) (client.HistoryBatch, error)
			}); ok {
				if _, err = refresher.FetchRecentHistory(ctx, p.ChatJID); err != nil {
					return finish("recent history could not be confirmed within the read window; showing cached messages")
				}
			}
		}
		return finish("")
	}
	// Only incomplete cached pages enter the backfill lane. Complete recent pages
	// never wait behind a slow older-history query for the same chat.
	h.mu.Lock()
	if h.lanes == nil {
		h.lanes = map[string]*readLane{}
	}
	lane := h.lanes[p.ChatJID]
	if lane == nil {
		lane = &readLane{token: make(chan struct{}, 1)}
		h.lanes[p.ChatJID] = lane
	}
	lane.users++
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		lane.users--
		if lane.users == 0 {
			delete(h.lanes, p.ChatJID)
		}
		h.mu.Unlock()
	}()
	select {
	case lane.token <- struct{}{}:
		defer func() { <-lane.token }()
	case <-ctx.Done():
		return finish("history loading exceeded the read window; showing cached messages")
	}
	triedPN := false
	visited := map[string]bool{}
	for {
		messages, err = backend.Store().ListMessages(resultCtx, base)
		if err != nil {
			return nil, "", "", err
		}
		if len(messages) >= limit {
			return finish("")
		}
		if ctx.Err() != nil {
			return finish("history loading exceeded the read window; showing cached messages")
		}
		boundary, err := backend.Store().HistoryBoundary(resultCtx, p.ChatJID)
		if err != nil {
			return nil, "", "", err
		}
		headUnknown, err := backend.Store().UnconfirmedHead(resultCtx, p.ChatJID)
		if err != nil {
			return nil, "", "", err
		}
		if boundary != "unknown" && !headUnknown {
			return finish("WhatsApp history boundary: " + boundary + "; returning the available cached matches")
		}
		if err := waitForRead(ctx, backend, p.ChatJID); err != nil {
			return finish("synchronization is incomplete; showing cached messages")
		}
		anchor, err := backend.Store().NextHistoryAnchor(resultCtx, p.ChatJID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return finish("no usable cached anchor is available yet; initial device history may still be arriving")
			}
			return nil, "", "", err
		}
		key := fmt.Sprintf("%s/%t", anchor.ID, triedPN)
		if visited[key] {
			return finish("history transfers made no further progress; showing cached messages")
		}
		visited[key] = true
		batch, err := backend.FetchHistory(ctx, p.ChatJID, !triedPN)
		if err != nil {
			return finish("history loading could not be completed within the read window; showing cached messages")
		}
		if batch.Exhausted {
			return finish("WhatsApp reports no older messages at this history boundary; returning cached matches")
		}
		if batch.Inaccessible {
			return finish("WhatsApp reports older history inaccessible; returning cached matches")
		}
		next, err := backend.Store().NextHistoryAnchor(resultCtx, p.ChatJID)
		if err != nil {
			return nil, "", "", err
		}
		if next.ID == anchor.ID {
			if triedPN || !strings.HasSuffix(p.ChatJID, "@s.whatsapp.net") {
				return finish("completed history transfers made no progress; showing cached messages")
			}
			triedPN = true
		} else {
			triedPN = false
		}
	}
}

func waitForRead(ctx context.Context, backend historyBackend, chat string) error {
	if scoped, ok := backend.(interface {
		WaitReadReadyFor(context.Context, string, ...string) error
	}); ok {
		return scoped.WaitReadReadyFor(ctx, chat, "messages")
	}
	return backend.WaitReadReady(ctx)
}
