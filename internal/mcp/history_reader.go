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

// read waits for catch-up and required history batches. Failed or unconfirmed
// sync never becomes a successful partial page. No polling or model retries.
func (h *historyReader) read(ctx context.Context, backend historyBackend, p store.ListMessagesParams) ([]store.Message, string, string, error) {
	budget := client.ReadTimeout
	if configured, ok := backend.(interface{ ReadPolicy() client.ReadPolicy }); ok {
		budget = configured.ReadPolicy().Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if err := waitForRead(ctx, backend, p.ChatJID); err != nil {
		return nil, "", "", err
	}
	base := p
	base.IncludeContext = false
	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}
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
		return nil, "", "", fmt.Errorf("history synchronization incomplete: %w; results withheld", ctx.Err())
	}
	status := snapshotNote
	triedPN := false
	for {
		if err := waitForRead(ctx, backend, p.ChatJID); err != nil {
			return nil, "", "", err
		}
		messages, err := backend.Store().ListMessages(ctx, base)
		if err != nil {
			return nil, "", "", err
		}
		covered := false
		if p.ChatJID != "" {
			covered, err = backend.Store().CoversMessages(ctx, p.ChatJID, messages)
			if err != nil {
				return nil, "", "", err
			}
		}
		if p.ChatJID == "" || p.UnreadOnly || (len(messages) >= limit && covered) {
			cursor := nextCursor(p, messages)
			if p.IncludeContext {
				messages, err = backend.Store().ListMessages(ctx, p)
			}
			return messages, status, cursor, err
		}
		boundary, err := backend.Store().HistoryBoundary(ctx, p.ChatJID)
		if err != nil {
			return nil, "", "", err
		}
		unknownHead, err := backend.Store().UnconfirmedHead(ctx, p.ChatJID)
		if err != nil {
			return nil, "", "", err
		}
		if boundary != "unknown" && !unknownHead {
			if boundary == "inaccessible" && len(messages) < limit {
				return nil, "", "", fmt.Errorf("history unavailable: WhatsApp reports older messages inaccessible; incomplete page withheld")
			}
			if len(messages) > 0 && !covered {
				return nil, "", "", fmt.Errorf("history coverage unknown for retained messages; results withheld")
			}
			status += " WhatsApp history boundary: " + boundary + "."
			if p.IncludeContext {
				messages, err = backend.Store().ListMessages(ctx, p)
			}
			return messages, status, nextCursor(p, messages), err
		}
		anchorKey, err := backend.Store().NextHistoryAnchor(ctx, p.ChatJID)
		anchor := anchorKey.ID
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, "", "", err
			}
			return nil, "", "", fmt.Errorf("history unavailable: no usable cached anchor for this chat; initial device history is required; results withheld")
		}
		batch, err := backend.FetchHistory(ctx, p.ChatJID, !triedPN)
		if err != nil {
			return nil, "", "", fmt.Errorf("history synchronization incomplete: %w; partial results withheld", err)
		}
		if err := waitForRead(ctx, backend, p.ChatJID); err != nil {
			return nil, "", "", err
		}
		messages, err = backend.Store().ListMessages(ctx, base)
		if err != nil {
			return nil, "", "", err
		}
		if batch.Exhausted || batch.Inaccessible {
			confirmed, err := backend.Store().CoversMessages(ctx, p.ChatJID, messages)
			if err != nil {
				return nil, "", "", err
			}
			if (batch.Inaccessible && len(messages) < limit) || (len(messages) > 0 && !confirmed) {
				return nil, "", "", fmt.Errorf("history unavailable: requested page cannot be established from confirmed accessible transfers; partial results withheld")
			}
			if batch.Inaccessible {
				status += " WhatsApp reports older history is inaccessible to this linked device."
			} else {
				status += " WhatsApp reports no older messages remain on the primary for this history boundary."
			}
			cursor := "" // this request reached an explicit boundary, not an inferred empty page
			if len(messages) >= limit {
				cursor = nextCursor(p, messages)
			}
			if p.IncludeContext {
				messages, err = backend.Store().ListMessages(ctx, p)
			}
			return messages, status, cursor, err
		}
		if len(messages) >= limit {
			confirmed, err := backend.Store().CoversMessages(ctx, p.ChatJID, messages)
			if err != nil {
				return nil, "", "", err
			}
			if confirmed {
				continue
			}
		}
		// A full cached page is not evidence that this transfer made progress.
		// Recheck coverage at the top, but always detect an unchanged frontier.
		nextAnchor, err := backend.Store().NextHistoryAnchor(ctx, p.ChatJID)
		oldest := nextAnchor.ID
		if err != nil {
			return nil, "", "", err
		}
		if oldest == anchor {
			if triedPN || !strings.HasSuffix(p.ChatJID, "@s.whatsapp.net") {
				return nil, "", "", fmt.Errorf("history incomplete: completed batches made no progress and no end-of-history was confirmed; partial results withheld")
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
