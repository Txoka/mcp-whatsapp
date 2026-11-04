package mcp

// This layer owns retrieval policy. Tool handlers do not manage WhatsApp
// anchors, LID fallback, asynchronous delivery, or retry throttling.
import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/client"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

type historyBackend interface {
	Store() *store.Store
	RequestHistorySync(context.Context, string, time.Time, string, int, bool) (string, error)
}

type syncAttempt struct {
	at     time.Time
	anchor string
	lid    bool
}
type historyReader struct {
	mu       sync.Mutex
	attempts map[string]syncAttempt
}

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
	if p.Page != 0 {
		return fmt.Errorf("cursor: cannot combine with page")
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

// read waits at most three seconds for one automatic history batch. A
// process-wide bound and per-chat cooldown prevent concurrent query storms.
// No chat messages, read receipts or group changes are sent.
func (h *historyReader) read(ctx context.Context, backend historyBackend, p store.ListMessagesParams, auto bool) ([]store.Message, string, string, error) {
	base := p
	base.IncludeContext = false
	messages, err := backend.Store().ListMessages(ctx, base)
	if err != nil {
		return nil, "", "", err
	}
	limit := p.Limit
	if limit <= 0 {
		limit = 20
	}
	status := ""
	if auto && !p.UnreadOnly && p.ChatJID != "" && len(messages) < limit {
		anchor, _, _, anchorErr := backend.Store().GetOldestMessage(p.ChatJID)
		if anchorErr != nil {
			status = "Initial history is not available yet; a new incoming message or device sync is needed."
		} else {
			now := time.Now()
			h.mu.Lock()
			if h.attempts == nil {
				h.attempts = map[string]syncAttempt{}
			}
			previous := h.attempts[p.ChatJID]
			// Retry the same unchanged cursor using PN after a cooldown; this handles
			// older history spanning WhatsApp's LID migration without model input.
			lid := true
			if previous.anchor == anchor {
				lid = !previous.lid
			}
			recent := now.Sub(previous.at) < 30*time.Second
			globalRecent := false
			for _, attempt := range h.attempts {
				if now.Sub(attempt.at) < time.Second {
					globalRecent = true
					break
				}
			}
			if !recent && !globalRecent {
				h.attempts[p.ChatJID] = syncAttempt{now, anchor, lid}
			}
			h.mu.Unlock()
			if recent || globalRecent {
				status = "Showing cached history; automatic history loading is rate-limited."
			} else {
				waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, syncErr := backend.RequestHistorySync(waitCtx, p.ChatJID, time.Time{}, client.AnchorOldest, 50, lid)
				if syncErr != nil {
					status = "Showing cached history; additional history could not be requested."
				} else {
					status = "Showing cached history; additional history is loading. Query again later if needed."
					ticker := time.NewTicker(150 * time.Millisecond)
					initialAnchor := anchor
				wait:
					for {
						select {
						case <-waitCtx.Done():
							break wait
						case <-ticker.C:
							current, queryErr := backend.Store().ListMessages(waitCtx, base)
							if queryErr != nil {
								break wait
							}
							messages = current
							oldest, _, _, _ := backend.Store().GetOldestMessage(p.ChatJID)
							if oldest != initialAnchor {
								status = "Additional history loaded; older history may still be available."
								break wait
							}
						}
					}
					ticker.Stop()
				}
				cancel()
			}
		}
	}
	cursor := nextCursor(p, messages)
	if p.IncludeContext {
		messages, err = backend.Store().ListMessages(ctx, p)
	}
	return messages, status, cursor, err
}
