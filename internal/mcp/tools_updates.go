package mcp

import (
	"context"
	"encoding/json"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"math"
	"time"
)

type unreadChatsArgs struct {
	Page    int    `json:"page,omitempty"`
	ChatJID string `json:"chat_jid,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

func (s *Server) registerUnreadChats() {
	tool := mcp.NewTool("list_unread_chats",
		mcp.WithDescription("List chats with known unread messages or a manual unread marker, including counts and how complete they are. Unknown counts are null, never invented zeros. Some phone counters may lack individually identified cached messages. This only reads local synchronized state and does not mark anything read."),
		mcp.WithString("chat_jid", mcp.Description("optional person or group chat to inspect")),
		mcp.WithInteger("page", mcp.Min(0), mcp.DefaultNumber(0), mcp.Description("zero-based result page; use next_page when has_more is true")),
		mcp.WithInteger("limit", mcp.Min(1), mcp.Max(float64(s.client.ReadPolicy().MaxPage)), mcp.DefaultNumber(float64(min(100, s.client.ReadPolicy().MaxPage))), mcp.Description("maximum chats returned within the configured page budget")),
		mcp.WithReadOnlyHintAnnotation(true), mcp.WithDestructiveHintAnnotation(false), mcp.WithIdempotentHintAnnotation(true), mcp.WithOpenWorldHintAnnotation(false))
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a unreadChatsArgs) (*mcp.CallToolResult, error) {
		if s.client != nil {
			chat, err := s.readChatReference(a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			a.ChatJID = chat
		}

		limit, err := s.pageBudget(a.Limit, a.Page, 100)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		a.Limit = limit

		return s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			result, err := s.client.Store().UnreadChats(ctx, a.ChatJID, a.Limit, a.Page)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return snapshotResult(result)
		}, a.ChatJID, "unread")
	}))
}

type waitUpdatesArgs struct {
	ChatJID string `json:"chat_jid,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Cursor  string `json:"cursor,omitempty"`
	Timeout *int64 `json:"timeout_seconds,omitempty"`
}

func (s *Server) registerWaitUpdates() {
	tool := mcp.NewTool("wait_for_updates",
		mcp.WithDescription("Wait for new incoming messages or synchronized read-state changes, globally or for one person/group. With no cursor, waits for events after this call starts. Reuse the returned cursor to avoid gaps between calls. Returns updates or an explicit timeout. Watches only supported WhatsApp events while the service is running, not all phone notifications. No read receipts, presence changes or messages are sent. Waits for one hour by default; -1 waits until an event or cancellation. Transport/client deadlines still apply. This is not an autonomous background notification service."),
		mcp.WithString("chat_jid", mcp.Description("optional person or group chat; omit to watch all chats")),
		mcp.WithString("kind", mcp.Enum("message", "read_state", "any"), mcp.DefaultString("message"), mcp.Description("message: incoming messages; read_state: unread/read changes; any: either supported event")),
		mcp.WithString("cursor", mcp.Description("Short resume bookmark from a previous wait; copy unchanged with the same chat and kind; omit to start waiting now")),
		mcp.WithInteger("timeout_seconds", mcp.Min(-1), mcp.DefaultNumber(3600), mcp.Description("wait duration in seconds, default 3600; 0 checks immediately; -1 waits until an event or cancellation")),
		mcp.WithReadOnlyHintAnnotation(true), mcp.WithDestructiveHintAnnotation(false), mcp.WithIdempotentHintAnnotation(true), mcp.WithOpenWorldHintAnnotation(false))
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a waitUpdatesArgs) (*mcp.CallToolResult, error) {
		if s.client != nil {
			chat, err := s.readChatReference(a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			a.ChatJID = chat
		}

		kind := a.Kind
		if kind == "" {
			kind = "message"
		}
		if kind != "message" && kind != "read_state" && kind != "any" {
			return mcp.NewToolResultError("kind: must be message, read_state or any"), nil
		}
		seconds := int64(3600)
		if a.Timeout != nil {
			seconds = *a.Timeout
		}
		if seconds < -1 || seconds > math.MaxInt64/int64(time.Second) {
			return mcp.NewToolResultError("timeout_seconds: use -1 for indefinite, 0 to check now, or a positive representable duration"), nil
		}
		duration := store.InfiniteWait
		if seconds >= 0 {
			duration = time.Duration(seconds) * time.Second
		}
		scopeBytes, _ := json.Marshal([]string{a.ChatJID, kind})
		scope := string(scopeBytes)
		rawCursor, err := s.client.Store().ResolveBookmark(ctx, "u", scope, a.Cursor)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		result, err := s.client.Store().WaitUpdates(ctx, a.ChatJID, kind, rawCursor, duration)
		if err == nil {
			result.Cursor, err = s.client.Store().PutBookmark(ctx, "u", scope, result.Cursor)
		}
		connected := s.client.IsConnected()
		result.Connected = &connected
		return toolResult(result, err)
	}))
}
