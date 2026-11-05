package mcp

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/sealjay/mcp-whatsapp/internal/store"
)

// registerQueryTools wires read-only query tools: list_chats, get_chat,
// list_messages, get_message_context, search_contacts, get_status,
// is_on_whatsapp.
func (s *Server) registerQueryTools() {
	s.registerSearchContacts()
	s.registerListMessages()
	s.registerListChats()
	s.registerGetChat()
	s.registerGetMessageContext()
	s.registerIsOnWhatsApp()
	s.registerGetStatus()
	s.registerPairingStatus()
	s.registerUnreadChats()
	s.registerWaitUpdates()
}

// -- search_contacts --------------------------------------------------------

type searchContactsArgs struct {
	Query string `json:"query"`
}

func (s *Server) registerSearchContacts() {
	tool := mcp.NewTool("search_contacts",
		mcp.WithDescription(snapshotReadPrefix+"Search the cached WhatsApp contact list by case-insensitive substring of name or phone number. Read-only; no side effects. Use is_on_whatsapp to verify whether an unknown phone number is registered. Returns up to 50 indexed contact matches; narrow the query if needed. Returns a JSON array of contact objects (each with JID, push name, full name, and phone)."),
		mcp.WithString("query", mcp.Required(), mcp.Description("case-insensitive substring to match against name or phone")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a searchContactsArgs) (*mcp.CallToolResult, error) {
		return s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			contacts, err := s.client.Store().SearchContacts(ctx, a.Query)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return snapshotResult(contacts)
		}, "", "contacts")
	}))
}

// -- list_messages ----------------------------------------------------------

type listMessagesArgs struct {
	UnreadOnly        bool   `json:"unread_only,omitempty"`
	Cursor            string `json:"cursor,omitempty"`
	Format            string `json:"format,omitempty"`
	Timezone          string `json:"timezone,omitempty"`
	After             string `json:"after,omitempty"`
	Before            string `json:"before,omitempty"`
	SenderPhoneNumber string `json:"sender_phone_number,omitempty"`
	ChatJID           string `json:"chat_jid,omitempty"`
	Query             string `json:"query,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	IncludeContext    *bool  `json:"include_context,omitempty"`
	ContextBefore     int    `json:"context_before,omitempty"`
	ContextAfter      int    `json:"context_after,omitempty"`
}

func (s *Server) registerListMessages() {
	tool := mcp.NewTool("list_messages",
		mcp.WithBoolean("unread_only", mcp.DefaultBool(false), mcp.Description("only incoming messages with known unread identities/read watermarks; phone counts may include messages not identified in cache")),
		mcp.WithString("cursor", mcp.Description("Short next-page bookmark returned by this tool; copy unchanged with the same filters")),
		mcp.WithString("format", mcp.Enum("compact", "detailed"), mcp.DefaultString("compact"), mcp.Description("compact: readable messages without transport IDs (default); detailed: exact message/chat IDs needed for replies, reactions, downloads or diagnostics")),
		mcp.WithString("timezone", mcp.Description("IANA timezone for compact timestamps, e.g. Europe/Madrid; defaults to server TZ, otherwise UTC")),
		mcp.WithDescription("Read WhatsApp messages after local catch-up and automatic history loading for a selected chat. Required history fetches wait for a correlated completed response; timeout or unconfirmed completion returns an error without partial messages. Returns compact local-time messages and an opaque next-page cursor when available. Results describe a local snapshot with explicitly limited coverage, not an authoritative current view of the phone; never interpret an incomplete cache as an empty conversation. Does not send chat messages or mark anything read. Use detailed format only when exact transport IDs are needed."),
		mcp.WithString("after", mcp.Description("ISO-8601 lower bound on message timestamp (exclusive; include timezone)")),
		mcp.WithString("before", mcp.Description("ISO-8601 upper bound on message timestamp (exclusive; include timezone)")),
		mcp.WithString("sender_phone_number", mcp.Description("filter to messages sent by this phone or JID ("+jidDesc+")")),
		mcp.WithString("chat_jid", mcp.Description("filter to messages in this chat ("+jidDesc+")")),
		mcp.WithString("query", mcp.Description("case-insensitive substring to match within message body")),
		mcp.WithNumber("limit", mcp.DefaultNumber(float64(min(20, s.client.ReadPolicy().MaxPage))), mcp.Description("max messages to return (default 20, subject to the configured page resource budget)")),
		mcp.WithBoolean("include_context", mcp.DefaultBool(false), mcp.Description("if true, add surrounding messages to search matches; overlapping windows are deduplicated. Defaults to false for normal chat history")),
		mcp.WithNumber("context_before", mcp.DefaultNumber(1), mcp.Description("messages to include before each match (default 1, capped at 20 server-side)")),
		mcp.WithNumber("context_after", mcp.DefaultNumber(1), mcp.Description("messages to include after each match (default 1, capped at 20 server-side)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(true),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, req mcp.CallToolRequest, a listMessagesArgs) (*mcp.CallToolResult, error) {
		if _, present := req.GetArguments()["page"]; present {
			return mcp.NewToolResultError("page: unsupported; use the returned cursor to continue"), nil
		}
		if s.client != nil {
			chat, err := s.readChatReference(a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			a.ChatJID = chat
		}

		ctx, cancel := context.WithTimeout(ctx, s.client.ReadPolicy().Timeout)
		defer cancel()
		if a.Format != "" && a.Format != "compact" && a.Format != "detailed" {
			return mcp.NewToolResultError("format must be compact or detailed"), nil
		}
		timezone := a.Timezone
		if timezone == "" {
			timezone = s.client.ReadPolicy().Timezone
		}
		if timezone == "" {
			timezone = "UTC"
		}
		if _, err := time.LoadLocation(timezone); err != nil {
			return mcp.NewToolResultError("timezone: unknown IANA timezone"), nil
		}
		// Resource budgets are explicit and independent of WhatsApp quotas.
		limit := a.Limit
		if limit == 0 {
			limit = min(20, s.client.ReadPolicy().MaxPage)
		}
		if limit > s.client.ReadPolicy().MaxPage || limit < 0 {
			return mcp.NewToolResultError("limit exceeds the configured page budget; use pagination"), nil
		}
		ctxBefore := a.ContextBefore
		if ctxBefore > 20 {
			return mcp.NewToolResultError("context_before exceeds the context budget of 20"), nil
		}
		ctxAfter := a.ContextAfter
		if ctxAfter > 20 {
			return mcp.NewToolResultError("context_after exceeds the context budget of 20"), nil
		}
		params := store.ListMessagesParams{
			UnreadOnly:     a.UnreadOnly,
			SenderPhone:    a.SenderPhoneNumber,
			ChatJID:        a.ChatJID,
			Query:          a.Query,
			Limit:          limit,
			IncludeContext: a.IncludeContext != nil && *a.IncludeContext,
			ContextBefore:  ctxBefore,
			ContextAfter:   ctxAfter,
		}
		if a.After != "" {
			t, err := parseTimestamp(a.After)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("invalid 'after': %v", err)), nil
			}
			params.After = t
		}
		if a.Before != "" {
			t, err := parseTimestamp(a.Before)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("invalid 'before': %v", err)), nil
			}
			params.Before = t
		}
		rawCursor, resolveErr := s.client.Store().ResolveBookmark(ctx, "p", filterKey(params), a.Cursor)
		if resolveErr != nil {
			return mcp.NewToolResultError(resolveErr.Error()), nil
		}
		if err := applyCursor(&params, rawCursor); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		msgs, status, cursor, err := s.history.read(ctx, s.client, params)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		domain := "messages"
		if a.UnreadOnly {
			domain = "unread"
		}
		result, readErr := s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			base := params
			base.IncludeContext = false
			msgs, err = s.client.Store().ListMessages(ctx, base)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			cursor = nextCursor(params, msgs)
			if params.IncludeContext {
				msgs, err = s.client.Store().ListMessages(ctx, params)
				if err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
			}
			if a.UnreadOnly {
				overview, overviewErr := s.client.Store().UnreadChats(ctx, a.ChatJID, 100)
				if overviewErr != nil {
					return mcp.NewToolResultError(overviewErr.Error()), nil
				}
				status += " " + overview.Note
			}
			if a.Format == "detailed" {
				text, err := s.client.Store().FormatMessagesDetailed(ctx, msgs, true, timezone)
				if err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				return mcp.NewToolResultText(readingFooter(text, status, "")), nil
			}
			text, err := s.client.Store().FormatMessagesCompact(ctx, msgs, timezone)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(readingFooter(text, status, "")), nil
		}, a.ChatJID, domain)
		if readErr != nil || result == nil || result.IsError || cursor == "" {
			return result, readErr
		}
		bookmark, err := s.client.Store().PutBookmark(ctx, "p", filterKey(params), cursor)
		if err != nil {
			return mcp.NewToolResultError("cursor: could not persist continuation; retry this read"), nil
		}
		result.Content = append(result.Content, mcp.NewTextContent("Next page: cursor="+strconv.Quote(bookmark)+" (reuse the same filters)."))
		return s.checkResponseBudget(result)

	}))
}

// -- list_chats -------------------------------------------------------------

type listChatsArgs struct {
	Query              string `json:"query,omitempty"`
	Limit              int    `json:"limit,omitempty"`
	Page               int    `json:"page,omitempty"`
	IncludeLastMessage *bool  `json:"include_last_message,omitempty"`
	SortBy             string `json:"sort_by,omitempty"`
}

func (s *Server) registerListChats() {
	tool := mcp.NewTool("list_chats",
		mcp.WithDescription(snapshotReadPrefix+"List cached WhatsApp chats (1:1 and group), optionally filtered by name substring and sorted by recency or alphabetic name. Read-only; no side effects. Use get_chat for a single chat by JID, list_groups for groups only. Returns a JSON array of chat objects (each with JID, name, last-message metadata when requested)."),
		mcp.WithString("query", mcp.Description("case-insensitive substring to match against chat name")),
		mcp.WithNumber("limit", mcp.DefaultNumber(float64(min(20, s.client.ReadPolicy().MaxPage))), mcp.Description("max chats to return (default 20)")),
		mcp.WithNumber("page", mcp.DefaultNumber(0), mcp.Description("zero-based page index for paging through results (default 0)")),
		mcp.WithBoolean("include_last_message", mcp.DefaultBool(true), mcp.Description("if true, include each chat's most recent message in the result (defaults to true)")),
		mcp.WithString("sort_by", mcp.DefaultString("last_active"), mcp.Enum("last_active", "name"), mcp.Description("sort order: `last_active` (most-recent first, default) or `name` (alphabetic)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a listChatsArgs) (*mcp.CallToolResult, error) {
		limit, err := s.pageBudget(a.Limit, a.Page, 20)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		a.Limit = limit

		return s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			sortBy := a.SortBy
			if sortBy == "" {
				sortBy = "last_active"
			}
			if sortBy != "last_active" && sortBy != "name" {
				return mcp.NewToolResultError("sort_by must be last_active or name"), nil
			}
			include := a.IncludeLastMessage == nil || *a.IncludeLastMessage
			chats, err := s.client.Store().ListChats(ctx, a.Query, a.Limit, a.Page, include, sortBy)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			loc, _ := time.LoadLocation(s.client.ReadPolicy().Timezone)
			for i := range chats {
				chats[i].LastMessageTime = chats[i].LastMessageTime.In(loc)
			}
			return snapshotResult(chats)
		}, "", "messages")
	}))
}

// -- get_chat ---------------------------------------------------------------

type getChatArgs struct {
	ChatJID            string `json:"chat_jid"`
	IncludeLastMessage *bool  `json:"include_last_message,omitempty"`
}

func (s *Server) registerGetChat() {
	tool := mcp.NewTool("get_chat",
		mcp.WithDescription(snapshotReadPrefix+"Fetch metadata for a single cached chat by JID. Read-only; no side effects. Use list_chats to discover chat JIDs, or get_group_info for live group metadata. Returns a JSON object describing the chat (JID, name, last-message metadata when requested), or the JSON literal `null` when the chat is not in the cache."),
		mcp.WithString("chat_jid", mcp.Required(), mcp.Description(jidDesc)),
		mcp.WithBoolean("include_last_message", mcp.DefaultBool(true), mcp.Description("if true, include the chat's most recent message in the result (defaults to true)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a getChatArgs) (*mcp.CallToolResult, error) {
		if s.client != nil {
			chat, err := s.readChatReference(a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			a.ChatJID = chat
		}

		return s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			include := a.IncludeLastMessage == nil || *a.IncludeLastMessage
			chat, err := s.client.Store().GetChat(ctx, a.ChatJID, include)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			if chat == nil {
				return snapshotResult(nil)
			}
			loc, _ := time.LoadLocation(s.client.ReadPolicy().Timezone)
			chat.LastMessageTime = chat.LastMessageTime.In(loc)
			return snapshotResult(chat)
		}, a.ChatJID, "messages")
	}))
}

// -- get_message_context ----------------------------------------------------

type getMessageContextArgs struct {
	ChatJID   string `json:"chat_jid,omitempty"`
	MessageID string `json:"message_id"`
	Before    int    `json:"before,omitempty"`
	After     int    `json:"after,omitempty"`
}

func (s *Server) registerGetMessageContext() {
	tool := mcp.NewTool("get_message_context",
		mcp.WithDescription(snapshotReadPrefix+"Fetch a specific cached message and the surrounding messages in its chat. Read-only; no side effects. Use list_messages for searching across many chats. Returns a JSON object with the target message and arrays of messages before and after it."),
		mcp.WithString("chat_jid", mcp.Description("Chat containing the message; required when the message ID occurs in multiple chats")),
		mcp.WithString("message_id", mcp.Required(), mcp.Description("WhatsApp message ID of the target message (use `message_id` from list_messages)")),
		mcp.WithNumber("before", mcp.DefaultNumber(5), mcp.Description("messages to fetch before the target (default 5; non-positive values fall back to the default)")),
		mcp.WithNumber("after", mcp.DefaultNumber(5), mcp.Description("messages to fetch after the target (default 5; non-positive values fall back to the default)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a getMessageContextArgs) (*mcp.CallToolResult, error) {
		if s.client != nil {
			chat, err := s.readChatReference(a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			a.ChatJID = chat
		}

		return s.cachedRead(ctx, func(ctx context.Context) (*mcp.CallToolResult, error) {
			before, after := a.Before, a.After
			if before <= 0 {
				before = 5
			}
			if after <= 0 {
				after = 5
			}
			if before > 100 {
				return mcp.NewToolResultError("before exceeds the context budget of 100"), nil
			}
			if after > 100 {
				return mcp.NewToolResultError("after exceeds the context budget of 100"), nil
			}
			ctxResult, err := s.client.Store().GetMessageContext(ctx, a.MessageID, before, after, a.ChatJID)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			_, ctxResult.Message.Mentions, ctxResult.Message.MentionsMe = s.client.Store().RenderMentions(ctx, ctxResult.Message, true)
			loc, _ := time.LoadLocation(s.client.ReadPolicy().Timezone)
			ctxResult.Message.Timestamp = ctxResult.Message.Timestamp.In(loc)
			for i := range ctxResult.Before {
				ctxResult.Before[i].Timestamp = ctxResult.Before[i].Timestamp.In(loc)
			}
			for i := range ctxResult.After {
				ctxResult.After[i].Timestamp = ctxResult.After[i].Timestamp.In(loc)
			}
			return snapshotResult(ctxResult)
		}, a.ChatJID, "messages")
	}))
}

// -- is_on_whatsapp ---------------------------------------------------------

type isOnWhatsAppArgs struct {
	Phones []string `json:"phones"`
}

func (s *Server) registerIsOnWhatsApp() {
	tool := mcp.NewTool("is_on_whatsapp",
		mcp.WithDescription("Query WhatsApp servers to check which of the supplied phone numbers are registered on WhatsApp; the queried users are not notified. Read-only with no chat side effects. Use before send_message when you only have a phone number and need to confirm the contact exists. Returns a JSON object keyed by input phone, each value `{is_in: bool, jid: string, verified_name?: string}` (or similar)."),
		mcp.WithArray("phones", mcp.Required(), mcp.Items(map[string]any{"type": "string"}), mcp.Description("phone numbers to check; digits only with no `+` prefix, spaces, or punctuation (e.g. `447700900000`); must be non-empty")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(true),
	)
	s.registerTool(tool, mcp.NewTypedToolHandler(func(ctx context.Context, _ mcp.CallToolRequest, a isOnWhatsAppArgs) (*mcp.CallToolResult, error) {
		if len(a.Phones) == 0 {
			return mcp.NewToolResultError("phones must not be empty"), nil
		}
		m, err := s.client.IsOnWhatsApp(ctx, a.Phones)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return resultJSON(m)
	}))
}

// -- get_status -------------------------------------------------------------

func (s *Server) registerGetStatus() {
	tool := mcp.NewTool("get_status",
		mcp.WithDescription("Report whether the embedded WhatsApp bridge is connected and which account it is paired with. Read-only; no side effects. Call this first when other tools fail with auth or connection errors. Returns a JSON object `{connected, paired, own_jid?, own_phone?, hint?}` — `hint` includes the URL of the local pairing UI when not yet paired."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		status := map[string]any{
			"connected": s.client.IsConnected(),
		}
		if wa := s.client.WA(); wa != nil && wa.Store != nil && wa.Store.ID != nil {
			status["own_jid"] = wa.Store.ID.String()
			status["own_phone"] = wa.Store.ID.User
			status["paired"] = true
		} else {
			status["paired"] = false
			status["hint"] = "Not paired. Open /pair on this server in a browser to scan QR (default: http://127.0.0.1:8765/pair)."
		}
		return resultJSON(status)
	})
}

// -- pairing_status ---------------------------------------------------------

// registerPairingStatus exposes the live device-pairing state as a structured
// "setup_state" envelope. Where get_status is human-oriented, this returns a
// machine-stable shape keyed by `state` so a polling supervisor (e.g. the Den
// daemon) can surface the WhatsApp linking QR to its own clients.
//
// The bound client is consulted only once the cache reports a paired device,
// keeping the awaiting_qr / error envelopes free of any whatsmeow dependency.
func (s *Server) registerPairingStatus() {
	tool := mcp.NewTool("pairing_status",
		mcp.WithDescription("Report the WhatsApp device-pairing state as a structured `setup_state` envelope for programmatic supervisors that surface the linking QR to their own clients (e.g. a polling daemon). Read-only; no side effects. Returns a JSON object `{type:\"setup_state\", state, …}` where `state` is `ready` (paired and connected; adds own_jid/own_phone), `awaiting_qr` (unpaired; adds qr_payload when a pairing code is cached), or `error` (pairing cache unavailable)."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithOpenWorldHintAnnotation(false),
	)
	s.registerTool(tool, func(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var ownJID, ownPhone string
		ready := false
		// Short-circuits before touching s.client unless we are paired, so a
		// nil cache or unpaired state never dereferences the WhatsApp client.
		if s.cache != nil && s.cache.Paired() && s.client.IsConnected() {
			if wa := s.client.WA(); wa != nil && wa.Store != nil && wa.Store.ID != nil {
				ownJID = wa.Store.ID.String()
				ownPhone = wa.Store.ID.User
				ready = true
			}
		}

		resp := map[string]any{"type": "setup_state"}
		switch {
		case ready:
			resp["state"] = "ready"
			resp["own_jid"] = ownJID
			resp["own_phone"] = ownPhone
		case s.cache == nil:
			resp["state"] = "error"
			resp["detail"] = "pairing cache unavailable"
		case s.cache.QR() != "":
			resp["state"] = "awaiting_qr"
			resp["qr_payload"] = s.cache.QR()
			resp["detail"] = "Scan with WhatsApp → Linked devices"
		default:
			resp["state"] = "awaiting_qr"
			resp["detail"] = "Generating pairing code…"
		}
		return resultJSON(resp)
	})
}

func readingFooter(text, status, cursor string) string {
	if status != "" {
		text += "\nHistory: " + status + "\n"
	}
	if cursor != "" {
		text += "\nNext: " + cursor + "\n"
	}
	return text
}
