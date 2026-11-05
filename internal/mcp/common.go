// Package mcp — style guide for tool authors.
//
// When writing or editing MCP tool descriptions in this package, follow these
// conventions so the LLM-facing surface stays consistent:
//
//  1. Tool-level descriptions are full sentences and end with a period. Lead
//     with what the tool does; add disambiguation ("use X when Y") as needed.
//  2. Argument descriptions are sentence fragments (no terminal period) —
//     they render as schema titles, not prose. Keep them short.
//  3. Error strings returned from handlers use the `<field>: <reason>` shape
//     (e.g. `"chat_jid: required"`, `"audio conversion failed: ..."`). Prefer
//     the shared helpers below to emit them so the shape stays uniform.
//  4. Tools that read only from the local SQLite cache are prefixed with
//     `(waits for local catch-up; returns a coverage-limited local snapshot) ` so callers know they
//     must wait for a live session to finish local catch-up.
//  5. Every `chat_jid`, `recipient`, `sender_jid`, and `sender_phone_number`
//     argument reuses the shared `jidDesc` / `recipientDesc` constants so
//     callers always see the same JID-shape hint.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mark3labs/mcp-go/mcp"
	"time"
)

// jidDesc describes the expected shape of a single-chat or group JID.
// Used for `chat_jid`, `sender_jid`, and `sender_phone_number` arguments.
const jidDesc = "WhatsApp JID: individual as `<digits>@s.whatsapp.net` or bare phone digits, group as `<digits>-<timestamp>@g.us`"

// recipientDesc describes the expected shape of a send-target. Callers may
// pass either a bare phone number or a fully qualified JID (individual or
// group). Bare numbers must be digits only — no `+` prefix, spaces, or
// punctuation; a leading `+` is stripped as a courtesy but everything else
// is rejected with a clear error rather than misrouted silently.
const recipientDesc = "Send target: digits only (E.164 without `+`, no spaces or punctuation); or `<digits>@s.whatsapp.net`; or group `<digits>-<timestamp>@g.us`"

// snapshotReadPrefix describes cache-backed reads gated on connection catch-up.
const snapshotReadPrefix = "(waits for local catch-up; returns a coverage-limited local snapshot) "

// requireNonEmpty returns a standard validation error when value is the empty
// string; otherwise it returns nil so callers can use the idiom:
//
//	if r := requireNonEmpty("chat_jid", a.ChatJID); r != nil { return r, nil }
//
// Note: whitespace-only values are NOT treated as empty — callers that want
// that behaviour should trim the string themselves before calling this.
func requireNonEmpty(name, value string) *mcp.CallToolResult {
	if value == "" {
		return mcp.NewToolResultError(name + ": required")
	}
	return nil
}

// toolResult is a small wrapper that funnels the two common success/failure
// shapes through a single call site:
//
//   - on error, it returns `mcp.NewToolResultError(err.Error()), nil`
//   - on success, it JSON-serialises payload via resultJSON
//
// Handlers that only ever return text (not JSON) should keep using
// mcp.NewToolResultText directly — forcing this helper there reduces clarity.
func toolResult(payload any, err error) (*mcp.CallToolResult, error) {
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return resultJSON(payload)
}

// snapshotResult preserves the JSON data block while making coverage limits explicit.
func snapshotResult(value any) (*mcp.CallToolResult, error) {
	result, err := resultJSON(value)
	if err == nil && result != nil && !result.IsError {
		result.Content = append(result.Content, mcp.NewTextContent(snapshotNote))
	}
	return result, err
}

func (s *Server) cachedRead(ctx context.Context, read func(context.Context) (*mcp.CallToolResult, error), scope ...string) (*mcp.CallToolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.client.ReadPolicy().Timeout)
	defer cancel()
	chat := ""
	if len(scope) > 0 {
		chat = scope[0]
	}
	domain := "all"
	if len(scope) > 1 {
		domain = scope[1]
	}
	if err := s.client.WaitReadReadyFor(ctx, chat, domain); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	var result *mcp.CallToolResult
	snapshot, err := s.client.Store().ReadSnapshot(ctx, func(ctx context.Context) error { var err error; result, err = read(ctx); return err })
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if result == nil {
		return mcp.NewToolResultError("read returned no result"), nil
	}
	if !result.IsError {
		result.Content = append(result.Content, mcp.NewTextContent(fmt.Sprintf("Snapshot v%d at %s", snapshot.Watermark, snapshot.At.Format(time.RFC3339))))

	}
	return s.checkResponseBudget(result)
}

func (s *Server) checkResponseBudget(result *mcp.CallToolResult) (*mcp.CallToolResult, error) {
	if result == nil || result.IsError {
		return result, nil
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len(raw) > s.client.ReadPolicy().MaxResponseBytes {
		return mcp.NewToolResultError("response resource budget exceeded; narrow the query or request a smaller page"), nil
	}
	return result, nil
}

func (s *Server) pageBudget(limit, page, defaultLimit int) (int, error) {
	if limit == 0 {
		limit = min(defaultLimit, s.client.ReadPolicy().MaxPage)
	}
	if limit < 1 || limit > s.client.ReadPolicy().MaxPage {
		return 0, fmt.Errorf("limit outside configured page budget; use pagination")
	}
	if page < 0 || page > int(^uint(0)>>1)/limit {
		return 0, fmt.Errorf("page outside supported offset range")
	}
	return limit, nil
}

func (s *Server) readChatReference(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	chat := normalizeRecipientToChatJID(raw)
	if chat == "" {
		return "", fmt.Errorf("chat_jid: invalid phone or JID")
	}
	return s.client.Store().ResolveLIDToJID(chat), nil
}
