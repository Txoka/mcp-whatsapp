package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

// registerTool records authorized mutating invocations before the handler runs.
// Approval remains the client's responsibility; annotations are not enforcement.
// Specific message operations also track their exact transport/target keys.
func (s *Server) registerTool(tool mcp.Tool, handler server.ToolHandlerFunc) {
	readonly := tool.Annotations.ReadOnlyHint != nil && *tool.Annotations.ReadOnlyHint
	if readonly {
		s.mcp.AddTool(tool, handler)
		return
	}
	s.mcp.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if s.client == nil {
			return handler(ctx, req)
		} // schema-only test server
		idBytes := make([]byte, 16)
		if _, err := rand.Read(idBytes); err != nil {
			return mcp.NewToolResultError("cannot allocate operation ID; handler not executed"), nil
		}
		id := hex.EncodeToString(idBytes)
		args := req.GetArguments()
		str := func(key string) string { value, _ := args[key].(string); return value }
		chat := str("chat_jid")
		if chat == "" {
			chat = str("recipient")
		}
		if _, _, err := s.client.Store().ReserveOperation(ctx, store.Operation{ID: id, Kind: "tool:" + tool.Name, Chat: chat, MessageID: str("message_id")}); err != nil {
			return mcp.NewToolResultError("operation journal unavailable; handler not executed"), nil
		}
		claimed, err := s.client.Store().BeginSubmission(ctx, id)
		if err != nil || !claimed {
			return mcp.NewToolResultError("operation claim failed; handler not executed"), nil
		}
		result, callErr := handler(ctx, req)
		state := "accepted"
		if callErr != nil || result == nil || result.IsError {
			state = "unknown"
		}
		// A generic tool-level failure may follow a side effect. Never classify it
		// as not-sent or replay it automatically. Exact child operations may later
		// establish a stronger outcome.
		if err := s.client.Store().SetOperation(context.Background(), id, state, ""); err != nil {
			if result != nil {
				result.Content = append(result.Content, mcp.NewTextContent(fmt.Sprintf("Outcome journal incomplete for operation %s; do not automatically repeat this write", id)))
			}
		}
		return result, callErr
	})
}
