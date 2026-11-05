package mcp

import (
	"context"
	"github.com/mark3labs/mcp-go/mcp"
	"testing"
)

func TestDeleteRequiresExplicitScope(t *testing.T) {
	s := NewServer(nil, nil)
	if _, ok := s.MCP().ListTools()["request_sync"]; ok {
		t.Fatal("manual sync exposed")
	}
	for _, scope := range []string{"", "all", "ME"} {
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{"scope": scope}
		res, err := s.MCP().ListTools()["delete_message"].Handler(context.Background(), req)
		if err != nil || !res.IsError {
			t.Fatalf("scope %q not rejected: %v", scope, err)
		}
	}
}
