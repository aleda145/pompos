// Package mcpserver adapts the ingestion development workflow to MCP.
package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pompos/internal/agent"
)

func New(service *agent.Service, operations Operations) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "pompos", Version: "1.0.0"}, &mcp.ServerOptions{Instructions: agent.MCPInstructions})
	no := false
	mcp.AddTool(server, &mcp.Tool{Name: "new_chat", Description: "Start developing a new ingestion in Pompos, then call context to discover destinations. Execution and files are managed on the server; no local repository inspection or setup is needed.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &no, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			Title string `json:"title"`
		}) (*mcp.CallToolResult, any, error) {
			v, err := service.NewMCPChat(input.Title)
			if err != nil {
				return nil, nil, err
			}
			return nil, chatResult(v), nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "list_chats", Description: "List durable MCP conversations to resume after reconnecting.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct{}) (*mcp.CallToolResult, any, error) {
			chats, err := service.ListSessions()
			if err != nil {
				return nil, nil, err
			}
			result := []agent.SessionSummary{}
			for _, chat := range chats {
				v, err := service.Load(chat.ID)
				if err != nil {
					return nil, nil, err
				}
				if v.External {
					result = append(result, chat)
				}
			}
			return nil, map[string]any{"chats": result}, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "get_chat", Description: "Resume a durable draft: returns code, loading settings and validation. Set include_history only when recent messages are needed. Ask unresolved questions directly in this client.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			SessionID      string `json:"session_id"`
			IncludeHistory bool   `json:"include_history,omitempty"`
		}) (*mcp.CallToolResult, any, error) {
			v, err := service.Load(input.SessionID)
			if err != nil {
				return nil, nil, err
			}
			if !v.External {
				return nil, nil, errors.New("MCP conversation not found")
			}
			return nil, fullChatResult(v, input.IncludeHistory), nil
		})
	addIngestionTools(server, service, operations)
	for _, definition := range agent.MCPToolDefinitions() {
		function := definition["function"].(map[string]any)
		name := function["name"].(string)
		schema := function["parameters"].(map[string]any)
		schema["properties"].(map[string]any)["session_id"] = map[string]any{"type": "string", "pattern": `^[a-zA-Z0-9_-]{1,80}$`}
		schema["required"] = append(schema["required"].([]string), "session_id")
		schema["additionalProperties"] = false
		// Probes run arbitrary Python with network access, so retain conservative
		// annotations. Other tools only change drafts or make public research calls.
		annotations := &mcp.ToolAnnotations{}
		if name != "test_script" && name != "validate_ingestion" {
			annotations.DestructiveHint = &no
		}
		mcp.AddTool(server, &mcp.Tool{Name: name, Description: developmentDescription(name, function["description"].(string)), InputSchema: schema, Annotations: annotations},
			func(ctx context.Context, req *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
				id, _ := input["session_id"].(string)
				delete(input, "session_id")
				arguments, err := json.Marshal(input)
				if err != nil {
					return nil, nil, err
				}
				output, err := service.MCPCall(ctx, id, name, arguments)
				if err != nil {
					return nil, nil, err
				}
				v, err := service.Load(id)
				if err != nil {
					return nil, nil, err
				}
				result := chatResult(v)
				result["result"] = toolOutput(name, output)
				if name == "context" {
					result["credentials_path"] = "/secrets"
				}
				return nil, result, nil
			})
	}
	return server
}

func chatResult(v agent.Session) map[string]any {
	return map[string]any{
		"session_id": v.ID,
		"state":      map[string]any{"tested": v.TestedDigest != "", "loading": v.Loading, "validated": v.Validation != nil, "ready": v.Ready, "published_id": v.PublishedID},
	}
}

func fullChatResult(v agent.Session, includeHistory bool) map[string]any {
	messages := []agent.Message{}
	for _, m := range v.Messages {
		if includeHistory && m.Role != "system" {
			messages = append(messages, m)
		}
	}
	if len(messages) > 20 {
		messages = messages[len(messages)-20:]
	}
	v.Messages = messages
	// Legacy web handoffs are not actionable through the direct MCP interface.
	v.Pending = nil
	return map[string]any{"session_id": v.ID, "review_path": "/chat/" + v.ID, "credentials_path": "/secrets", "session": v}
}

func toolOutput(name, output string) any {
	if name == "test_script" {
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(line, "POMPOS_PROBE_RESULT=") {
				output = strings.TrimPrefix(line, "POMPOS_PROBE_RESULT=")
				break
			}
		}
	}
	var result any
	if json.Unmarshal([]byte(output), &result) != nil {
		return output
	}
	if name == "context" {
		if object, ok := result.(map[string]any); ok {
			if draft, ok := object["draft"].(map[string]any); ok {
				delete(draft, "code")
			}
		}
	}
	return result
}

func developmentDescription(name, fallback string) string {
	switch name {
	case "context":
		return "Discover configured destinations, source secret names and draft settings on the Pompos server. Use this instead of inspecting local files or databases. Missing credential values must be entered by the user at credentials_path; refresh context afterward."
	case "write_script":
		return "Write the Python extractor draft on the Pompos server; no local files needed. Invalidates probe and validation. Existing configure_loading settings take precedence for the same source and destination; use configure_loading to change them. After saving, writing starts a separate ingestion."
	}
	return fallback
}
