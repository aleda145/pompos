package mcpserver

import (
	"context"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pompos/internal/agent"
	"pompos/internal/ingestion"
	runnerpython "pompos/internal/runner/python"
)

// Operations are the same publication and execution operations used by the UI.
type Operations struct {
	Save     func(context.Context, string) (string, error)
	List     func(context.Context) ([]ingestion.Ingestion, error)
	Get      func(context.Context, string) (ingestion.Ingestion, error)
	Run      func(context.Context, string) error
	Schedule func(context.Context, string, string) error
	Preview  func(context.Context, string) (runnerpython.TablePreview, error)
}

type ingestionInput struct {
	IngestionID string `json:"ingestion_id"`
}

func addIngestionTools(server *mcp.Server, service *agent.Service, ops Operations) {
	no, yes := false, true
	mcp.AddTool(server, &mcp.Tool{Name: "save_ingestion", Description: "Save the current successfully validated draft as one ingestion. Call after validate_ingestion; no finish step. Honor the user's scope. Saving activates its configured UTC cron schedule; blank cron stays manual. Retries return the same saved ingestion. Does not immediately queue a full load.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, IdempotentHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			SessionID string `json:"session_id"`
		}) (*mcp.CallToolResult, any, error) {
			v, err := service.Load(input.SessionID)
			if err != nil {
				return nil, nil, err
			}
			if !v.External {
				return nil, nil, errors.New("MCP conversation not found")
			}
			id, err := ops.Save(ctx, input.SessionID)
			if err != nil {
				return nil, nil, err
			}
			return nil, map[string]any{"ingestion_id": id, "detail_path": "/ingestions/" + id, "saved": true}, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "list_ingestions", Description: "Find existing Pompos ingestions for an ingestion request. Lists source, destination, schedule and execution status. Use this instead of searching local files; reuse a matching ingestion when appropriate.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct{}) (*mcp.CallToolResult, any, error) {
			items, err := ops.List(ctx)
			return nil, map[string]any{"ingestions": items}, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "get_ingestion", Description: "Read a saved ingestion's settings, last run status/error and next scheduled run. Queueing is not completion; check this after a run. Avoid continuous polling.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input ingestionInput) (*mcp.CallToolResult, any, error) {
			item, err := ops.Get(ctx, input.IngestionID)
			return nil, map[string]any{"ingestion": item}, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "run_ingestion", Description: "Queue a full production load for a saved ingestion. Use only when the user authorizes a full run. This makes source requests and writes the configured destination using replace/append/merge; replace overwrites the table and append can duplicate rows. Returns queued, not completed. Retrying can enqueue another run.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, OpenWorldHint: &yes}},
		func(ctx context.Context, req *mcp.CallToolRequest, input ingestionInput) (*mcp.CallToolResult, any, error) {
			if err := ops.Run(ctx, input.IngestionID); err != nil {
				return nil, nil, err
			}
			return nil, map[string]any{"ingestion_id": input.IngestionID, "queued": true}, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "set_schedule", Description: "Change a saved ingestion's five-field UTC cron schedule, or pass an empty cron to disable automatic runs. Use the user's authorized cadence; setting a cron enables future production loads. Keeps its source, code, destination and loading strategy.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, IdempotentHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			IngestionID string `json:"ingestion_id"`
			Cron        string `json:"cron"`
		}) (*mcp.CallToolResult, any, error) {
			if err := ops.Schedule(ctx, input.IngestionID, input.Cron); err != nil {
				return nil, nil, err
			}
			item, err := ops.Get(ctx, input.IngestionID)
			return nil, map[string]any{"ingestion": item}, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "preview_ingestion", Description: "Read up to 10 destination rows and its total row count using a fixed read-only query. Does not run the extractor or accept SQL. Samples may contain source data.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input ingestionInput) (*mcp.CallToolResult, any, error) {
			preview, err := ops.Preview(ctx, input.IngestionID)
			return nil, map[string]any{"preview": preview}, err
		})
}
