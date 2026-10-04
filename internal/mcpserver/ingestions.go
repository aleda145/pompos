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
	Edit     func(context.Context, string, int64, bool) (agent.Session, error)
	Apply    func(context.Context, string, string) (string, error)
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
	mcp.AddTool(server, &mcp.Tool{Name: "edit_ingestion", Description: "Open a saved ingestion as an isolated edit draft using its current YAML, Python, dependencies and the selected run's redacted log (latest run by default). Destination stays fixed. Returns session_id and draft; probe, validate, review_ingestion_changes, then apply_ingestion_changes. Does not change the published ingestion.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &no, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			IngestionID string `json:"ingestion_id"`
			RunID       int64  `json:"run_id,omitempty"`
		}) (*mcp.CallToolResult, any, error) {
			if input.RunID < 0 {
				return nil, nil, errors.New("run_id must be positive or omitted")
			}
			v, err := ops.Edit(ctx, input.IngestionID, input.RunID, true)
			return nil, fullChatResult(v, false), err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "review_ingestion_changes", Description: "Review a successfully validated edit. Returns saved/proposed Python and YAML, validation and a fingerprint required to apply this exact revision. Present meaningful changes to the user before applying.", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: &no}},
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
			review, err := service.ReviewEdit(ctx, input.SessionID)
			return nil, review, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "apply_ingestion_changes", Description: "Apply reviewed, validated changes to the same ingestion, preserving run history. Requires the review fingerprint and user authorization for the changes. Rejects stale reviews, changed published files, or queued/running work. Updates the schedule but does not queue a run.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, IdempotentHint: true, OpenWorldHint: &no}},
		func(ctx context.Context, req *mcp.CallToolRequest, input struct {
			SessionID   string `json:"session_id"`
			Fingerprint string `json:"fingerprint"`
		}) (*mcp.CallToolResult, any, error) {
			v, err := service.Load(input.SessionID)
			if err != nil {
				return nil, nil, err
			}
			if !v.External {
				return nil, nil, errors.New("MCP conversation not found")
			}
			id, err := ops.Apply(ctx, input.SessionID, input.Fingerprint)
			return nil, map[string]any{"ingestion_id": id, "detail_path": "/ingestions/" + id, "applied": err == nil}, err
		})
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
	mcp.AddTool(server, &mcp.Tool{Name: "run_ingestion", Description: "Queue a full production load for a saved ingestion. Use only when the user authorizes a full run. Row ingestions use replace/append/merge; replace overwrites the table and append can duplicate rows. Object ingestions download files and update their DuckDB catalog using update/skip, retaining missing source files. Returns queued, not completed. Retrying can enqueue another run.", Annotations: &mcp.ToolAnnotations{DestructiveHint: &yes, OpenWorldHint: &yes}},
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
