package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"pompos/internal/compiler"
	"pompos/internal/destination"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
)

type ValidationProposal struct {
	Limit          int    `json:"limit"`
	MinCount       int    `json:"min_count,omitempty"`
	MaxBytes       int64  `json:"max_bytes,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	Fingerprint    string `json:"fingerprint"`
}

func (s *Service) SetManualValidation(enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, err := s.settings()
	if err != nil {
		return err
	}
	cfg.ManualValidation = enabled
	return writeJSON(filepath.Join(s.Dir, "settings.json"), cfg)
}

func validationRequest(v *Session, arguments string) (ValidationProposal, error) {
	var request ValidationProposal
	files := v.Draft != nil && v.Draft.Data == "files"
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return request, err
	}
	if request.Limit < 0 {
		return request, errors.New("validation limit must be nonnegative; 0 means all items")
	}
	if request.MinCount < 0 || (request.Limit > 0 && request.MinCount > request.Limit) {
		return request, errors.New("min_count must be nonnegative and no greater than a positive validation limit")
	}
	if request.MaxBytes < 0 || request.TimeoutSeconds < 0 {
		return request, errors.New("max_bytes and timeout_seconds must be nonnegative; 0 means unlimited")
	}
	if !files && request.MaxBytes != 0 {
		return request, errors.New("max_bytes applies only to file validation")
	}
	return request, nil
}

type Validation struct {
	ArtifactDigest string                        `json:"artifact_digest,omitempty"`
	Fingerprint    string                        `json:"fingerprint"`
	Result         runnerpython.ValidationResult `json:"result"`
}

func validationProperties() map[string]any {
	return map[string]any{
		"limit":           map[string]any{"type": "integer", "minimum": 0, "description": "Choose how many rows or files to validate. Omitted or 0 fetches all items. No upper ceiling. Choose a sample or full extraction based on the source and suspected bugs."},
		"min_count":       map[string]any{"type": "integer", "minimum": 0, "description": "Optional minimum count required to pass; cannot exceed a positive limit. Set from source evidence or the user's expected count. For a suspected 10-item cap, use limit=11 and min_count=11."},
		"max_bytes":       map[string]any{"type": "integer", "minimum": 0, "description": "Optional file download budget in bytes across both loads. Omitted or 0 means unlimited. No upper ceiling."},
		"timeout_seconds": map[string]any{"type": "integer", "minimum": 0, "description": "Optional validation execution timeout for rows or files. Omitted or 0 means no timeout. No upper ceiling."},
	}
}

// Bind approval and success to the current script, settings, and destination.
func (s *Service) validationPlan(ctx context.Context, v *Session) (compiler.ExecutionPlan, string, error) {
	var plan compiler.ExecutionPlan
	if v.Draft == nil || v.Loading == nil {
		if v.External {
			return plan, "", errors.New("call write_script, test_script and configure_loading before validate_ingestion")
		}
		return plan, "", errors.New("write and probe the script, then confirm loading settings before validation")
	}
	if err := v.Loading.Validate(); err != nil {
		return plan, "", err
	}
	if !v.Probed {
		return plan, "", errors.New("the current script must pass test_script first")
	}

	dest, err := s.Destinations.GetDestination(ctx, v.Draft.Destination)
	if err != nil {
		return plan, "", err
	}
	if (dest.Type == "objects") != (v.Draft.Data == "files") {
		return plan, "", errors.New("destination type changed; rewrite and probe the script")
	}
	if err := v.Loading.ValidateDraft(v.Draft); err != nil {
		return plan, "", err
	}
	applyLoading(v)
	plan = compiler.ExecutionPlan{Engine: "python", Script: s.scriptPath(v.ID),
		// Draft JSON omits empty dependencies. Canonicalize [] and nil so saving
		// and reloading a session cannot change its validation fingerprint.
		Python: v.Draft.Python, Dependencies: append([]string(nil), v.Draft.Dependencies...),
		SecretRefs: v.Draft.SecretRefs, DestinationType: dest.Type, DestinationPath: dest.Path,
		DestinationSchema: destination.SchemaName(v.Draft.Schema), DestinationObject: v.Draft.Table, Strategy: v.Loading.Strategy, PrimaryKey: v.Loading.PrimaryKey}
	data, err := json.Marshal(struct {
		Draft *Draft
		Plan  compiler.ExecutionPlan
	}{v.Draft, plan})
	return plan, spec.Digest(data), err
}

// A stale approval never runs a changed draft. Replace its handoff so the user
// can review and confirm again without sending a message to the model.
func (s *Service) refreshValidation(ctx context.Context, v *Session) error {
	arguments, err := json.Marshal(v.Pending.Validation)
	if err != nil {
		return err
	}
	v.Messages = append(v.Messages, Message{Role: "assistant", Content: "Draft changed. Review the updated validation settings, then choose Run validation."})
	if _, err := s.proposeValidation(ctx, v, string(arguments)); err != nil {
		return err
	}
	return s.save(*v)
}

func (s *Service) proposeValidation(ctx context.Context, v *Session, arguments string) (string, error) {
	cfg, err := s.settings()
	if err != nil {
		return "", err
	}
	request, err := validationRequest(v, arguments)
	if err != nil {
		return "", err
	}
	plan, fingerprint, err := s.validationPlan(ctx, v)
	if err != nil {
		return "", err
	}
	v.Ready = false
	v.Validation = nil
	request.Fingerprint = fingerprint
	if !cfg.ManualValidation {
		v.Pending = nil
		return s.validateSample(ctx, v, plan, &request)
	}
	scope := "all items"
	if request.Limit > 0 {
		scope = fmt.Sprintf("up to %d items", request.Limit)
	}
	v.Pending = &Handoff{ID: fmt.Sprint(len(v.Messages)), Kind: "validation",
		Prompt:     "Validate a sample before saving this ingestion?",
		Validation: &request,
		Actions: []Action{
			{ID: "accept_validation", Label: "Run validation", Message: fmt.Sprintf("Validate %s in temporary storage using the selected validation settings.", scope)},
			{ID: "explain", Label: "Tell me more", Message: "Explain the validation sample and what will be checked. Keep validation awaiting my confirmation."},
			{ID: "defer_validation", Label: "Not now", Message: "Do not run validation yet. Keep the draft; I will decide when to validate."},
		}}
	return "Waiting for explicit user confirmation. The card shows the validation row limit and temporary loading checks. Validation has not run.", nil
}

func (s *Service) requireValidation(ctx context.Context, v *Session) error {
	_, fingerprint, err := s.validationPlan(ctx, v)
	if err != nil {
		return err
	}
	if v.Validation == nil || v.Validation.Fingerprint != fingerprint {
		if v.External {
			return errors.New("call validate_ingestion successfully for the current script and loading settings before saving")
		}
		return errors.New("call propose_validation successfully for the current script and loading settings before finishing or saving; if approval is enabled, wait for the user's Run validation action")
	}
	return nil
}

// Approval runs exactly once, before asking the model to interpret its results.
func (s *Service) runValidation(ctx context.Context, v *Session, proposal *ValidationProposal, send func(Event)) error {
	plan, fingerprint, err := s.validationPlan(ctx, v)
	if err != nil {
		return err
	}
	if fingerprint != proposal.Fingerprint {
		return errors.New("draft changed; request fresh validation confirmation")
	}
	v.Validation = nil
	call := Call{ID: fmt.Sprintf("validation_%d", len(v.Messages)), Type: "function"}
	call.Function.Name = "validate_ingestion"
	args, _ := json.Marshal(map[string]any{"limit": proposal.Limit, "min_count": proposal.MinCount, "max_bytes": proposal.MaxBytes, "timeout_seconds": proposal.TimeoutSeconds, "strategy": plan.Strategy, "destination": "temporary DuckDB"})
	call.Function.Arguments = string(args)
	message := Message{Role: "assistant", Calls: []Call{call}}
	v.Messages = append(v.Messages, message)
	send(Event{Type: "message", Message: &message})
	send(Event{Type: "tool_start", Call: &call})
	output, runErr := s.validateSample(ctx, v, plan, proposal)
	if runErr != nil {
		output = "Error: " + runErr.Error()
	}
	message = Message{Role: "tool", CallID: call.ID, Content: output}
	v.Messages = append(v.Messages, message)
	send(Event{Type: "message", Message: &message})
	return s.save(*v)
}

func (s *Service) validateSample(ctx context.Context, v *Session, plan compiler.ExecutionPlan, proposal *ValidationProposal) (string, error) {
	result, err := s.performValidation(ctx, v, plan, proposal)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(result)
	return "POMPOS_VALIDATION_RESULT=" + string(data), err
}

func (s *Service) performValidation(ctx context.Context, v *Session, plan compiler.ExecutionPlan, proposal *ValidationProposal) (runnerpython.ValidationResult, error) {
	v.Ready, v.Validation = false, nil
	plan.ValidationMaxBytes, plan.ValidationTimeoutSeconds = proposal.MaxBytes, proposal.TimeoutSeconds
	result, _, err := s.Python.Validate(ctx, plan, proposal.Limit)
	if err != nil {
		return result, err
	}
	if result.SampleCount < proposal.MinCount {
		return result, fmt.Errorf("validation fetched %d items; expected at least %d (limit %d). Investigate pagination, filters, or extraction caps before saving", result.SampleCount, proposal.MinCount, proposal.Limit)
	}
	v.Validation = &Validation{Fingerprint: proposal.Fingerprint, Result: result}
	if v.Edit != nil {
		artifacts, err := s.draftArtifacts(v.ID)
		if err != nil {
			v.Validation = nil
			return result, err
		}
		v.Validation.ArtifactDigest = artifacts.digest()
	}
	return result, nil
}
