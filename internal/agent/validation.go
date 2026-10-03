package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"pompos/internal/compiler"
	"pompos/internal/destination"
	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
)

type ValidationProposal struct {
	Limit       int    `json:"limit"`
	Fingerprint string `json:"fingerprint"`
}

type Validation struct {
	Fingerprint string                        `json:"fingerprint"`
	Result      runnerpython.ValidationResult `json:"result"`
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
	if dest.Type != "duckdb" {
		return plan, "", errors.New("validation supports DuckDB destinations")
	}
	applyLoading(v)
	plan = compiler.ExecutionPlan{Engine: "python", Script: s.scriptPath(v.ID),
		Python: v.Draft.Python, Dependencies: v.Draft.Dependencies,
		SecretRefs: v.Draft.SecretRefs, DestinationType: dest.Type, DestinationPath: dest.Path,
		DestinationSchema: destination.SchemaName(v.Draft.Schema), DestinationObject: v.Draft.Table, Strategy: v.Loading.Strategy, PrimaryKey: v.Loading.PrimaryKey}
	data, err := json.Marshal(struct {
		Draft *Draft
		Plan  compiler.ExecutionPlan
	}{v.Draft, plan})
	return plan, spec.Digest(data), err
}

func (s *Service) proposeValidation(ctx context.Context, v *Session, arguments string) (string, error) {
	request := struct {
		Limit int `json:"limit"`
	}{Limit: 100}
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return "", err
	}
	if request.Limit < 1 || request.Limit > 1000 {
		return "", errors.New("validation limit must be between 1 and 1000 rows")
	}
	_, fingerprint, err := s.validationPlan(ctx, v)
	if err != nil {
		return "", err
	}
	v.Ready = false
	v.Validation = nil
	v.Pending = &Handoff{ID: fmt.Sprint(len(v.Messages)), Kind: "validation",
		Prompt:     "Validate a sample before saving this ingestion?",
		Validation: &ValidationProposal{Limit: request.Limit, Fingerprint: fingerprint},
		Actions: []Action{
			{ID: "accept_validation", Label: "Run validation", Message: fmt.Sprintf("Validate up to %d source rows in a temporary database using the confirmed loading strategy.", request.Limit)},
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
		return errors.New("call propose_validation and wait for the user to approve a successful validation before finishing or saving")
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
	args, _ := json.Marshal(map[string]any{"limit": proposal.Limit, "strategy": plan.Strategy, "destination": "temporary DuckDB"})
	call.Function.Arguments = string(args)
	message := Message{Role: "assistant", Calls: []Call{call}}
	v.Messages = append(v.Messages, message)
	send(Event{Type: "message", Message: &message})
	send(Event{Type: "tool_start", Call: &call})
	result, output, runErr := s.Python.Validate(ctx, plan, proposal.Limit)
	if runErr != nil {
		output = "Error: " + runErr.Error()
	} else {
		v.Validation = &Validation{Fingerprint: fingerprint, Result: result}
		data, _ := json.Marshal(result)
		output = "POMPOS_VALIDATION_RESULT=" + string(data)
	}
	message = Message{Role: "tool", CallID: call.ID, Content: output}
	v.Messages = append(v.Messages, message)
	send(Event{Type: "message", Message: &message})
	return s.save(*v)
}
