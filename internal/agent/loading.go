package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"pompos/internal/scheduler"
	"pompos/internal/spec"
)

// Loading is the user-confirmed configuration for one ingestion.
type Loading struct {
	Cron       string   `json:"cron"`
	Strategy   string   `json:"strategy"`
	PrimaryKey []string `json:"primary_key"`
}

func (o Loading) Validate() error {
	if err := scheduler.ValidateCron(o.Cron); err != nil {
		return err
	}
	if o.Strategy != "replace" && o.Strategy != "append" && o.Strategy != "merge" {
		return errors.New("choose replace, append, or merge")
	}
	return (spec.Materialization{Strategy: o.Strategy, PrimaryKey: o.PrimaryKey}).Validate()
}
func (o Loading) description() string {
	schedule := "manual runs only"
	if o.Cron != "" {
		schedule = fmt.Sprintf("cron %q in UTC", o.Cron)
	}
	keys := ""
	if len(o.PrimaryKey) > 0 {
		keys = "; row keys: " + strings.Join(o.PrimaryKey, ", ")
	}
	return fmt.Sprintf("Use %s; loading strategy: %s%s. These settings are confirmed. Continue testing and preparing this ingestion.", schedule, o.Strategy, keys)
}
func proposeLoading(v *Session, arguments string) (string, error) {
	var request struct {
		Loading
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return "", err
	}
	request.Cron = strings.TrimSpace(request.Cron)
	if err := request.Loading.Validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 1500 {
		return "", errors.New("explain the proposed cadence and loading strategy briefly")
	}
	v.Ready = false
	v.Loading = nil // A revised proposal needs a fresh confirmation.
	v.Pending = &Handoff{ID: fmt.Sprint(len(v.Messages)), Kind: "loading", Prompt: request.Reason, Loading: &request.Loading, Actions: []Action{
		{ID: "accept_loading", Label: "Use these settings", Message: request.Loading.description()},
		{ID: "explain", Label: "Tell me more", Message: "Explain the proposed schedule, timezone, loading strategy and row keys, including replacement or duplicate risks."},
	}}
	return "Waiting for the user to confirm or adjust the proposed schedule and loading strategy. The UI shows cron in UTC, strategy and primary-key fields.", nil
}
func applyLoading(v *Session) {
	if v.Loading != nil && v.Draft != nil {
		v.Draft.Schedule = v.Loading.Cron
		v.Draft.Strategy = v.Loading.Strategy
		v.Draft.PrimaryKey = append([]string(nil), v.Loading.PrimaryKey...)
	}
}
