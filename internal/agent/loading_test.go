package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runnerpython "pompos/internal/runner/python"
	"pompos/internal/spec"
	"pompos/internal/store"
)

func TestLoadingValidation(t *testing.T) {
	for _, options := range []Loading{
		{Cron: "0 6 * * *", Strategy: "replace"},
		{Cron: "", Strategy: "append"},
		{Cron: "0 * * * *", Strategy: "merge", PrimaryKey: []string{"id"}},
	} {
		if err := options.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, options := range []Loading{
		{Cron: "every day", Strategy: "replace"},
		{Cron: "0 0 6 * * *", Strategy: "replace"},
		{Cron: "90 * * * *", Strategy: "replace"},
		{Cron: "CRON_TZ=Europe/Stockholm 0 6 * * *", Strategy: "replace"},
		{Strategy: "merge"},
		{Strategy: "scd2", PrimaryKey: []string{"id"}},
		{Strategy: "merge", PrimaryKey: []string{"id", "id"}},
	} {
		if err := options.Validate(); err == nil {
			t.Errorf("invalid options accepted: %#v", options)
		}
	}
}

func TestLoadingEditsAreConfirmedPersistedAndAuthoritative(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), filepath.Join(dir, "out.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Service{Dir: filepath.Join(dir, "agent"), Secrets: db.Secrets(), Destinations: db}
	if err = s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	v := Session{ID: "loading", Messages: []Message{{Role: "system", Content: prompt}}, Draft: &Draft{Name: "Stars", Source: "fixture", Table: "stars", Destination: "local-duckdb", Strategy: "replace", Code: "def fetch(secret, limit):\n    yield {'id': 1}\n"}}
	data := []byte(runnerpython.Wrap(v.Draft.Code))
	if err = WriteFile(s.scriptPath(v.ID), data); err != nil {
		t.Fatal(err)
	}
	v.TestedDigest = spec.Digest(data)
	if _, err = proposeLoading(&v, `{"cron":"0 6 * * *","strategy":"replace","primary_key":[],"reason":"Daily refresh of the current list."}`); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Settings confirmed."}}]}`)), Header: make(http.Header)}, nil
	})}
	input := Input{ActionID: "accept_loading", HandoffID: v.Pending.ID, Loading: &Loading{Cron: "not cron", Strategy: "replace"}}
	if _, err = s.TurnWithEvents(ctx, v.ID, input, nil); err == nil {
		t.Fatal("bad cron accepted")
	}
	persisted, _ := s.Load(v.ID)
	if persisted.Loading != nil || persisted.Pending == nil {
		t.Fatal("invalid settings changed saved state")
	}
	input.Loading = &Loading{Cron: "", Strategy: "merge", PrimaryKey: []string{"id"}}
	v, err = s.TurnWithEvents(ctx, v.ID, input, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Loading == nil || v.Draft.Schedule != "" || v.Draft.Strategy != "merge" || v.TestedDigest != spec.Digest(data) {
		t.Fatal("settings were not applied or unnecessarily discarded probe")
	}
	loaded, err := s.Load(v.ID)
	if err != nil || loaded.Loading == nil || loaded.Loading.Strategy != "merge" {
		t.Fatal("confirmed settings not persisted")
	}
	// Rewriting the same extractor must not quietly override the user's settings.
	draft := *v.Draft
	draft.Strategy = "append"
	draft.PrimaryKey = nil
	args, _ := json.Marshal(draft)
	call := Call{}
	call.Function.Name = "write_script"
	call.Function.Arguments = string(args)
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if v.Draft.Strategy != "merge" || len(v.Draft.PrimaryKey) != 1 {
		t.Fatal("model overrode confirmed settings")
	}
	data, _ = os.ReadFile(s.scriptPath(v.ID))
	v.TestedDigest = spec.Digest(data)
	// This unit test focuses on settings persistence; the approval flow is tested separately.
	_, fingerprint, err := s.validationPlan(ctx, &v)
	if err != nil {
		t.Fatal(err)
	}
	v.Validation = &Validation{Fingerprint: fingerprint}
	call.Function.Name = "finish"
	call.Function.Arguments = "{}"
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if err = s.save(v); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Publish(ctx, v.ID, filepath.Join(dir, "ingestions"), func(_ string, doc spec.Ingestion) error {
		if doc.Schedule != nil || doc.Materialization.Strategy != "merge" {
			t.Fatal("manual or merge choice lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A different source needs its own confirmation.
	draft.Source = "different-source"
	args, _ = json.Marshal(draft)
	call.Function.Name = "write_script"
	call.Function.Arguments = string(args)
	if _, err = s.execute(ctx, &v, call); err != nil {
		t.Fatal(err)
	}
	if v.Loading != nil {
		t.Fatal("reused confirmation for a different source")
	}
}

func TestRevisedLoadingProposalNeedsFreshConfirmation(t *testing.T) {
	v := Session{Ready: true, Loading: &Loading{Cron: "0 6 * * *", Strategy: "replace"}}
	if _, err := proposeLoading(&v, `{"cron":"0 * * * *","strategy":"append","primary_key":[],"reason":"Hourly history needs appending timestamped observations."}`); err != nil {
		t.Fatal(err)
	}
	if v.Ready || v.Loading != nil || v.Pending == nil || v.Pending.Loading.Cron != "0 * * * *" {
		t.Fatal("revised settings reused prior confirmation")
	}
}
