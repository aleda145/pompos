package ingestr

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"

	"pompos/internal/compiler"
	"pompos/internal/ingestion"
)

func TestBuildArgs(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "./data/pompos.duckdb", Table: "customers"},
	}
	want := []string{
		"ingest",
		"--source-uri", "https://example.com/customers.csv",
		"--source-table", "data#csv",
		"--dest-uri", "duckdb://data/pompos.duckdb",
		"--dest-table", "customers",
		"--schema-naming", "direct",
		"--incremental-strategy", "replace",
	}
	got, err := BuildArgs(planFor(item), item.Source.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildArgs() = %#v, want %#v", got, want)
	}
}

func TestLogWriterRedactsGitHubToken(t *testing.T) {
	var output bytes.Buffer
	writer := &logWriter{
		logger: log.New(&output, "", 0),
		prefix: "ingestr stderr",
		secret: "github_pat_a&b",
	}
	message := "failed github://?access_token=github_pat_a%26b and github_pat_a&b\n"
	if _, err := writer.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "github_pat") || !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatalf("logged output = %q", output.String())
	}
}

func TestExecutionErrorMaterialIsRedacted(t *testing.T) {
	got := redact("token=github_pat_a%26b raw=github_pat_a&b", "github_pat_a&b")
	if strings.Contains(got, "github_pat") || strings.Count(got, "[REDACTED]") != 2 {
		t.Fatalf("redacted = %q", got)
	}
}

func TestBuildArgsAbsoluteDestination(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "/data/pompos.duckdb", Table: "customers"},
	}
	args, err := BuildArgs(planFor(item), item.Source.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if args[6] != "duckdb:///data/pompos.duckdb" {
		t.Fatalf("destination URI = %q", args[6])
	}
}

func TestBuildArgsRepoRelativeDestination(t *testing.T) {
	item := ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "data/pompos.duckdb", Table: "customers"},
	}
	args, err := BuildArgs(planFor(item), item.Source.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if args[6] != "duckdb://data/pompos.duckdb" {
		t.Fatalf("destination URI = %q", args[6])
	}
}

func TestBuildArgsGitHub(t *testing.T) {
	item := ingestion.Ingestion{
		Source: ingestion.Source{
			Type: "github", Owner: "openai", Repository: "codex", AccessToken: "github_pat_a&b", Table: "pull_requests",
		},
		Destination: ingestion.Destination{Type: "duckdb", Path: "data/pompos.duckdb", Table: "openai_codex_pull_requests"},
	}
	want := []string{
		"ingest",
		"--source-uri", "github://?access_token=github_pat_a%26b&owner=openai&repo=codex",
		"--source-table", "pull_requests",
		"--dest-uri", "duckdb://data/pompos.duckdb",
		"--dest-table", "openai_codex_pull_requests",
		"--schema-naming", "direct",
		"--incremental-strategy", "replace",
	}
	got, err := BuildArgs(planFor(item), item.Source.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildArgs() = %#v, want %#v", got, want)
	}
}

func TestBuildArgsGitHubRequiresAccessToken(t *testing.T) {
	item := ingestion.Ingestion{
		Source: ingestion.Source{
			Type: "github", Owner: "openai", Repository: "codex", Table: "stargazers",
		},
		Destination: ingestion.Destination{Type: "duckdb", Path: "data/pompos.duckdb", Table: "openai_codex_stargazers"},
	}
	_, err := BuildArgs(planFor(item), item.Source.AccessToken)
	if err == nil || !strings.Contains(err.Error(), "access token is required") {
		t.Fatalf("BuildArgs() error = %v", err)
	}
}

func TestBuildArgsIncludesLoadingKeys(t *testing.T) {
	plan := planFor(ingestion.Ingestion{
		Source:      ingestion.Source{Type: "csv", URL: "https://example.com/customers.csv"},
		Destination: ingestion.Destination{Type: "duckdb", Path: "data/pompos.duckdb", Table: "customers"},
	})
	plan.Strategy = "merge"
	plan.PrimaryKey = []string{"account_id", "id"}
	plan.IncrementalKey = "updated_at"
	args, err := BuildArgs(plan, "")
	if err != nil {
		t.Fatal(err)
	}
	wantTail := []string{"--primary-key", "account_id", "--primary-key", "id", "--incremental-key", "updated_at"}
	if !reflect.DeepEqual(args[len(args)-len(wantTail):], wantTail) {
		t.Fatalf("args = %#v", args)
	}
}

func planFor(item ingestion.Ingestion) compiler.ExecutionPlan {
	plan := compiler.ExecutionPlan{Engine: "ingestr", EngineVersion: "1.1.8", SourceURI: item.Source.URL, SourceTable: "data#csv",
		DestinationURI: destinationURI(item.Destination.Path), DestinationObject: item.Destination.Table, SchemaNaming: "direct", Strategy: "replace"}
	if item.Source.Type == "github" {
		plan.SourceURI = "github://?owner=" + item.Source.Owner + "&repo=" + item.Source.Repository
		plan.SourceTable, plan.CredentialRef = item.Source.Table, "github"
	}
	return plan
}

func destinationURI(path string) string {
	if strings.HasPrefix(path, "/") {
		return "duckdb://" + path
	}
	return "duckdb://" + strings.TrimPrefix(path, "./")
}
