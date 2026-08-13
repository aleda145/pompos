package validation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"pompos/internal/ingestion"
)

func TestHTTPSourceValidatorFallsBackToRangeGET(t *testing.T) {
	var gotRange string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodHead {
			return response(http.StatusMethodNotAllowed, ""), nil
		}
		gotRange = r.Header.Get("Range")
		return response(http.StatusPartialContent, "x"), nil
	})}

	validator := HTTPSourceValidator{Client: client}
	if err := validator.Validate(context.Background(), ingestion.Source{Type: "csv", URL: "https://example.com/data.csv"}); err != nil {
		t.Fatal(err)
	}
	if gotRange != "bytes=0-0" {
		t.Fatalf("Range = %q", gotRange)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestParseGitHubRepository(t *testing.T) {
	for _, test := range []struct{ input, owner, repository string }{
		{"openai/codex", "openai", "codex"},
		{"https://github.com/openai/codex", "openai", "codex"},
		{"https://github.com/openai/codex.git", "openai", "codex"},
	} {
		owner, repository, err := ParseGitHubRepository(test.input)
		if err != nil {
			t.Fatalf("ParseGitHubRepository(%q): %v", test.input, err)
		}
		if owner != test.owner || repository != test.repository {
			t.Fatalf("ParseGitHubRepository(%q) = %q/%q", test.input, owner, repository)
		}
	}
}

func TestHTTPSourceValidatorRequiresGitHubToken(t *testing.T) {
	validator := HTTPSourceValidator{Client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatal("GitHub should not be called without a token")
		return nil, nil
	})}}
	err := validator.Validate(context.Background(), ingestion.Source{
		Type: "github", Owner: "openai", Repository: "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "access token is required") {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestHTTPSourceValidatorChecksStargazerAccess(t *testing.T) {
	var paths []string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, "/stargazers") {
			return response(http.StatusForbidden, `{"message":"Resource not accessible by personal access token"}`), nil
		}
		return response(http.StatusOK, `{}`), nil
	})}
	err := (HTTPSourceValidator{Client: client}).Validate(context.Background(), ingestion.Source{
		Type: "github", Owner: "openai", Repository: "codex", AccessToken: "token", Table: "stargazers",
	})
	if err == nil || !strings.Contains(err.Error(), "admins and collaborators") {
		t.Fatalf("Validate() error = %v", err)
	}
	if len(paths) != 2 || paths[1] != "/repos/openai/codex/stargazers" {
		t.Fatalf("requested paths = %v", paths)
	}
}
