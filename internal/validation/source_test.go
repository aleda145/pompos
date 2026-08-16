package validation

import (
	"context"
	"io"
	"net"
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

	validator := HTTPSourceValidator{Client: client, Resolver: publicResolver()}
	if err := validator.Validate(context.Background(), ingestion.Source{Type: "csv", URL: "https://example.com/data.csv"}); err != nil {
		t.Fatal(err)
	}
	if gotRange != "bytes=0-0" {
		t.Fatalf("Range = %q", gotRange)
	}
}

func TestHTTPSourceValidatorDiscoversCSVColumnsFromOneRow(t *testing.T) {
	var gotRange string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotRange = r.Header.Get("Range")
		return response(http.StatusPartialContent, "\ufeffid,name,updated_at\n1,Ada,2026-08-16\n2,Grace,2026-08-17\n"), nil
	})}
	columns, err := (HTTPSourceValidator{Client: client, Resolver: publicResolver()}).Columns(context.Background(), ingestion.Source{
		Type: "csv", URL: "https://example.com/data.csv",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(columns, ",") != "id,name,updated_at" {
		t.Fatalf("columns = %v", columns)
	}
	if gotRange != "bytes=0-262143" {
		t.Fatalf("Range = %q", gotRange)
	}
}

func TestHTTPSourceValidatorDiscoversGitHubColumns(t *testing.T) {
	var authorization, path string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		authorization, path = r.Header.Get("Authorization"), r.URL.RequestURI()
		return response(http.StatusOK, `[{"updated_at":"2026-08-16","id":1,"title":"Magic"}]`), nil
	})}
	columns, err := (HTTPSourceValidator{Client: client}).Columns(context.Background(), ingestion.Source{
		Type: "github", Owner: "openai", Repository: "codex", Table: "issues", AccessToken: "secret-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(columns, ",") != "id,title,updated_at" {
		t.Fatalf("columns = %v", columns)
	}
	if authorization != "Bearer secret-token" || path != "/repos/openai/codex/issues?state=all&per_page=1" {
		t.Fatalf("authorization = %q, path = %q", authorization, path)
	}
}

func TestHTTPSourceValidatorRejectsPrivateNetworks(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatal("private URL must be rejected before HTTP request")
		return nil, nil
	})}
	resolver := resolverFunc(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.0.8")}}, nil
	})
	validator := HTTPSourceValidator{Client: client, Resolver: resolver}
	for _, rawURL := range []string{"http://127.0.0.1/data.csv", "http://169.254.169.254/latest/meta-data", "https://internal.example/data.csv"} {
		err := validator.Validate(context.Background(), ingestion.Source{Type: "csv", URL: rawURL})
		if err == nil || !strings.Contains(err.Error(), "policy.http-file-public-network") {
			t.Fatalf("Validate(%q) error = %v", rawURL, err)
		}
	}
}

type resolverFunc func(context.Context, string) ([]net.IPAddr, error)

func (f resolverFunc) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f(ctx, host)
}

func publicResolver() resolverFunc {
	return func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
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
