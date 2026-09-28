package agent

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pompos/internal/store"
)

func researchResponse(r *http.Request, contentType, body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}
}
func TestReadWebpageExtractsDocsAndPaginates(t *testing.T) {
	client := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("x-api-key") != "" || r.Header.Get("Cookie") != "" {
			t.Fatal("website received credentials")
		}
		return researchResponse(r, "text/html", `<html><head><title>API reference</title></head><body><nav>menu noise</nav><main><h1>Read ads</h1><script>steal credentials</script><p>Use pagination &amp; limits.</p><pre>def fetch():
    return 42</pre><a href="/docs/paging">Pagination</a><a href="javascript:alert(1)">bad</a><p hidden>hidden noise</p><p>`+strings.Repeat("Data λ ", 3000)+`</p></main></body></html>`), nil
	})}
	s := &Service{ResearchClient: client}
	output, err := s.readWebpage(context.Background(), `{"url":"https://docs.example/reference"}`)
	if err != nil {
		t.Fatal(err)
	}
	var doc webDocument
	if err = json.Unmarshal([]byte(output), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Title != "API reference" || doc.URL != "https://docs.example/reference" || !doc.Untrusted || doc.RetrievedAt == "" {
		t.Fatalf("missing provenance: %#v", doc)
	}
	if !strings.Contains(doc.Text, "    return 42") || !strings.Contains(doc.Text, "pagination & limits") {
		t.Fatal("lost readable content or code indentation")
	}
	for _, noise := range []string{"menu noise", "steal credentials", "hidden noise"} {
		if strings.Contains(doc.Text, noise) {
			t.Fatal("included hidden content")
		}
	}
	if len(doc.Links) != 1 || doc.Links[0].URL != "https://docs.example/docs/paging" {
		t.Fatalf("wrong links: %#v", doc.Links)
	}
	if !doc.Truncated || doc.NextOffset == nil || len([]rune(doc.Text)) != documentPageSize {
		t.Fatal("missing continuation")
	}
	args, _ := json.Marshal(map[string]any{"url": doc.URL, "offset": *doc.NextOffset})
	output, err = s.readWebpage(context.Background(), string(args))
	if err != nil {
		t.Fatal(err)
	}
	var continuation webDocument
	json.Unmarshal([]byte(output), &continuation)
	if continuation.Offset != *doc.NextOffset || continuation.Text == doc.Text || continuation.Truncated {
		t.Fatal("incorrect continuation")
	}
}
func TestReadWebpageRejectsUnsupportedAndOversizedResponses(t *testing.T) {
	for _, tc := range []struct {
		name, kind, body string
		status           int
	}{
		{"blocked", "text/html", "login", 403},
		{"pdf", "application/pdf", "%PDF", 200},
		{"empty shell", "text/html", "<script>renderDocs()</script>", 200},
		{"oversized", "text/plain", strings.Repeat("a", researchBodyLimit+1), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{ResearchClient: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				resp := researchResponse(r, tc.kind, tc.body)
				resp.StatusCode = tc.status
				return resp, nil
			})}}
			if _, err := s.readWebpage(context.Background(), `{"url":"https://docs.example/"}`); err == nil {
				t.Fatal("unreadable page accepted")
			}
		})
	}
}
func TestResearchOnlyAllowsPublicURLsAndRedirects(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "http://localhost/a", "http://127.0.0.1/a", "http://10.0.0.1/", "http://169.254.169.254/", "http://[::1]/", "http://[fd00::1]/", "http://100.100.100.200/", "https://user:password@docs.example/", "https://docs.example:8080/"} {
		if _, err := publicWebURL(raw); err == nil {
			t.Errorf("allowed %s", raw)
		}
	}
	for _, raw := range []string{"https://docs.example/", "http://8.8.8.8/", "https://[2606:4700:4700::1111]/"} {
		if _, err := publicWebURL(raw); err != nil {
			t.Errorf("rejected %s: %v", raw, err)
		}
	}
	client := newResearchClient()
	u, _ := url.Parse("http://127.0.0.1/secrets")
	if err := client.CheckRedirect(&http.Request{URL: u}, []*http.Request{{}}); err == nil {
		t.Fatal("redirect to localhost allowed")
	}
	_, err := client.Transport.(*http.Transport).DialContext(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", "80"))
	if err == nil || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("dial did not reject private IP: %v", err)
	}
}
func TestSearchUsesDedicatedCredentialAndReturnsSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(dir, "db.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Secrets().Put(ctx, "search", []byte("private-search-token"))
	db.Secrets().Put(ctx, "model", []byte("private-model-token"))
	s := &Service{Dir: dir, Secrets: db.Secrets(), Destinations: db}
	if err = s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test", APIKeyRef: "model", ExaAPIKeyRef: "search"}); err != nil {
		t.Fatal(err)
	}
	requests := 0
	s.ResearchClient = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		var payload struct {
			Query      string `json:"query"`
			Type       string `json:"type"`
			NumResults int    `json:"numResults"`
			Contents   struct {
				Highlights bool `json:"highlights"`
				Text       bool `json:"text"`
			} `json:"contents"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.Query != "official ads docs" || payload.Type != "auto" || payload.NumResults != 5 || !payload.Contents.Highlights || payload.Contents.Text {
			t.Fatalf("wrong Exa payload: %#v", payload)
		}
		if r.Method != "POST" || r.URL.String() != "https://api.exa.ai/search" || r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("wrong search request: %v", r.URL)
		}
		if r.Header.Get("x-api-key") != "private-search-token" || r.Header.Get("Authorization") != "" {
			t.Fatal("wrong credentials")
		}
		return researchResponse(r, "application/json", `{"results":[{"title":"Official API","url":"https://docs.example/ads","highlights":["API pagination", "Use a cursor"]},{"title":"private","url":"http://127.0.0.1/"}]}`), nil
	})}
	output, err := s.webSearch(ctx, `{"query":"official ads docs"}`)
	if err != nil || !strings.Contains(output, "https://docs.example/ads") || !strings.Contains(output, "Use a cursor") || strings.Contains(output, "127.0.0.1") || strings.Contains(output, "private-search-token") {
		t.Fatalf("search: %s %v", output, err)
	}
	call := Call{}
	call.Function.Name = "context"
	output, err = s.execute(ctx, &Session{}, call)
	if err != nil || strings.Contains(output, `"search"`) || strings.Contains(output, `"model"`) {
		t.Fatalf("provider key listed as source: %s %v", output, err)
	}
	call.Function.Name = "ask_user"
	call.Function.Arguments = `{"kind":"secret","prompt":"Add token","secret_name":"search"}`
	if _, err = s.execute(ctx, &Session{}, call); err == nil {
		t.Fatal("asked for provider key as source")
	}
	if requests != 1 {
		t.Fatal("unexpected search requests")
	}
}
func TestResearchToolLoopFeedsDocsToModelAndPersistsSources(t *testing.T) {
	s := &Service{Dir: t.TempDir()}
	if err := s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	s.ResearchClient = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		return researchResponse(r, "text/markdown", "# Current API\nUse cursor pagination."), nil
	})}
	step := 0
	s.Client = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		payload, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(payload), "web_search") || !strings.Contains(string(payload), "read_webpage") {
			t.Fatal("research tools missing")
		}
		response := `{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"docs","type":"function","function":{"name":"read_webpage","arguments":"{\"url\":\"https://docs.example/api\"}"}}]}}]}`
		if step > 0 {
			if !strings.Contains(string(payload), "Use cursor pagination") {
				t.Fatal("document was not fed back to model")
			}
			response = `{"choices":[{"message":{"role":"assistant","content":"The documentation at https://docs.example/api describes cursor pagination."}}]}`
		}
		step++
		return researchResponse(r, "application/json", response), nil
	})}
	v, err := s.Turn(context.Background(), "research", "Read these API docs")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := s.Load(v.ID)
	if err != nil || len(loaded.Messages) != len(v.Messages) || !strings.Contains(loaded.Messages[len(loaded.Messages)-2].Content, `"retrieved_at"`) {
		t.Fatal("research provenance not saved")
	}
}
func TestSearchMissingKeyAndRateLimitAreActionable(t *testing.T) {
	s := &Service{Dir: t.TempDir()}
	if _, err := s.webSearch(context.Background(), `{"query":"docs"}`); err == nil || !strings.Contains(err.Error(), "Agent settings") {
		t.Fatalf("unconfigured search: %v", err)
	}
	// HTTP errors never relay provider bodies, which could contain credentials.
	dir := t.TempDir()
	db, err := store.Open(context.Background(), filepath.Join(dir, "db.sqlite"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Secrets().Put(context.Background(), "search", []byte("private"))
	s.Secrets = db.Secrets()
	s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test", ExaAPIKeyRef: "search"})
	s.ResearchClient = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		resp := researchResponse(r, "application/json", "private")
		resp.StatusCode = 429
		return resp, nil
	})}
	if _, err = s.webSearch(context.Background(), `{"query":"docs"}`); err == nil || !strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "private") {
		t.Fatalf("bad rate limit error: %v", err)
	}
}

func TestReadLiveDocumentation(t *testing.T) {
	if os.Getenv("POMPOS_TEST_WEB") != "1" {
		t.Skip("set POMPOS_TEST_WEB=1 for a public documentation request")
	}
	s := &Service{}
	output, err := s.readWebpage(context.Background(), `{"url":"https://go.dev/doc/"}`)
	if err != nil {
		t.Fatal(err)
	}
	var doc webDocument
	if err = json.Unmarshal([]byte(output), &doc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Title, "Documentation") || len(doc.Text) < 100 || len(doc.Links) == 0 {
		t.Fatalf("missing document content: title=%q, text=%d chars, links=%d", doc.Title, len(doc.Text), len(doc.Links))
	}
}
