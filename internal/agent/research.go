package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/net/html"
)

const researchBodyLimit = 2 << 20
const documentPageSize = 12000

type webLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}
type searchHit struct {
	webLink
	Description string `json:"description"`
}
type webDocument struct {
	URL         string    `json:"url"`
	Title       string    `json:"title"`
	Text        string    `json:"text"`
	Links       []webLink `json:"links"`
	RetrievedAt string    `json:"retrieved_at"`
	Offset      int       `json:"offset"`
	NextOffset  *int      `json:"next_offset,omitempty"`
	Truncated   bool      `json:"truncated"`
	Untrusted   bool      `json:"untrusted_content"`
}

func publicWebURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, errors.New("provide a public HTTP(S) URL without embedded credentials")
	}
	if len(raw) > 4000 {
		return nil, errors.New("URL is too long")
	}
	if port := u.Port(); port != "" && port != "80" && port != "443" {
		return nil, errors.New("documentation URLs must use port 80 or 443")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") {
		return nil, errors.New("documentation tools only access public websites")
	}
	if ip := net.ParseIP(host); ip != nil && !publicIP(ip) {
		return nil, errors.New("documentation tools only access public websites")
	}
	return u, nil
}
func publicIP(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 0 || (v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127) || (v4[0] == 198 && (v4[1] == 18 || v4[1] == 19)) {
			return false
		}
	}
	return true
}

// Resolve and validate at dial time, then connect to that exact IP. No proxy,
// cookies, model credentials, or source credentials are shared with websites.
func newResearchClient() *http.Client {
	transport := &http.Transport{TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 64 << 10, IdleConnTimeout: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.New("could not resolve documentation host")
		}
		if len(ips) == 0 {
			return nil, errors.New("documentation host has no addresses")
		}
		for _, ip := range ips {
			if !publicIP(ip.IP) {
				return nil, errors.New("documentation host resolves to a non-public address")
			}
		}
		var last error
		for _, ip := range ips {
			connection, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return connection, nil
			}
			last = err
		}
		return nil, last
	}
	return &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many documentation redirects")
		}
		_, err := publicWebURL(req.URL.String())
		return err
	}}
}

var defaultResearchClient = newResearchClient()

func (s *Service) researchHTTP() *http.Client {
	if s.ResearchClient != nil {
		return s.ResearchClient
	}
	return defaultResearchClient
}
func researchBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("website returned HTTP %d; try another official documentation URL or ask the user for an accessible excerpt", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, researchBodyLimit+1))
	if err != nil {
		return nil, errors.New("could not read the website response")
	}
	if len(body) > researchBodyLimit {
		return nil, errors.New("page exceeds the 2 MiB limit; request a smaller documentation page")
	}
	return body, nil
}
func (s *Service) webSearch(ctx context.Context, arguments string) (string, error) {
	var request struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return "", err
	}
	request.Query = strings.TrimSpace(request.Query)
	if request.Query == "" || utf8.RuneCountInString(request.Query) > 600 {
		return "", errors.New("search query must contain 1–600 characters")
	}
	cfg, err := s.settings()
	if err != nil {
		return "", err
	}
	if cfg.ExaAPIKeyRef == "" {
		return "", errors.New("web search is not configured: add a Exa API key in Secrets and select it in Agent settings. read_webpage still works for known URLs. Ask the user to configure search or provide an official documentation URL; do not invent search results")
	}
	key, err := s.Secrets.Get(ctx, cfg.ExaAPIKeyRef)
	if err != nil || len(key) == 0 {
		return "", errors.New("the configured Exa API key is unavailable; update it in Secrets")
	}
	payload, err := json.Marshal(map[string]any{
		"query": request.Query, "type": "auto", "numResults": 5,
		"contents": map[string]any{"highlights": true, "text": false},
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.exa.ai/search", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", string(key))
	// Never forward the search key across redirects.
	client := *s.researchHTTP()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("could not contact Exa; retry or read an official documentation URL directly")
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return "", fmt.Errorf("Exa returned HTTP %d; check the search key, quota, or retry later", resp.StatusCode)
	}
	body, err := researchBody(resp)
	if err != nil {
		return "", err
	}
	var response struct {
		Results []struct {
			Title      string   `json:"title"`
			URL        string   `json:"url"`
			Highlights []string `json:"highlights"`
		} `json:"results"`
	}
	if err = json.Unmarshal(body, &response); err != nil {
		return "", errors.New("Exa returned an unreadable response")
	}
	results := []searchHit{}
	for _, item := range response.Results {
		if _, err := publicWebURL(item.URL); err != nil {
			continue
		}
		results = append(results, searchHit{webLink{clip(item.Title, 200), item.URL}, clip(strings.Join(item.Highlights, "\n"), 1000)})
		if len(results) == 5 {
			break
		}
	}
	output, err := json.Marshal(map[string]any{"query": request.Query, "results": results, "retrieved_at": time.Now().UTC().Format(time.RFC3339), "untrusted_content": true, "note": "Search snippets are leads, not verified documentation. Read the official pages before relying on their details."})
	// A provider error/response must not expose its credential in chat.
	return strings.ReplaceAll(string(output), string(key), "[REDACTED]"), err
}
func (s *Service) readWebpage(ctx context.Context, arguments string) (string, error) {
	var request struct {
		URL    string `json:"url"`
		Offset int    `json:"offset"`
	}
	if err := json.Unmarshal([]byte(arguments), &request); err != nil {
		return "", err
	}
	u, err := publicWebURL(request.URL)
	if err != nil {
		return "", err
	}
	if request.Offset < 0 || request.Offset > researchBodyLimit {
		return "", errors.New("invalid document offset")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Pompos/1.0 (documentation reader)")
	req.Header.Set("Accept", "text/html, text/plain, text/markdown, application/json;q=0.9")
	resp, err := s.researchHTTP().Do(req)
	if err != nil {
		return "", errors.New("could not fetch the public documentation page; it may be blocked, unavailable, or redirecting to a non-public address")
	}
	body, err := researchBody(resp)
	if err != nil {
		return "", err
	}
	finalURL := resp.Request.URL
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType, _, _ = mime.ParseMediaType(http.DetectContentType(body))
	}
	document := webDocument{URL: finalURL.String(), RetrievedAt: time.Now().UTC().Format(time.RFC3339), Offset: request.Offset, Untrusted: true, Links: []webLink{}}
	switch contentType {
	case "text/html", "application/xhtml+xml":
		document.Title, document.Text, document.Links, err = extractHTML(string(body), finalURL)
		if err != nil {
			return "", err
		}
	case "text/plain", "text/markdown", "text/x-markdown", "application/json", "application/yaml", "text/yaml", "application/x-yaml":
		document.Text = strings.ToValidUTF8(string(body), "�")
	default:
		return "", fmt.Errorf("unsupported document type %q; use an HTML, text, Markdown, JSON, or YAML version", contentType)
	}
	if strings.TrimSpace(document.Text) == "" {
		return "", errors.New("page contains no readable text; it may need JavaScript or sign-in. Find an accessible official page or ask for an excerpt")
	}
	text := []rune(document.Text)
	if request.Offset >= len(text) {
		return "", errors.New("offset is beyond the document; start at offset 0")
	}
	end := min(request.Offset+documentPageSize, len(text))
	document.Text = string(text[request.Offset:end])
	document.Truncated = end < len(text)
	if document.Truncated {
		document.NextOffset = &end
	}
	output, err := json.Marshal(document)
	return string(output), err
}
func clip(s string, n int) string { return string([]rune(s)[:min(utf8.RuneCountInString(s), n)]) }
func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}
func extractHTML(body string, base *url.URL) (string, string, []webLink, error) {
	root, err := html.Parse(strings.NewReader(body))
	if err != nil {
		return "", "", nil, errors.New("could not parse documentation HTML")
	}
	var main, article *html.Node
	title := ""
	var find func(*html.Node)
	find = func(n *html.Node) {
		if n.Type == html.ElementNode {
			if n.Data == "title" && n.FirstChild != nil {
				title = n.FirstChild.Data
			}
			if main == nil && (n.Data == "main" || attr(n, "role") == "main") {
				main = n
			}
			if article == nil && n.Data == "article" {
				article = n
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			find(c)
		}
	}
	find(root)
	if main != nil {
		root = main
	} else if article != nil {
		root = article
	}
	var text strings.Builder
	links := []webLink{}
	seen := map[string]bool{}
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, pre bool) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript", "svg", "template", "nav", "footer", "header", "aside", "form":
				return
			}
			for _, a := range n.Attr {
				if a.Key == "hidden" || (a.Key == "aria-hidden" && a.Val == "true") {
					return
				}
			}
			if n.Data == "pre" {
				pre = true
			}
			if n.Data == "a" && len(links) < 40 {
				href, e := base.Parse(attr(n, "href"))
				if e == nil && attr(n, "href") != "" {
					if _, e = publicWebURL(href.String()); e == nil && !seen[href.String()] {
						seen[href.String()] = true
						var label strings.Builder
						var labelText func(*html.Node)
						labelText = func(c *html.Node) {
							if c.Type == html.TextNode {
								label.WriteString(c.Data)
							}
							for child := c.FirstChild; child != nil; child = child.NextSibling {
								labelText(child)
							}
						}
						labelText(n)
						links = append(links, webLink{clip(strings.Join(strings.Fields(label.String()), " "), 200), href.String()})
					}
				}
			}
			switch n.Data {
			case "p", "div", "section", "article", "main", "li", "tr", "pre", "h1", "h2", "h3", "h4", "br":
				text.WriteString("\n")
			}
		}
		if n.Type == html.TextNode {
			if pre {
				text.WriteString(n.Data)
			} else {
				text.WriteString(strings.Join(strings.Fields(n.Data), " "))
				text.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, pre)
		}
		if n.Type == html.ElementNode {
			switch n.Data {
			case "p", "div", "section", "li", "tr", "pre", "h1", "h2", "h3", "h4":
				text.WriteString("\n")
			}
		}
	}
	walk(root, false)
	lines := strings.Split(text.String(), "\n")
	// Remove empty lines while retaining code indentation.
	clean := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			clean = append(clean, strings.TrimRight(line, " \t\r"))
		}
	}
	return clip(title, 200), strings.Join(clean, "\n"), links, nil
}

// Provider credentials are never eligible as ingestion credentials.
func (cfg Settings) reservedSecret(name string) bool {
	return name != "" && (name == cfg.APIKeyRef || name == cfg.ExaAPIKeyRef)
}
