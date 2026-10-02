package web

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"pompos/internal/compiler"
	"pompos/internal/mcpserver"
	runnerpython "pompos/internal/runner/python"
)

func (a *App) mcpHandler() http.Handler {
	server := mcpserver.New(a.Agent, mcpserver.Operations{
		Save: a.publishSession, List: a.ingestionList, Get: a.getIngestion,
		Run: a.queueIngestion, Schedule: a.setIngestionSchedule, Preview: a.previewIngestion,
	})
	transport := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		// This is a local, single-operator endpoint. Reject DNS rebinding and browser
		// origins rather than trusting forwarded headers or enabling CORS.
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			http.Error(w, "MCP requires a localhost URL", http.StatusForbidden)
			return
		}
		if r.Header.Get("Origin") != "" || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			http.Error(w, "Browser origins are not allowed for MCP", http.StatusForbidden)
			return
		}
		cfg, err := a.Agent.Settings()
		if err != nil || !cfg.MCPEnabled {
			http.Error(w, "MCP is not enabled", http.StatusServiceUnavailable)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		transport.ServeHTTP(w, r)
	})
}

// Protect browser writes from cross-site forms.
func sameOriginWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
			origin := r.Header.Get("Origin")
			parsed, err := url.Parse(origin)
			if r.Header.Get("Sec-Fetch-Site") == "cross-site" || (origin != "" && (err != nil || parsed.Host != r.Host || (parsed.Scheme != "http" && parsed.Scheme != "https"))) {
				http.Error(w, "Cross-origin request rejected", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func mcpEndpoint(host string) string {
	return fmt.Sprintf("http://127.0.0.1:%s/mcp", localPort(host))
}

func (a *App) toggleMCP(w http.ResponseWriter, r *http.Request) {
	if a.Agent == nil {
		http.Error(w, "Agent is not configured", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid MCP settings", http.StatusBadRequest)
		return
	}
	enabled := r.PostForm.Get("enabled")
	if enabled != "on" && enabled != "" {
		http.Error(w, "Invalid MCP switch value", http.StatusBadRequest)
		return
	}
	if err := a.Agent.SetMCPEnabled(enabled == "on"); err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, "/settings/agent#mcp", http.StatusSeeOther)
}

func localPort(host string) string {
	_, port, err := net.SplitHostPort(host)
	if err == nil {
		if number, err := strconv.Atoi(port); err == nil && number > 0 && number <= 65535 {
			return strconv.Itoa(number)
		}
	}
	return "8080"
}

func (a *App) previewIngestion(ctx context.Context, id string) (runnerpython.TablePreview, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	item, err := a.getIngestion(ctx, id)
	if err != nil {
		return runnerpython.TablePreview{}, err
	}
	if item.LoadError != "" {
		return runnerpython.TablePreview{}, errors.New(item.LoadError)
	}
	if a.Previewer == nil {
		return runnerpython.TablePreview{}, errors.New(runnerpython.PreviewUnavailable)
	}
	return a.Previewer.Preview(ctx, compiler.ExecutionPlan{
		DestinationType: item.Destination.Type, DestinationPath: item.Destination.Path,
		DestinationSchema: item.Destination.Schema, DestinationObject: item.Destination.Table, SecretRefs: item.Runtime.SecretRefs,
	})
}
