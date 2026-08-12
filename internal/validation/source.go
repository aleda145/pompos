package validation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type SourceValidator interface {
	Validate(context.Context, string) error
}

type HTTPSourceValidator struct {
	Client *http.Client
}

func (v HTTPSourceValidator) Validate(ctx context.Context, rawURL string) error {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("CSV URL must be a valid HTTP or HTTPS URL")
	}
	client := v.Client
	if client == nil {
		client = http.DefaultClient
	}

	response, err := request(ctx, client, http.MethodHead, rawURL)
	if err != nil {
		return fmt.Errorf("CSV URL is not reachable: %w", err)
	}
	response.Body.Close()
	if response.StatusCode == http.StatusMethodNotAllowed || response.StatusCode == http.StatusNotImplemented {
		response, err = request(ctx, client, http.MethodGet, rawURL)
		if err != nil {
			return fmt.Errorf("CSV URL is not reachable: %w", err)
		}
		_, _ = io.CopyN(io.Discard, response.Body, 1)
		response.Body.Close()
	}
	if response.StatusCode < 200 || response.StatusCode >= 400 {
		return fmt.Errorf("CSV URL returned HTTP %d", response.StatusCode)
	}
	return nil
}

func request(ctx context.Context, client *http.Client, method, rawURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if method == http.MethodGet {
		req.Header.Set("Range", "bytes=0-0")
	}
	return client.Do(req)
}
