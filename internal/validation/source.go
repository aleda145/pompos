package validation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"pompos/internal/ingestion"
)

type SourceValidator interface {
	Validate(context.Context, ingestion.Source) error
}

type HTTPSourceValidator struct {
	Client *http.Client
}

func (v HTTPSourceValidator) Validate(ctx context.Context, source ingestion.Source) error {
	if source.Type == "github" {
		return v.validateGitHub(ctx, source)
	}
	rawURL := source.URL
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

func (v HTTPSourceValidator) validateGitHub(ctx context.Context, source ingestion.Source) error {
	if strings.TrimSpace(source.AccessToken) == "" {
		return fmt.Errorf("GitHub access token is required because ingestr uses GitHub's authenticated GraphQL API")
	}
	client := v.Client
	if client == nil {
		client = http.DefaultClient
	}
	endpoint := "https://api.github.com/repos/" + url.PathEscape(source.Owner) + "/" + url.PathEscape(source.Repository)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build GitHub request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+source.AccessToken)
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub repository is not reachable: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("GitHub rejected the token; check that it is valid")
	}
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("GitHub repository was not found or the token cannot access it")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("GitHub returned HTTP %d", response.StatusCode)
	}
	if source.Table == "stargazers" {
		return validateGitHubStargazers(ctx, client, source)
	}
	return nil
}

func validateGitHubStargazers(ctx context.Context, client *http.Client, source ingestion.Source) error {
	endpoint := "https://api.github.com/repos/" + url.PathEscape(source.Owner) + "/" + url.PathEscape(source.Repository) + "/stargazers?per_page=1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build GitHub stargazers request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.star+json")
	req.Header.Set("Authorization", "Bearer "+source.AccessToken)
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("GitHub stargazers are not reachable: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("this token cannot read stargazers; GitHub now limits stargazer lists to repository admins and collaborators, and fine-grained tokens require Administration: read and write access")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("GitHub stargazers returned HTTP %d", response.StatusCode)
	}
	return nil
}

func ParseGitHubRepository(value string) (owner, repository string, err error) {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, "/")
	if strings.HasPrefix(value, "http://") || strings.HasPrefix(value, "https://") {
		parsed, parseErr := url.Parse(value)
		if parseErr != nil || !strings.EqualFold(parsed.Host, "github.com") {
			return "", "", fmt.Errorf("enter a GitHub URL or owner/repository")
		}
		value = strings.Trim(parsed.Path, "/")
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("enter a GitHub URL or owner/repository")
	}
	return parts[0], strings.TrimSuffix(parts[1], ".git"), nil
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
