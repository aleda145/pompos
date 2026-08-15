package validation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"pompos/internal/ingestion"
)

type SourceValidator interface {
	Validate(context.Context, ingestion.Source) error
}

type HTTPSourceValidator struct {
	Client   *http.Client
	Resolver interface {
		LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
	}
}

func (v HTTPSourceValidator) Validate(ctx context.Context, source ingestion.Source) error {
	if source.Type == "github" {
		return v.validateGitHub(ctx, source)
	}
	rawURL := source.URL
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("CSV URL must be a valid HTTP or HTTPS URL")
	}
	if err := v.validatePublicURL(ctx, parsed); err != nil {
		return err
	}
	client := v.Client
	if client == nil {
		client = http.DefaultClient
	}

	safeClient := *client
	if client.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = v.publicDialContext
		safeClient.Transport = transport
	} else if transport, ok := client.Transport.(*http.Transport); ok {
		clone := transport.Clone()
		clone.Proxy = nil
		clone.DialContext = v.publicDialContext
		safeClient.Transport = clone
	}
	previousRedirect := client.CheckRedirect
	safeClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := v.validatePublicURL(req.Context(), req.URL); err != nil {
			return err
		}
		if previousRedirect != nil {
			return previousRedirect(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	response, err := request(ctx, &safeClient, http.MethodHead, rawURL)
	if err != nil {
		return fmt.Errorf("CSV URL is not reachable: %w", err)
	}
	response.Body.Close()
	if response.StatusCode == http.StatusMethodNotAllowed || response.StatusCode == http.StatusNotImplemented {
		response, err = request(ctx, &safeClient, http.MethodGet, rawURL)
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

func (v HTTPSourceValidator) validatePublicURL(ctx context.Context, parsed *url.URL) error {
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "localhost" || host == "" {
		return errors.New("policy.http-file-public-network: source URL must not target localhost or a private network")
	}
	if literal := net.ParseIP(host); literal != nil {
		if !publicIP(literal) {
			return errors.New("policy.http-file-public-network: source URL must not target localhost or a private network")
		}
		return nil
	}
	resolver := v.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolve CSV URL hostname: %w", err)
	}
	if len(addresses) == 0 {
		return errors.New("CSV URL hostname did not resolve")
	}
	for _, address := range addresses {
		if !publicIP(address.IP) {
			return errors.New("policy.http-file-public-network: source URL resolves to a private or local network")
		}
	}
	return nil
}

func publicIP(ip net.IP) bool {
	return ip != nil && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified() && !ip.IsMulticast()
}

func (v HTTPSourceValidator) publicDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("parse HTTP destination: %w", err)
	}
	var addresses []net.IPAddr
	if literal := net.ParseIP(host); literal != nil {
		addresses = []net.IPAddr{{IP: literal}}
	} else {
		resolver := v.Resolver
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		addresses, err = resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve HTTP destination: %w", err)
		}
	}
	for _, candidate := range addresses {
		if !publicIP(candidate.IP) {
			return nil, errors.New("policy.http-file-public-network: HTTP destination resolved to a private or local network")
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("HTTP destination did not resolve")
	}
	return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
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
