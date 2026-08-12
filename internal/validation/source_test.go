package validation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
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
	if err := validator.Validate(context.Background(), "https://example.com/data.csv"); err != nil {
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
