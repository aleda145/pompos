package python

import (
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"
)

// Keep a suffix across writes so secrets split across pipe reads are never
// published. Match longer secrets first, including URL-encoded credentials.
type redactedLog struct {
	write   func(string)
	secrets []string
	pending string
	keep    int
}

func newRedactedLog(values map[string]string, write func(string)) *redactedLog {
	l := &redactedLog{write: write}
	for _, value := range values {
		if value != "" {
			l.secrets = append(l.secrets, value, url.QueryEscape(value))
		}
	}
	sort.Slice(l.secrets, func(i, j int) bool { return len(l.secrets[i]) > len(l.secrets[j]) })
	if len(l.secrets) > 0 {
		l.keep = len(l.secrets[0]) - 1
	}
	return l
}

func (l *redactedLog) Write(p []byte) (int, error) {
	l.pending += string(p)
	l.flush(false)
	return len(p), nil
}

func (l *redactedLog) flush(final bool) {
	end := len(l.pending) - l.keep
	if final {
		end = len(l.pending)
	}
	var output strings.Builder
	i := 0
	for i < end {
		matched := false
		for _, secret := range l.secrets {
			if strings.HasPrefix(l.pending[i:], secret) {
				output.WriteString("[REDACTED]")
				i += len(secret)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if !final && !utf8.FullRuneInString(l.pending[i:]) {
			break
		}
		_, size := utf8.DecodeRuneInString(l.pending[i:])
		output.WriteString(l.pending[i : i+size])
		i += size
	}
	l.pending = l.pending[i:]
	if output.Len() > 0 {
		l.write(output.String())
	}
}
