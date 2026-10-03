package agent

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type SessionSummary struct {
	ID         string
	Title      string
	UpdatedAt  time.Time
	SavedCount int
	External   bool
}

var ErrMCPConversation = errors.New("MCP conversations belong to the MCP client")

// LoadWebChat repairs empty placeholders created by the former web MCP mode.
// Actual MCP drafts and their conversation history remain owned by the client.
func (s *Service) LoadWebChat(id string) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.load(id)
	if err != nil || !v.External {
		return v, err
	}
	if len(v.Messages) == 2 && v.Messages[0].Role == "system" &&
		v.Messages[1].Role == "user" && v.Messages[1].Content == "New ingestion" &&
		len(v.Messages[0].Calls) == 0 && len(v.Messages[1].Calls) == 0 &&
		v.Draft == nil && v.Pending == nil && v.Loading == nil && v.Validation == nil &&
		v.PublishedID == "" && v.DraftID == "" && v.TestedDigest == "" && !v.Ready &&
		len(v.SavedIngestions) == 0 {
		v.External = false
		v.Messages = nil
		return v, s.save(v)
	}
	return v, ErrMCPConversation
}

// ListSessions reads the existing conversation files, newest first.
func (s *Service) ListSessions() ([]SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sessions []SessionSummary
	for _, entry := range entries {
		if !entry.Type().IsRegular() || entry.Name() == "settings.json" || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		if !validID.MatchString(id) {
			continue
		}
		v, err := s.load(id)
		if err != nil {
			return nil, fmt.Errorf("read chat %s: %w", id, err)
		}
		var title string
		for _, message := range v.Messages {
			if message.Role == "user" {
				title = strings.Join(strings.Fields(message.Content), " ")
				if title != "" {
					break
				}
			}
		}
		if title == "" {
			continue
		}
		if runes := []rune(title); len(runes) > 120 {
			title = string(runes[:120]) + "…"
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, SessionSummary{ID: id, Title: title, UpdatedAt: info.ModTime().UTC(), SavedCount: len(v.SavedIngestions), External: v.External})
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].UpdatedAt.Equal(sessions[j].UpdatedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions, nil
}
