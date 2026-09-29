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
		sessions = append(sessions, SessionSummary{ID: id, Title: title, UpdatedAt: info.ModTime().UTC(), SavedCount: len(v.SavedIngestions)})
	}
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].UpdatedAt.Equal(sessions[j].UpdatedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions, nil
}
