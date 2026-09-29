package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestListSessionsRestoresHistoryNewestFirst(t *testing.T) {
	s := &Service{Dir: filepath.Join(t.TempDir(), "agent")}
	if sessions, err := s.ListSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("new chat directory: %#v %v", sessions, err)
	}
	if err := s.SaveSettings(Settings{Endpoint: "http://model.test", Model: "test"}); err != nil {
		t.Fatal(err)
	}
	old := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for i, session := range []Session{
		{ID: "old", PublishedID: "old", Draft: &Draft{Name: "Men", Table: "men_records"}, Messages: []Message{{Role: "system", Content: "hidden"}, {Role: "user", Content: "  Ingest\n high jump records  "}, {Role: "user", Content: "Now do women"}}},
		{ID: "recent", SavedIngestions: []SavedIngestion{{ID: "men"}, {ID: "women"}}, Messages: []Message{{Role: "user", Content: strings.Repeat("跳", 130)}}},
		{ID: "empty"},
	} {
		if err := s.save(session); err != nil {
			t.Fatal(err)
		}
		updated := old.Add(time.Duration(i) * time.Hour)
		if err := os.Chtimes(filepath.Join(s.Dir, session.ID+".json"), updated, updated); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteFile(filepath.Join(s.Dir, "recent.py"), []byte("not a chat")); err != nil {
		t.Fatal(err)
	}
	restarted := &Service{Dir: s.Dir}
	sessions, err := restarted.ListSessions()
	if err != nil || len(sessions) != 2 {
		t.Fatalf("listing should exclude settings, scripts and empty chats: %#v %v", sessions, err)
	}
	if sessions[0].ID != "recent" || sessions[0].Title != strings.Repeat("跳", 120)+"…" || sessions[0].SavedCount != 2 {
		t.Fatalf("recent chat: %#v", sessions[0])
	}
	if sessions[1].ID != "old" || sessions[1].Title != "Ingest high jump records" || sessions[1].SavedCount != 1 || !sessions[1].UpdatedAt.Equal(old) {
		t.Fatalf("legacy chat: %#v", sessions[1])
	}
	loaded, err := restarted.Load(sessions[0].ID)
	if err != nil || loaded.Messages[0].Content != strings.Repeat("跳", 130) {
		t.Fatal("listing modified the original conversation")
	}
}
