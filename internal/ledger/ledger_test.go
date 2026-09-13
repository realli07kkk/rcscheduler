package ledger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(text); err != nil {
		t.Fatal(err)
	}
	f.Close()
}
func event(level, msg, key string) string {
	b, _ := json.Marshal(map[string]any{"time": time.Now().UTC(), "level": level, "msg": msg, "object": key})
	return string(b) + "\n"
}

func TestReplayRetryAndDistinctCounters(t *testing.T) {
	s, _ := store.New(t.TempDir())
	keys := []string{"copied", "existing", "failed", "unknown"}
	l := New(keys, "hash")
	l.Begin("one")
	dir := s.Path("one")
	os.Mkdir(dir, 0700)
	appendFile(t, filepath.Join(dir, "rclone.jsonl"), event("info", "Copied (new)", "copied")+event("info", "Copied (new)", "copied")+event("error", "Failed to copy: bad", "failed")+event("error", "Failed to copy: bad", "failed"))
	appendFile(t, filepath.Join(dir, "matched.txt"), "existing\n")
	appendFile(t, filepath.Join(dir, "failed.txt"), "failed\nfailed\n")
	if err := l.Consume(dir, "one", true); err != nil {
		t.Fatal(err)
	}
	l.Finish()
	if l.Checkpoint.Counts.CopiedObjects != 1 || l.Checkpoint.Counts.FailedObjects != 1 || l.Checkpoint.Counts.ErrorEvents != 2 || l.Checkpoint.Counts.UnknownObjects != 1 {
		t.Fatalf("%+v", l.Checkpoint.Counts)
	}
	if err := l.Save(s, s.Path("objects.json")); err != nil {
		t.Fatal(err)
	}
	l, err := Load(s.Path("objects.json"), keys, "hash")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Consume(dir, "one", true); err != nil {
		t.Fatal(err)
	}
	if l.Checkpoint.Counts.ErrorEvents != 2 {
		t.Fatal("重放重复累计错误")
	}
	l.Begin("two")
	dir = s.Path("two")
	os.Mkdir(dir, 0700)
	appendFile(t, filepath.Join(dir, "matched.txt"), "copied\nexisting\nunknown\n")
	appendFile(t, filepath.Join(dir, "rclone.jsonl"), event("info", "Copied (Rcat, new)", "failed"))
	if err := l.Consume(dir, "two", true); err != nil {
		t.Fatal(err)
	}
	l.Finish()
	c := l.Checkpoint.Counts
	if c.CopiedObjects != 2 || c.SkippedExistingObjects != 2 || c.FailedObjects != 0 || c.CompletedObjects != 4 || c.ErrorEvents != 2 {
		t.Fatalf("%+v", c)
	}
}

func TestPartialEvidenceAndUnknownOutput(t *testing.T) {
	dir := t.TempDir()
	l := New([]string{"a", "b"}, "hash")
	l.Begin("one")
	line := event("info", "Copied (new)", "a")
	path := filepath.Join(dir, "rclone.jsonl")
	appendFile(t, path, line[:len(line)-1])
	if err := l.Consume(dir, "one", false); err != nil {
		t.Fatal(err)
	}
	if l.Checkpoint.Cursors["one/rclone.jsonl"] != 0 {
		t.Fatal("消费了不完整尾行")
	}
	appendFile(t, path, "\n"+event("info", "Copied something unknown", "b"))
	if err := l.Consume(dir, "one", true); err != nil {
		t.Fatal(err)
	}
	l.Finish()
	if l.Checkpoint.Counts.CopiedObjects != 1 || l.Checkpoint.Counts.UnknownObjects != 1 {
		t.Fatalf("%+v", l.Checkpoint.Counts)
	}
	if err := os.WriteFile(path, []byte(""), 0600); err != nil {
		t.Fatal(err)
	}
	if err := l.Consume(dir, "one", true); err == nil || !strings.Contains(err.Error(), "截断") {
		t.Fatalf("%v", err)
	}
}

func TestActiveAndCountsPartition(t *testing.T) {
	l := New([]string{"a", "b", "c"}, "hash")
	l.Begin("one")
	l.result("a", model.ObjectCopied, "")
	l.Sample(model.Progress{AttemptID: "one", SampledAt: time.Now(), Checking: []string{"a", "b"}, Transferring: []model.Transfer{{Name: "b"}}})
	c := l.Recount()
	if c.TotalObjects != c.CopiedObjects+c.SkippedExistingObjects+c.ActiveObjects+c.FailedObjects+c.PendingObjects+c.UnknownObjects || c.ActiveObjects != 1 {
		t.Fatalf("%+v", c)
	}
	l.result("b", model.ObjectFailed, "failure")
	l.Recount()
	if l.Checkpoint.Counts.FailedObjects != 1 {
		t.Fatal("失败未替换活动状态")
	}
}
