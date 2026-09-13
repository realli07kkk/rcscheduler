package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func TestLoadTenThousandSummariesWithoutLoadingManifests(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	settings := model.DefaultSettings()
	settings.Paused = true
	st.Write(st.Path("scheduler.json"), settings)
	now := time.Now().UTC()
	b := model.Batch{ID: "scale", Generation: 1, ImportState: "ready", SnapshotsComplete: true, CreatedAt: now, CommittedAt: &now, Request: model.ImportRequest{ID: "scale", Source: "source:bucket", Destination: "target:bucket"}}
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("task-%05d", i)
		m := model.Manifest{TaskID: id, RelativePath: id + ".txt", SHA256: strings.Repeat("a", 64), Objects: 1}
		b.Manifests = append(b.Manifests, m)
		task := model.Task{ID: id, BatchID: b.ID, Generation: 1, Name: m.RelativePath, Source: b.Request.Source, Destination: b.Request.Destination, Manifest: m, State: model.Queued, DesiredState: "run", CreatedAt: now, QueuedAt: now, Counts: model.Counts{TotalObjects: 1, PendingObjects: 1}}
		data, err := json.Marshal(struct {
			Version int        `json:"version"`
			Record  model.Task `json:"record"`
		}{1, task})
		if err != nil {
			t.Fatal(err)
		}
		dir := st.Path("tasks", id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "task.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		// 故意不创建清单文件，启动只能读取摘要。
	}
	if err := st.Write(st.Path("batches", b.ID, "batch.json"), b); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	svc, err := New(st, &fakeEngine{}, Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close(context.Background())
	items, total := svc.Tasks("scale", "queued", 9995, 100)
	if total != 10000 || len(items) != 5 {
		t.Fatalf("total=%d items=%d", total, len(items))
	}
	writes := map[string]bool{}
	st.SetFault(func(stage, path string) error {
		if stage == "before_write" {
			writes[path] = true
		}
		return nil
	})
	if _, err := svc.Priority("task-05000", 10); err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 || !writes[st.Path("tasks", "task-05000", "task.json")] {
		t.Fatalf("更新一个任务重写了其他文件: %v", writes)
	}
	t.Logf("10,000 个任务摘要加载、分页和单任务更新: %s", time.Since(start))
}
