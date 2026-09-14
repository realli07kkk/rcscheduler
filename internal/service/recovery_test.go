package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func openAt(t *testing.T, st *store.Store, e *fakeEngine) *Service {
	t.Helper()
	s, err := New(st, e, Options{PollInterval: 10 * time.Millisecond, CheckpointInterval: 30 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func closeService(t *testing.T, s *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySettingsAndRcloneOverridesAfterRestart(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	legacy := `{"version":1,"record":{"revision":3,"maxRunning":1,"bandwidthBudget":"40M","transfers":64,"checkers":64,"paused":false}}`
	if err := os.WriteFile(st.Path("scheduler.json"), []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}
	e := &fakeEngine{}
	s := openAt(t, st, e)
	if got := s.Settings(); got.Revision != 3 || got.UserAgent != "" || got.S3UploadConcurrency != 0 {
		t.Fatalf("旧配置加载错误: %+v", got)
	}
	ua, uploads := "aws-sdk-go-v2/1.41.4", 8
	if _, err := s.UpdateSettings(model.SettingsPatch{UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
		t.Fatal(err)
	}
	want := s.Settings()
	closeService(t, s)
	s2 := openAt(t, st, e)
	if got := s2.Settings(); got != want {
		t.Fatalf("重启后配置改变: %+v", got)
	}
	ua, uploads = "", 0
	if _, err := s2.UpdateSettings(model.SettingsPatch{UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
		t.Fatal(err)
	}
	want = s2.Settings()
	closeService(t, s2)
	s3 := openAt(t, st, e)
	defer closeService(t, s3)
	if got := s3.Settings(); got != want {
		t.Fatalf("清除覆盖值未持久化: %+v", got)
	}
}

func TestRecoverUncommittedBatchFromSnapshots(t *testing.T) {
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e := &fakeEngine{}
	s := openAt(t, st, e)
	paused := true
	s.UpdateSettings(model.SettingsPatch{Paused: &paused})
	s.Start(context.Background())
	st.SetFault(func(stage, path string) error {
		if stage == "before_rename" && path == st.Path("batches", "batch", "batch.json") {
			var b model.Batch
			if store.Read(path, &b) == nil && b.SnapshotsComplete && b.ImportState == "validating" {
				return errors.New("power loss before commit")
			}
		}
		return nil
	})
	dir := inputDir(t, 2)
	s.Import(request(dir, "batch"))
	until(t, 3*time.Second, func() bool { return s.Health()["ready"] == false })
	closeService(t, s)
	st.SetFault(nil)
	if e.count() != 0 {
		t.Fatal("批次提交前派发任务")
	}
	os.WriteFile(filepath.Join(dir, "000.txt"), []byte("changed\n"), 0600)
	s2 := openAt(t, st, e)
	defer closeService(t, s2)
	if err := s2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := waitBatch(t, s2, "batch", "ready")
	if b.Summary.Tasks != 2 {
		t.Fatalf("%+v", b)
	}
	if got := readFile(t, st.Path("tasks", b.Manifests[0].TaskID, "manifest.txt")); got != "key-000\n" {
		t.Fatal("恢复重新读取了外部目录")
	}
}

func TestStorageFailureStopsSchedulingAfterRename(t *testing.T) {
	e := &fakeEngine{}
	s, st := newTestService(t, e)
	one := 1
	s.UpdateSettings(model.SettingsPatch{MaxRunning: &one})
	s.Start(context.Background())
	s.Import(request(inputDir(t, 3), "batch"))
	waitBatch(t, s, "batch", "ready")
	until(t, 3*time.Second, func() bool { return e.count() == 1 })
	st.SetFault(func(stage, path string) error {
		if stage == "after_rename" && path == st.Path("scheduler.json") {
			return errors.New("directory fsync failed")
		}
		return nil
	})
	bw := "40M"
	if _, err := s.UpdateSettings(model.SettingsPatch{BandwidthBudget: &bw}); err == nil {
		t.Fatal("未报告提交不确定")
	}
	until(t, 3*time.Second, func() bool { return s.Health()["running"] == 0 })
	if s.Health()["ready"] != false || e.count() != 1 {
		t.Fatal("存储失败后继续派发")
	}
	st.SetFault(nil)
}

func TestCorruptTaskCannotBeOverwrittenByPriority(t *testing.T) {
	st, _ := store.New(t.TempDir())
	e := &fakeEngine{}
	s := openAt(t, st, e)
	pause := true
	s.UpdateSettings(model.SettingsPatch{Paused: &pause})
	s.Start(context.Background())
	s.Import(request(inputDir(t, 1), "batch"))
	b := waitBatch(t, s, "batch", "ready")
	closeService(t, s)
	id := b.Manifests[0].TaskID
	path := st.Path("tasks", id, "task.json")
	os.WriteFile(path, []byte("broken evidence"), 0600)
	s2 := openAt(t, st, e)
	defer closeService(t, s2)
	task, err := s2.Task(id)
	if err != nil || task.State != model.Blocked {
		t.Fatalf("%+v %v", task, err)
	}
	if _, err := s2.Priority(id, 10); err == nil {
		t.Fatal("覆盖了损坏文档")
	}
	if got := readFile(t, path); got != "broken evidence" {
		t.Fatal(got)
	}
}

func TestSnapshotWriteFailureIsStorageFault(t *testing.T) {
	e := &fakeEngine{}
	s, st := newTestService(t, e)
	s.Start(context.Background())
	st.SetFault(func(stage, path string) error {
		if stage == "before_file_sync" && filepath.Ext(path) == ".txt" {
			return errors.New("disk full")
		}
		return nil
	})
	s.Import(request(inputDir(t, 1), "batch"))
	until(t, 3*time.Second, func() bool { return s.Health()["ready"] == false })
	if e.count() != 0 {
		t.Fatal("快照写入失败后仍然派发")
	}
	st.SetFault(nil)
}
