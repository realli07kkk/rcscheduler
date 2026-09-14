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
	"sync"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/ledger"
	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

type fakeEngine struct {
	mu         sync.Mutex
	launches   []runner.Launch
	processes  []*fakeProcess
	recoverErr error
}

func (f *fakeEngine) Version() string                               { return "test" }
func (f *fakeEngine) Socket(id string) string                       { return filepath.Join(os.TempDir(), "fake-"+id+".sock") }
func (f *fakeEngine) Recover(context.Context, string, string) error { return f.recoverErr }
func (f *fakeEngine) Start(spec runner.Launch) (runner.Process, error) {
	p := &fakeProcess{done: make(chan struct{})}
	f.mu.Lock()
	f.launches = append(f.launches, spec)
	f.processes = append(f.processes, p)
	f.mu.Unlock()
	return p, nil
}
func (f *fakeEngine) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.launches) }
func (f *fakeEngine) get(index int) (runner.Launch, *fakeProcess) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.launches[index], f.processes[index]
}

type fakeProcess struct {
	mu     sync.Mutex
	done   chan struct{}
	result runner.Result
	once   sync.Once
}

func (p *fakeProcess) PID() int                                    { return 12345 }
func (p *fakeProcess) Done() <-chan struct{}                       { return p.done }
func (p *fakeProcess) Result() runner.Result                       { p.mu.Lock(); defer p.mu.Unlock(); return p.result }
func (p *fakeProcess) Stats(context.Context) (ledger.Stats, error) { return ledger.Stats{}, nil }
func (p *fakeProcess) Stop(context.Context) error                  { p.finish(-1); return nil }
func (p *fakeProcess) finish(code int) {
	p.once.Do(func() { p.mu.Lock(); p.result = runner.Result{ExitCode: code}; p.mu.Unlock(); close(p.done) })
}

func newTestService(t *testing.T, engine runner.Engine) (*Service, *store.Store) {
	t.Helper()
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(s, engine, Options{PollInterval: 10 * time.Millisecond, CheckpointInterval: 30 * time.Millisecond, RetryDelays: []time.Duration{20 * time.Millisecond, 30 * time.Millisecond}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := svc.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return svc, s
}
func until(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待状态超时")
}
func inputDir(t *testing.T, n int) string {
	t.Helper()
	dir := t.TempDir()
	for i := 0; i < n; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%03d.txt", i)), []byte(fmt.Sprintf("key-%03d\n", i)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
func request(dir, id string) model.ImportRequest {
	return model.ImportRequest{ID: id, ManifestDir: dir, Source: "source:bucket", Destination: "target:bucket"}
}
func waitBatch(t *testing.T, s *Service, id, state string) model.Batch {
	t.Helper()
	var b model.Batch
	until(t, 5*time.Second, func() bool { b, _ = s.Batch(id); return b.ImportState == state })
	return b
}
func completeFake(t *testing.T, e *fakeEngine, index int) {
	t.Helper()
	spec, p := e.get(index)
	if err := os.MkdirAll(spec.AttemptDir, 0700); err != nil {
		t.Fatal(err)
	}
	line, _ := json.Marshal(map[string]any{"time": time.Now().UTC(), "level": "info", "msg": "Copied (new)", "object": strings.TrimSpace(readFile(t, spec.ManifestPath))})
	if err := os.WriteFile(filepath.Join(spec.AttemptDir, "rclone.jsonl"), append(line, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	p.finish(0)
}
func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestBatchValidationBarrierAndRetryImport(t *testing.T) {
	e := &fakeEngine{}
	s, _ := newTestService(t, e)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := inputDir(t, 2)
	os.WriteFile(filepath.Join(dir, "001.txt"), []byte("a\na\n"), 0600)
	if _, err := s.Import(request(dir, "batch")); err != nil {
		t.Fatal(err)
	}
	waitBatch(t, s, "batch", "rejected")
	if e.count() != 0 {
		t.Fatal("校验失败前启动了迁移")
	}
	_, total := s.Tasks("batch", "", 0, 100)
	if total != 0 {
		t.Fatal("暴露了未提交任务")
	}
	report, err := s.Validation("batch")
	if err != nil || len(report.Issues) != 1 || report.Issues[0].Line != 2 {
		t.Fatalf("%+v %v", report, err)
	}
	os.WriteFile(filepath.Join(dir, "001.txt"), []byte("fixed\n"), 0600)
	if _, err := s.Import(request(dir, "batch")); err != nil {
		t.Fatal(err)
	}
	b := waitBatch(t, s, "batch", "ready")
	if b.Generation != 2 {
		t.Fatal("没有创建新的校验代次")
	}
	until(t, 3*time.Second, func() bool { return e.count() == 2 })
}

func TestRollingConcurrencyAndConfigurationSnapshots(t *testing.T) {
	e := &fakeEngine{}
	s, _ := newTestService(t, e)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Import(request(inputDir(t, 6), "batch")); err != nil {
		t.Fatal(err)
	}
	waitBatch(t, s, "batch", "ready")
	until(t, 3*time.Second, func() bool { return e.count() == 4 })
	for i := 0; i < 4; i++ {
		spec, _ := e.get(i)
		if spec.Attempt.BandwidthBytesPerSecond != 5<<20 {
			t.Fatal("初始分配错误")
		}
	}
	bw := "40M"
	if _, err := s.UpdateSettings(model.SettingsPatch{BandwidthBudget: &bw}); err != nil {
		t.Fatal(err)
	}
	completeFake(t, e, 0)
	until(t, 3*time.Second, func() bool { return e.count() == 5 })
	fifth, _ := e.get(4)
	if fifth.Attempt.BandwidthBytesPerSecond != 10<<20 {
		t.Fatal("新执行未使用新配置")
	}
	for i := 1; i < 4; i++ {
		old, _ := e.get(i)
		if old.Attempt.BandwidthBytesPerSecond != 5<<20 {
			t.Fatal("修改了旧执行")
		}
	}
	bw = "20M"
	if _, err := s.UpdateSettings(model.SettingsPatch{BandwidthBudget: &bw}); err != nil {
		t.Fatal(err)
	}
	completeFake(t, e, 1)
	until(t, 3*time.Second, func() bool { return e.count() == 6 })
	sixth, _ := e.get(5)
	if sixth.Attempt.BandwidthBytesPerSecond != 5<<20 {
		t.Fatal("降低带宽未仅对新执行生效")
	}
	for i := 2; i < 6; i++ {
		completeFake(t, e, i)
	}
	until(t, 3*time.Second, func() bool { _, total := s.Tasks("batch", "succeeded", 0, 100); return total == 6 })
}

func TestUnknownResultRetryAndPause(t *testing.T) {
	e := &fakeEngine{}
	s, _ := newTestService(t, e)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Import(request(inputDir(t, 1), "batch"))
	b := waitBatch(t, s, "batch", "ready")
	id := b.Manifests[0].TaskID
	until(t, 3*time.Second, func() bool { return e.count() == 1 })
	_, p := e.get(0)
	p.finish(0)
	until(t, 3*time.Second, func() bool { x, _ := s.Task(id); return x.State == model.NeedsReview && x.Counts.UnknownObjects == 1 })
	if _, err := s.Action(id, "retry"); err != nil {
		t.Fatal(err)
	}
	until(t, 3*time.Second, func() bool { return e.count() == 2 })
	if _, err := s.Action(id, "pause"); err != nil {
		t.Fatal(err)
	}
	until(t, 3*time.Second, func() bool { x, _ := s.Task(id); return x.State == model.Paused })
	bw := "40M"
	s.UpdateSettings(model.SettingsPatch{BandwidthBudget: &bw})
	s.Action(id, "resume")
	until(t, 3*time.Second, func() bool { return e.count() == 3 })
	spec, _ := e.get(2)
	if spec.Attempt.BandwidthBytesPerSecond != 10<<20 || spec.Attempt.Reason != "resume" {
		t.Fatalf("%+v", spec.Attempt)
	}
	completeFake(t, e, 2)
	until(t, 3*time.Second, func() bool { x, _ := s.Task(id); return x.State == model.Succeeded })
}

func TestAutomaticRetryLimit(t *testing.T) {
	e := &fakeEngine{}
	s, _ := newTestService(t, e)
	ua, uploads := "initial-agent", 8
	if _, err := s.UpdateSettings(model.SettingsPatch{UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	s.Import(request(inputDir(t, 1), "batch"))
	b := waitBatch(t, s, "batch", "ready")
	for i := 0; i < 3; i++ {
		want := i + 1
		until(t, 3*time.Second, func() bool { return e.count() == want })
		spec, p := e.get(i)
		if spec.Attempt.UserAgent != ua || spec.Attempt.S3UploadConcurrency != uploads {
			t.Fatalf("自动重试未使用最新配置: %+v", spec.Attempt)
		}
		ua, uploads = "retry-agent", 4
		if _, err := s.UpdateSettings(model.SettingsPatch{UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
			t.Fatal(err)
		}
		p.finish(5)
	}
	until(t, 3*time.Second, func() bool {
		x, _ := s.Task(b.Manifests[0].TaskID)
		return x.State == model.Failed && x.FailuresInCycle == 3
	})
	if e.count() != 3 {
		t.Fatal("超过三次自动执行")
	}
}

func TestSerialManifestWorkflow(t *testing.T) {
	for _, tc := range []struct {
		name  string
		code  int
		state model.TaskState
	}{
		{"success", 0, model.Succeeded},
		{"incomplete", 0, model.NeedsReview},
		{"failed", 1, model.Failed},
		{"retry_wait", 5, model.RetryWait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &fakeEngine{}
			s, _ := newTestService(t, e)
			s.options.RetryDelays = []time.Duration{time.Hour}
			one, workers, uploads := 1, 64, 8
			bw, ua := "40M", "aws-sdk-go-v2/1.41.4"
			if _, err := s.UpdateSettings(model.SettingsPatch{MaxRunning: &one, BandwidthBudget: &bw, Transfers: &workers, Checkers: &workers, UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			req := request(inputDir(t, 3), "batch")
			req.Source = "source,no_head_object=true:example-bucket"
			if _, err := s.Import(req); err != nil {
				t.Fatal(err)
			}
			waitBatch(t, s, "batch", "ready")
			until(t, 3*time.Second, func() bool { return e.count() == 1 })
			first, p := e.get(0)
			if first.Task.Name != "000.txt" || first.Task.Source != req.Source || first.Attempt.UserAgent != ua || first.Attempt.S3UploadConcurrency != uploads {
				t.Fatalf("首份清单参数错误: %+v", first)
			}
			ua, uploads = "updated-agent", 4
			if _, err := s.UpdateSettings(model.SettingsPatch{UserAgent: &ua, S3UploadConcurrency: &uploads}); err != nil {
				t.Fatal(err)
			}
			if tc.state == model.Succeeded {
				completeFake(t, e, 0)
			} else {
				p.finish(tc.code)
			}
			for i := 1; i < 3; i++ {
				until(t, 3*time.Second, func() bool { return e.count() == i+1 })
				spec, _ := e.get(i)
				if spec.Task.Name != fmt.Sprintf("%03d.txt", i) || spec.Attempt.UserAgent != ua || spec.Attempt.S3UploadConcurrency != uploads {
					t.Fatalf("后续清单参数错误: %+v", spec)
				}
				prior, _ := e.get(i - 1)
				previous, err := s.Task(prior.Task.ID)
				if err != nil || previous.Attempts[0].EndedAt == nil || spec.Attempt.StartedAt.Before(*previous.Attempts[0].EndedAt) {
					t.Fatalf("串行执行发生重叠: %+v %v", previous, err)
				}
				completeFake(t, e, i)
			}
			until(t, 3*time.Second, func() bool { return s.Health()["running"] == 0 })
			old, err := s.Task(first.Task.ID)
			if err != nil || old.State != tc.state || old.Attempts[0].UserAgent != "aws-sdk-go-v2/1.41.4" || old.Attempts[0].S3UploadConcurrency != 8 {
				t.Fatalf("旧执行状态或快照被修改: %+v %v", old, err)
			}
			for i := 0; i < 3; i++ {
				spec, _ := e.get(i)
				if spec.Attempt.BandwidthBytesPerSecond != 40<<20 || spec.Attempt.Transfers != 64 || spec.Attempt.Checkers != 64 {
					t.Fatalf("串行带宽或对象并发错误: %+v", spec.Attempt)
				}
			}
		})
	}
}

func TestCommittedSnapshotIgnoresSourceEdits(t *testing.T) {
	e := &fakeEngine{}
	s, st := newTestService(t, e)
	pause := true
	s.UpdateSettings(model.SettingsPatch{Paused: &pause})
	s.Start(context.Background())
	dir := inputDir(t, 1)
	s.Import(request(dir, "batch"))
	b := waitBatch(t, s, "batch", "ready")
	os.WriteFile(filepath.Join(dir, "000.txt"), []byte("different\n"), 0600)
	if got := readFile(t, st.Path("tasks", b.Manifests[0].TaskID, "manifest.txt")); got != "key-000\n" {
		t.Fatal(got)
	}
	different := request(dir, "batch")
	different.Source = "another:bucket"
	if _, err := s.Import(different); err == nil {
		t.Fatal("覆盖了已提交批次")
	}
}

func TestRealRcloneContractOver100Objects(t *testing.T) {
	binary := os.Getenv("RCSCHEDULER_TEST_RCLONE")
	if binary == "" {
		t.Skip("设置 RCSCHEDULER_TEST_RCLONE 运行真实 rclone 契约测试")
	}
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	destination := filepath.Join(dir, "destination")
	lists := filepath.Join(dir, "lists")
	for _, d := range []string{source, destination, lists} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{"中文.txt", " leading and trailing ", "#literal", ";literal", "file with spaces"}
	for i := 0; i < 205; i++ {
		keys = append(keys, fmt.Sprintf("object-%03d", i))
	}
	for _, key := range keys {
		if err := os.WriteFile(filepath.Join(source, key), []byte("new data "+key), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(destination, keys[0]), []byte("keep existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lists, "001.txt"), []byte(strings.Join(keys, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "rclone.conf")
	os.WriteFile(config, nil, 0600)
	st, err := store.New(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := runner.New(context.Background(), binary, config, st.Root, "", false)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(st, e, Options{PollInterval: 50 * time.Millisecond, CheckpointInterval: 100 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		s.Close(ctx)
	}()
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Import(model.ImportRequest{ID: "real", ManifestDir: lists, Source: source, Destination: destination}); err != nil {
		t.Fatal(err)
	}
	b := waitBatch(t, s, "real", "ready")
	id := b.Manifests[0].TaskID
	var task model.Task
	until(t, 15*time.Second, func() bool { task, _ = s.Task(id); return !task.State.Active() && task.State != model.Queued })
	if task.State != model.Succeeded || task.Counts.CopiedObjects != int64(len(keys)-1) || task.Counts.SkippedExistingObjects != 1 {
		for _, a := range task.Attempts {
			t.Log(readFile(t, st.Path("tasks", id, "attempts", a.ID, "stderr.log")))
			t.Log(readFile(t, st.Path("tasks", id, "attempts", a.ID, "rclone.jsonl")))
		}
		t.Fatalf("状态或计数错误: %+v", task)
	}
	if got := readFile(t, filepath.Join(destination, keys[0])); got != "keep existing" {
		t.Fatal("覆盖了已有对象")
	}
	for _, key := range keys[1:] {
		if got := readFile(t, filepath.Join(destination, key)); got != "new data "+key {
			t.Fatalf("对象内容错误 %q", key)
		}
	}
}
