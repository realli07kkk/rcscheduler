// Package service 负责批次提交、任务状态及唯一调度决策。
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/ledger"
	"github.com/realli07kkk/rcscheduler/internal/manifest"
	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

type Error struct {
	Status        int
	Code, Message string
}

func (e *Error) Error() string                       { return e.Message }
func problem(status int, code, message string) error { return &Error{status, code, message} }

type Options struct {
	PollInterval       time.Duration
	CheckpointInterval time.Duration
	RetryDelays        []time.Duration
	Logger             *slog.Logger
}

type execution struct {
	process        runner.Process
	ledger         *ledger.Ledger
	attemptID      string
	stopReason     string
	lastCheckpoint time.Time
}

type Service struct {
	mu         sync.Mutex
	Store      *store.Store
	engine     runner.Engine
	lock       *store.Lock
	settings   model.Settings
	batches    map[string]model.Batch
	tasks      map[string]model.Task
	running    map[string]*execution
	reserved   map[string]bool
	ctx        context.Context
	cancel     context.CancelFunc
	wake       chan struct{}
	importGate chan struct{}
	wg         sync.WaitGroup
	options    Options
	fault      error
	stopping   bool
	started    bool
	loadIssues []string
}

func New(s *store.Store, engine runner.Engine, opt Options) (*Service, error) {
	lock, err := store.Acquire(s.Path("controller.lock"))
	if err != nil {
		return nil, err
	}
	if opt.PollInterval <= 0 {
		opt.PollInterval = 2 * time.Second
	}
	if opt.CheckpointInterval <= 0 {
		opt.CheckpointInterval = 10 * time.Second
	}
	if len(opt.RetryDelays) == 0 {
		opt.RetryDelays = []time.Duration{30 * time.Second, 120 * time.Second}
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	svc := &Service{Store: s, engine: engine, lock: lock, settings: model.DefaultSettings(), batches: map[string]model.Batch{}, tasks: map[string]model.Task{}, running: map[string]*execution{}, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), importGate: make(chan struct{}, 1), options: opt}
	svc.reserved = map[string]bool{}
	if err := svc.load(); err != nil {
		cancel()
		lock.Close()
		return nil, err
	}
	return svc, nil
}

func (s *Service) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) writableLocked() error {
	if s.stopping {
		return problem(503, "stopping", "服务正在停止")
	}
	if s.fault != nil {
		return problem(503, "storage_unavailable", s.fault.Error())
	}
	return nil
}

func (s *Service) storageFailureLocked(err error) error {
	if s.fault == nil {
		s.fault = err
		s.options.Logger.Error("持久化失败，停止派发并回收执行", "error", err)
		for _, active := range s.running {
			active.stopReason = "interrupted"
			if active.process != nil {
				p := active.process
				go func() { _ = p.Stop(context.Background()) }()
			}
		}
	}
	return problem(503, "storage_unavailable", err.Error())
}

func (s *Service) PersistenceFailure(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storageFailureLocked(err)
}

func cloneTask(t model.Task) model.Task { t.Attempts = slices.Clone(t.Attempts); return t }
func cloneBatch(b model.Batch) model.Batch {
	b.Manifests = slices.Clone(b.Manifests)
	if b.Summary.States != nil {
		m := make(map[model.TaskState]int, len(b.Summary.States))
		for k, v := range b.Summary.States {
			m[k] = v
		}
		b.Summary.States = m
	}
	return b
}

func (s *Service) saveTaskLocked(t model.Task) error {
	t.Revision = max(t.Revision, s.tasks[t.ID].Revision) + 1
	t.UpdatedAt = time.Now().UTC()
	if err := s.Store.Write(s.taskPath(t.ID, "task.json"), t); err != nil {
		return s.storageFailureLocked(err)
	}
	s.tasks[t.ID] = t
	return nil
}

func (s *Service) saveBatchLocked(b model.Batch) error {
	b.Revision = max(b.Revision, s.batches[b.ID].Revision) + 1
	if err := s.Store.Write(s.Store.Path("batches", b.ID, "batch.json"), b); err != nil {
		return s.storageFailureLocked(err)
	}
	s.batches[b.ID] = b
	return nil
}

func (s *Service) taskPath(id string, parts ...string) string {
	return s.Store.Path(append([]string{"tasks", id}, parts...)...)
}
func (s *Service) lockPath(id string) string { return s.Store.Path("runtime", id+".lock") }
func (s *Service) attemptDir(task, attempt string) string {
	return s.taskPath(task, "attempts", attempt)
}

func (s *Service) Settings() model.Settings { s.mu.Lock(); defer s.mu.Unlock(); return s.settings }

func (s *Service) UpdateSettings(p model.SettingsPatch) (model.Settings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return model.Settings{}, err
	}
	v := s.settings
	if p.Revision != nil && *p.Revision != v.Revision {
		return v, problem(409, "revision_conflict", "调度配置已被其他请求修改")
	}
	if p.MaxRunning != nil {
		v.MaxRunning = *p.MaxRunning
	}
	if p.BandwidthBudget != nil {
		v.BandwidthBudget = *p.BandwidthBudget
	}
	if p.Transfers != nil {
		v.Transfers = *p.Transfers
	}
	if p.Checkers != nil {
		v.Checkers = *p.Checkers
	}
	if p.UserAgent != nil {
		v.UserAgent = *p.UserAgent
	}
	if p.S3UploadConcurrency != nil {
		v.S3UploadConcurrency = *p.S3UploadConcurrency
	}
	if p.Paused != nil {
		v.Paused = *p.Paused
	}
	if err := v.Validate(); err != nil {
		return v, problem(400, "invalid_settings", err.Error())
	}
	v.Revision++
	if err := s.Store.Write(s.Store.Path("scheduler.json"), v); err != nil {
		return v, s.storageFailureLocked(err)
	}
	s.settings = v
	s.notify()
	return v, nil
}

func (s *Service) visibleTaskLocked(t model.Task) model.Task {
	t = cloneTask(t)
	if ex := s.running[t.ID]; ex != nil {
		t.Counts = ex.ledger.Checkpoint.Counts
		if len(t.Attempts) > 0 {
			i := len(t.Attempts) - 1
			t.Attempts[i].Counts = t.Counts
			t.Attempts[i].Progress = ex.ledger.Checkpoint.Progress
		}
	}
	return t
}

func (s *Service) Task(id string) (model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return t, problem(404, "task_not_found", "任务不存在")
	}
	return s.visibleTaskLocked(t), nil
}

func (s *Service) Tasks(batch, state string, offset, limit int) ([]model.Task, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []model.Task
	for _, t := range s.tasks {
		if batch != "" && t.BatchID != batch || state != "" && string(t.State) != state {
			continue
		}
		tasks = append(tasks, s.visibleTaskLocked(t))
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].Name < tasks[j].Name
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
	})
	total := len(tasks)
	if offset >= total {
		return []model.Task{}, total
	}
	return tasks[offset:min(total, offset+limit)], total
}

func (s *Service) batchLocked(id string) (model.Batch, error) {
	b, ok := s.batches[id]
	if !ok {
		return b, problem(404, "batch_not_found", "批次不存在")
	}
	b = cloneBatch(b)
	b.Summary = model.BatchSummary{States: map[model.TaskState]int{}, UpdatedAt: time.Now().UTC()}
	for _, m := range b.Manifests {
		if t, ok := s.tasks[m.TaskID]; ok && t.Generation == b.Generation {
			t = s.visibleTaskLocked(t)
			b.Summary.Tasks++
			b.Summary.States[t.State]++
			b.Summary.Objects.Add(t.Counts)
		}
	}
	return b, nil
}

func (s *Service) Batch(id string) (model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchLocked(id)
}

func (s *Service) Validation(id string) (model.ValidationReport, error) {
	s.mu.Lock()
	_, ok := s.batches[id]
	s.mu.Unlock()
	if !ok {
		return model.ValidationReport{}, problem(404, "batch_not_found", "批次不存在")
	}
	var report model.ValidationReport
	if err := store.Read(s.Store.Path("batches", id, "validation.json"), &report); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.ValidationReport{BatchID: id, Issues: []model.ValidationIssue{}}, nil
		}
		return report, err
	}
	return report, nil
}

func (s *Service) loadLedgerLocked(t model.Task) (*ledger.Ledger, error) {
	if len(t.Attempts) > 0 {
		if _, err := os.Stat(s.taskPath(t.ID, "objects.json")); err != nil {
			return nil, fmt.Errorf("已有执行的对象检查点缺失或不可读: %w", err)
		}
	}
	keys, err := manifest.Load(s.taskPath(t.ID, "manifest.txt"), t.Manifest.SHA256)
	if err != nil {
		return nil, err
	}
	if int64(len(keys)) != t.Manifest.Objects {
		return nil, fmt.Errorf("任务中的对象数与清单不一致")
	}
	return ledger.Load(s.taskPath(t.ID, "objects.json"), keys, t.Manifest.SHA256)
}

func (s *Service) Objects(id, state string, offset, limit int) ([]model.ObjectView, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, 0, problem(404, "task_not_found", "任务不存在")
	}
	var l *ledger.Ledger
	if ex := s.running[id]; ex != nil {
		l = ex.ledger
	} else {
		var err error
		l, err = s.loadLedgerLocked(t)
		if err != nil {
			return nil, 0, err
		}
	}
	items, total := l.Objects(state, offset, limit)
	return items, total, nil
}

func (s *Service) Action(id, action string) (model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return model.Task{}, err
	}
	t, ok := s.tasks[id]
	if !ok {
		return t, problem(404, "task_not_found", "任务不存在")
	}
	t = cloneTask(t)
	if t.State == model.Blocked {
		return t, problem(409, "blocked", "任务文档或旧执行需要先修复")
	}
	switch action {
	case "pause":
		if t.State == model.Paused || t.State == model.Pausing {
			return s.visibleTaskLocked(t), nil
		}
		if t.State != model.Queued && t.State != model.RetryWait && !t.State.Active() {
			return t, problem(409, "invalid_state", "此状态不能暂停")
		}
		t.DesiredState = "pause"
		t.NextRunAt = nil
		if t.State.Active() {
			t.State = model.Pausing
		} else {
			t.State = model.Paused
		}
	case "cancel":
		if t.State == model.Cancelled || t.State == model.Cancelling {
			return s.visibleTaskLocked(t), nil
		}
		if t.State == model.Succeeded {
			return t, problem(409, "invalid_state", "已成功任务不能取消")
		}
		t.DesiredState = "cancel"
		t.NextRunAt = nil
		if t.State.Active() {
			t.State = model.Cancelling
		} else {
			t.State = model.Cancelled
		}
	case "resume":
		if t.State != model.Paused {
			return t, problem(409, "invalid_state", "只有暂停任务可以恢复")
		}
		t.State = model.Queued
		t.DesiredState = "run"
		t.QueuedAt = time.Now().UTC()
		t.NextReason = "resume"
	case "retry":
		if t.State != model.Failed && t.State != model.Cancelled && t.State != model.NeedsReview {
			return t, problem(409, "invalid_state", "只有失败、取消或待核对任务可以重试")
		}
		t.State = model.Queued
		t.DesiredState = "run"
		t.QueuedAt = time.Now().UTC()
		t.NextReason = "manual_retry"
		t.FailuresInCycle = 0
		t.NextRunAt = nil
		t.Error = ""
	default:
		return t, problem(400, "invalid_action", "未知任务操作")
	}
	if err := s.saveTaskLocked(t); err != nil {
		return t, err
	}
	if ex := s.running[id]; ex != nil && (action == "pause" || action == "cancel") {
		ex.stopReason = action
		if ex.process != nil {
			p := ex.process
			go func() {
				if err := p.Stop(context.Background()); err != nil {
					s.options.Logger.Error("停止任务失败", "task", id, "error", err)
				}
			}()
		}
	}
	s.notify()
	return s.visibleTaskLocked(s.tasks[id]), nil
}

func (s *Service) Priority(id string, priority int) (model.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return model.Task{}, err
	}
	t, ok := s.tasks[id]
	if !ok {
		return t, problem(404, "task_not_found", "任务不存在")
	}
	if t.State == model.Blocked {
		return t, problem(409, "blocked", "任务文档或旧执行需要先修复")
	}
	t = cloneTask(t)
	t.Priority = priority
	if err := s.saveTaskLocked(t); err != nil {
		return t, err
	}
	s.notify()
	return s.visibleTaskLocked(s.tasks[id]), nil
}

func (s *Service) Health() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	health := map[string]any{"ready": s.fault == nil && !s.stopping, "running": len(s.running), "tasks": len(s.tasks), "loadIssues": slices.Clone(s.loadIssues), "rcloneVersion": s.engine.Version()}
	if s.fault != nil {
		health["error"] = s.fault.Error()
	}
	return health
}

// BeginShutdown 先固定中断意图，避免正常停服被记录为传输失败。
func (s *Service) BeginShutdown() {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return
	}
	s.stopping = true
	s.cancel()
	var processes []runner.Process
	for _, ex := range s.running {
		ex.stopReason = "interrupted"
		if ex.process != nil {
			processes = append(processes, ex.process)
		}
	}
	s.mu.Unlock()
	for _, p := range processes {
		go func(p runner.Process) { _ = p.Stop(context.Background()) }(p)
	}
}

func (s *Service) Close(ctx context.Context) error {
	s.BeginShutdown()
	s.mu.Lock()
	s.stopping = true
	s.cancel()
	var processes []runner.Process
	for _, ex := range s.running {
		ex.stopReason = "interrupted"
		if ex.process != nil {
			processes = append(processes, ex.process)
		}
	}
	s.mu.Unlock()
	var stops sync.WaitGroup
	for _, p := range processes {
		stops.Add(1)
		go func(p runner.Process) { defer stops.Done(); _ = p.Stop(ctx) }(p)
	}
	stops.Wait()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return s.lock.Close()
	case <-ctx.Done():
		return ctx.Err() // 仍有受控工作时保留锁，调用方退出进程后由 OS 回收。
	}
}

func (s *Service) removeSocket(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

func normalizeFS(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("源和目标不能为空")
	}
	if stringsContainsNUL(path) {
		return "", fmt.Errorf("源和目标不能包含 NUL")
	}
	for _, r := range path {
		if r == ':' {
			return path, nil
		}
	}
	return filepath.Abs(path)
}
func stringsContainsNUL(s string) bool {
	for _, r := range s {
		if r == 0 {
			return true
		}
	}
	return false
}
