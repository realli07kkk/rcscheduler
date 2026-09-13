package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
)

type runtimeInfo struct {
	TaskID    string `json:"taskId"`
	AttemptID string `json:"attemptId"`
	Socket    string `json:"socket"`
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("服务已启动")
	}
	s.started = true
	s.mu.Unlock()
	if err := s.recoverExecutions(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	for _, b := range s.batches {
		if b.ImportState == "validating" && b.SnapshotsComplete {
			s.wg.Add(1)
			go func(b model.Batch) { defer s.wg.Done(); s.importWorker(b, true) }(cloneBatch(b))
		}
	}
	s.wg.Add(1)
	s.mu.Unlock()
	go s.loop()
	s.notify()
	return nil
}

func (s *Service) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		case <-ticker.C:
		}
		for {
			s.mu.Lock()
			if s.stopping || s.fault != nil || s.settings.Paused || len(s.running) >= s.settings.MaxRunning {
				s.mu.Unlock()
				break
			}
			var selected *model.Task
			now := time.Now()
			for _, t := range s.tasks {
				if t.State != model.Queued && t.State != model.RetryWait {
					continue
				}
				if t.DesiredState != "run" || (t.NextRunAt != nil && now.Before(*t.NextRunAt)) {
					continue
				}
				b, ok := s.batches[t.BatchID]
				if !ok || b.ImportState != "ready" || b.Generation != t.Generation {
					continue
				}
				if selected == nil || before(t, *selected) {
					copy := cloneTask(t)
					selected = &copy
				}
			}
			if selected == nil {
				s.mu.Unlock()
				break
			}
			spec, ex, err := s.prepareLocked(*selected)
			if err != nil {
				if s.fault == nil {
					t := cloneTask(*selected)
					t.State = model.Blocked
					t.Error = err.Error()
					_ = s.saveTaskLocked(t)
				}
				s.mu.Unlock()
				continue
			}
			s.wg.Add(1)
			s.mu.Unlock()
			go s.execute(spec, ex)
		}
	}
}

func before(a, b model.Task) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.QueuedAt.Equal(b.QueuedAt) {
		return a.QueuedAt.Before(b.QueuedAt)
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	return a.ID < b.ID
}

func (s *Service) prepareLocked(t model.Task) (runner.Launch, *execution, error) {
	l, err := s.loadLedgerLocked(t)
	if err != nil {
		return runner.Launch{}, nil, err
	}
	budget, _ := model.BandwidthBytes(s.settings.BandwidthBudget)
	id := model.NewID()
	now := time.Now().UTC()
	reason := t.NextReason
	if reason == "" {
		reason = "initial"
	}
	a := model.Attempt{ID: id, Reason: reason, SettingsRevision: s.settings.Revision, BandwidthBudget: s.settings.BandwidthBudget, MaxRunning: s.settings.MaxRunning, BandwidthBytesPerSecond: budget / int64(s.settings.MaxRunning), Transfers: s.settings.Transfers, Checkers: s.settings.Checkers, RcloneVersion: s.engine.Version(), Socket: s.engine.Socket(id), StartedAt: now}
	l.Begin(id)
	if err := l.Save(s.Store, s.taskPath(t.ID, "objects.json")); err != nil {
		return runner.Launch{}, nil, s.storageFailureLocked(err)
	}
	t.Attempts = append(t.Attempts, a)
	t.CurrentAttempt = id
	t.State = model.Starting
	t.Counts = l.Checkpoint.Counts
	t.NextRunAt = nil
	t.Error = ""
	if err := s.saveTaskLocked(t); err != nil {
		return runner.Launch{}, nil, err
	}
	if err := s.Store.Write(s.Store.Path("runtime", t.ID+".json"), runtimeInfo{TaskID: t.ID, AttemptID: id, Socket: a.Socket}); err != nil {
		return runner.Launch{}, nil, s.storageFailureLocked(err)
	}
	ex := &execution{ledger: l, attemptID: id, lastCheckpoint: now}
	s.running[t.ID] = ex
	return runner.Launch{Task: t, Attempt: a, ManifestPath: s.taskPath(t.ID, "manifest.txt"), AttemptDir: s.attemptDir(t.ID, id), LockPath: s.lockPath(t.ID)}, ex, nil
}

func (s *Service) execute(spec runner.Launch, ex *execution) {
	defer s.wg.Done()
	s.mu.Lock()
	abort := s.stopping || s.fault != nil || ex.stopReason != ""
	s.mu.Unlock()
	if abort {
		s.finish(spec, ex, runner.Result{ExitCode: -1, Error: "启动前已停止"})
		return
	}
	p, err := s.engine.Start(spec)
	if err != nil {
		s.finish(spec, ex, runner.Result{ExitCode: -1, Error: err.Error()})
		return
	}
	s.mu.Lock()
	ex.process = p
	t := cloneTask(s.tasks[spec.Task.ID])
	i := len(t.Attempts) - 1
	t.Attempts[i].PID = p.PID()
	if ex.stopReason == "" && !s.stopping && s.fault == nil {
		t.State = model.Running
	}
	if s.fault == nil {
		_ = s.saveTaskLocked(t)
	}
	shouldStop := ex.stopReason != "" || s.stopping || s.fault != nil
	s.mu.Unlock()
	if shouldStop {
		go func() { _ = p.Stop(context.Background()) }()
	}
	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.Done():
			s.finish(spec, ex, p.Result())
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), min(s.options.PollInterval, 2*time.Second))
			stats, statsErr := p.Stats(ctx)
			cancel()
			s.mu.Lock()
			if s.fault != nil {
				s.mu.Unlock()
				continue
			}
			if statsErr == nil {
				ex.ledger.Sample(stats.Progress(ex.attemptID, time.Now().UTC()))
			}
			if err := ex.ledger.Consume(spec.AttemptDir, ex.attemptID, false); err != nil {
				if _, ok := err.(*os.PathError); ok {
					s.storageFailureLocked(err)
				} else {
					ex.stopReason = "evidence_error"
					go func() { _ = p.Stop(context.Background()) }()
				}
			}
			if time.Since(ex.lastCheckpoint) >= s.options.CheckpointInterval && s.fault == nil {
				if s.checkpointLocked(spec.Task.ID, ex) == nil {
					ex.lastCheckpoint = time.Now()
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *Service) checkpointLocked(id string, ex *execution) error {
	if err := ex.ledger.Save(s.Store, s.taskPath(id, "objects.json")); err != nil {
		return s.storageFailureLocked(err)
	}
	if err := s.Store.Write(s.taskPath(id, "progress.json"), ex.ledger.Checkpoint.Progress); err != nil {
		return s.storageFailureLocked(err)
	}
	t := cloneTask(s.tasks[id])
	t.Counts = ex.ledger.Checkpoint.Counts
	if len(t.Attempts) > 0 {
		i := len(t.Attempts) - 1
		t.Attempts[i].Counts = t.Counts
		t.Attempts[i].Progress = ex.ledger.Checkpoint.Progress
	}
	if err := s.saveTaskLocked(t); err != nil {
		return err
	}
	b, err := s.batchLocked(t.BatchID)
	if err != nil {
		return err
	}
	return s.saveBatchLocked(b)
}

func (s *Service) finish(spec runner.Launch, ex *execution, result runner.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() { delete(s.running, spec.Task.ID); s.removeSocket(spec.Attempt.Socket); s.notify() }()
	if s.fault != nil {
		return
	}
	err := ex.ledger.Consume(spec.AttemptDir, ex.attemptID, true)
	if err != nil {
		ex.ledger.Checkpoint.IntegrityError = err.Error()
	}
	ex.ledger.Finish()
	if err := ex.ledger.Save(s.Store, s.taskPath(spec.Task.ID, "objects.json")); err != nil {
		s.storageFailureLocked(err)
		return
	}
	if err := s.Store.Write(s.taskPath(spec.Task.ID, "progress.json"), ex.ledger.Checkpoint.Progress); err != nil {
		s.storageFailureLocked(err)
		return
	}
	t := cloneTask(s.tasks[spec.Task.ID])
	now := time.Now().UTC()
	i := len(t.Attempts) - 1
	a := &t.Attempts[i]
	a.EndedAt = &now
	a.ExitCode = &result.ExitCode
	if ex.ledger.Checkpoint.LastError != "" && result.ExitCode != 0 {
		result.Error = ex.ledger.Checkpoint.LastError
	}
	a.Error = result.Error
	a.Counts = ex.ledger.Checkpoint.Counts
	a.Progress = ex.ledger.Checkpoint.Progress
	t.Counts = a.Counts
	t.CurrentAttempt = ""
	t.Error = result.Error
	complete := t.Counts.CompletedObjects == t.Counts.TotalObjects && ex.ledger.Checkpoint.IntegrityError == ""
	switch {
	case result.ExitCode == 0 && complete:
		t.State = model.Succeeded
		t.Error = ""
		a.Error = ""
		a.Outcome = "succeeded"
	case ex.stopReason == "pause" || t.DesiredState == "pause":
		t.State = model.Paused
		a.Outcome = "paused"
	case ex.stopReason == "cancel" || t.DesiredState == "cancel":
		t.State = model.Cancelled
		a.Outcome = "cancelled"
	case s.stopping || ex.stopReason == "interrupted":
		t.State = model.Queued
		t.NextReason = "recovery"
		a.Outcome = "interrupted"
	case ex.stopReason == "evidence_error" || ex.ledger.Checkpoint.IntegrityError != "" || result.ExitCode == 0:
		t.State = model.NeedsReview
		a.Outcome = "needs_review"
		t.Error = ex.ledger.Checkpoint.IntegrityError
		if t.Error == "" {
			t.Error = "rclone 已退出，但清单对象结果不完整"
		}
		a.Error = t.Error
	default:
		a.Outcome = "failed"
		t.FailuresInCycle++
		if result.ExitCode == 5 && t.FailuresInCycle < 3 {
			t.State = model.RetryWait
			delay := s.options.RetryDelays[min(t.FailuresInCycle-1, len(s.options.RetryDelays)-1)]
			next := now.Add(delay)
			t.NextRunAt = &next
			t.NextReason = "automatic_retry"
		} else {
			t.State = model.Failed
		}
	}
	if err := s.saveTaskLocked(t); err != nil {
		return
	}
	// 汇总必须使用刚保存的终态，不能再混入 running 内旧对象引用。
	delete(s.running, t.ID)
	if b, err := s.batchLocked(t.BatchID); err == nil {
		_ = s.saveBatchLocked(b)
	}
	s.options.Logger.Info("任务执行结束", "task", t.ID, "attempt", a.ID, "state", t.State, "copied", t.Counts.CopiedObjects, "skipped", t.Counts.SkippedExistingObjects, "failed", t.Counts.FailedObjects, "unknown", t.Counts.UnknownObjects)
}

func (s *Service) validSocketFile(path string) bool {
	return filepath.Ext(path) == ".sock" && filepath.Base(path) != ".sock"
}
