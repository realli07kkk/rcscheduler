package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func (s *Service) load() error {
	for _, dir := range []string{"batches", "tasks", "runtime", "staging", "uploads"} {
		if err := store.EnsureDir(s.Store.Path(dir)); err != nil {
			return err
		}
	}
	if err := store.Read(s.Store.Path("scheduler.json"), &s.settings); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := s.Store.Write(s.Store.Path("scheduler.json"), s.settings); err != nil {
			return err
		}
	}
	if err := s.settings.Validate(); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.Store.Path("batches"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !model.ValidID(entry.Name()) {
			continue
		}
		var b model.Batch
		err := store.Read(s.Store.Path("batches", entry.Name(), "batch.json"), &b)
		if err == nil {
			err = validateBatch(b, entry.Name())
		}
		if err != nil {
			s.loadIssues = append(s.loadIssues, err.Error())
			s.batches[entry.Name()] = model.Batch{ID: entry.Name(), ImportState: "blocked", Error: err.Error()}
			continue
		}
		if b.ImportState == "validating" && !b.SnapshotsComplete {
			b.ImportState = "rejected"
			b.Error = "导入中断且快照不完整，请重新导入"
			report := model.ValidationReport{BatchID: b.ID, Generation: b.Generation, CheckedFiles: b.CheckedFiles, Issues: []model.ValidationIssue{{Code: "import_interrupted", Message: b.Error}}, FinishedAt: time.Now().UTC()}
			report.IssueCount = 1
			if err := s.Store.Write(s.Store.Path("batches", b.ID, "validation.json"), report); err != nil {
				return err
			}
			b.Revision++
			if err := s.Store.Write(s.Store.Path("batches", b.ID, "batch.json"), b); err != nil {
				return err
			}
		}
		s.batches[b.ID] = b
		if b.ImportState != "ready" {
			continue
		}
		for _, m := range b.Manifests {
			var t model.Task
			err := store.Read(s.taskPath(m.TaskID, "task.json"), &t)
			if err == nil {
				err = validateTask(t, b, m)
			}
			if err != nil {
				s.loadIssues = append(s.loadIssues, err.Error())
				t = model.Task{ID: m.TaskID, BatchID: b.ID, Generation: b.Generation, Name: m.RelativePath, State: model.Blocked, Manifest: m, Error: err.Error(), Counts: model.Counts{TotalObjects: m.Objects, UnknownObjects: m.Objects}}
			}
			s.tasks[t.ID] = t
		}
	}
	return nil
}

func validateBatch(b model.Batch, id string) error {
	if b.ID != id || b.Generation < 1 {
		return fmt.Errorf("批次 %s 的 ID 或代次无效", id)
	}
	switch b.ImportState {
	case "ready", "rejected", "validating":
	default:
		return fmt.Errorf("批次 %s 的状态无效", id)
	}
	if b.ImportState == "rejected" {
		return nil
	}
	seen := map[string]bool{}
	for _, m := range b.Manifests {
		if !model.ValidID(m.TaskID) || seen[m.TaskID] || m.Objects < 1 || len(m.SHA256) != 64 {
			return fmt.Errorf("批次 %s 的清单元数据无效", id)
		}
		seen[m.TaskID] = true
	}
	if b.ImportState == "ready" && (!b.SnapshotsComplete || len(b.Manifests) == 0) {
		return fmt.Errorf("批次 %s 缺少完整提交信息", id)
	}
	return nil
}

func validateTask(t model.Task, b model.Batch, m model.Manifest) error {
	if t.ID != m.TaskID || t.BatchID != b.ID || t.Generation != b.Generation || t.Manifest != m || !t.State.Valid() {
		return fmt.Errorf("任务 %s 与批次或状态不一致", m.TaskID)
	}
	if t.Source != b.Request.Source || t.Destination != b.Request.Destination {
		return fmt.Errorf("任务 %s 的迁移位置与批次不一致", t.ID)
	}
	if t.DesiredState != "run" && t.DesiredState != "pause" && t.DesiredState != "cancel" {
		return fmt.Errorf("任务 %s 的运行意图无效", t.ID)
	}
	for _, a := range t.Attempts {
		if !model.ValidID(a.ID) {
			return fmt.Errorf("任务 %s 的执行编号无效", t.ID)
		}
	}
	if t.State.Active() && (len(t.Attempts) == 0 || t.CurrentAttempt != t.Attempts[len(t.Attempts)-1].ID) {
		return fmt.Errorf("任务 %s 缺少当前执行记录", t.ID)
	}
	return nil
}

func (s *Service) recoverExecutions(ctx context.Context) error {
	entries, err := os.ReadDir(s.Store.Path("runtime"))
	if err != nil {
		return err
	}
	// 独立运行描述符覆盖任务文档损坏以及 fork 后 PID 尚未写回的窗口。
	blocked := map[string]string{}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var info runtimeInfo
		if err := store.Read(s.Store.Path("runtime", entry.Name()), &info); err != nil {
			return fmt.Errorf("无法安全恢复运行描述符: %w", err)
		}
		if !model.ValidID(info.TaskID) || !model.ValidID(info.AttemptID) || info.TaskID+".json" != entry.Name() || !s.validSocketFile(info.Socket) {
			return fmt.Errorf("运行描述符无效: %s", entry.Name())
		}
		if err := s.engine.Recover(ctx, s.lockPath(info.TaskID), info.Socket); err != nil {
			blocked[info.TaskID] = err.Error()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, original := range s.tasks {
		t := cloneTask(original)
		if message, ok := blocked[id]; ok {
			t.State = model.Blocked
			t.Error = message
			if err := s.saveTaskLocked(t); err != nil {
				return err
			}
			continue
		}
		if t.State == model.Blocked {
			continue
		}
		if len(t.Attempts) == 0 {
			continue
		}
		i := len(t.Attempts) - 1
		if t.Attempts[i].EndedAt != nil {
			continue
		}
		a := &t.Attempts[i]
		l, err := s.loadLedgerLocked(t)
		if err != nil {
			t.State = model.Blocked
			t.Error = err.Error()
			if err := s.saveTaskLocked(t); err != nil {
				return err
			}
			continue
		}
		if err := l.Consume(s.attemptDir(id, a.ID), a.ID, true); err != nil {
			l.Checkpoint.IntegrityError = err.Error()
		}
		l.Finish()
		if err := l.Save(s.Store, s.taskPath(id, "objects.json")); err != nil {
			return s.storageFailureLocked(err)
		}
		now := time.Now().UTC()
		a.EndedAt = &now
		a.Outcome = "interrupted"
		a.Error = "服务重启，旧执行已回收，最终退出结果未知"
		a.Counts = l.Checkpoint.Counts
		a.Progress = l.Checkpoint.Progress
		t.CurrentAttempt = ""
		t.Counts = a.Counts
		t.NextReason = "recovery"
		t.NextRunAt = nil
		t.Error = ""
		switch t.DesiredState {
		case "pause":
			t.State = model.Paused
		case "cancel":
			t.State = model.Cancelled
		default:
			t.State = model.Queued
		}
		if err := s.saveTaskLocked(t); err != nil {
			return err
		}
	}
	for id, message := range blocked {
		if _, ok := s.tasks[id]; !ok {
			return fmt.Errorf("未知任务 %s 的旧进程尚未退出: %s", id, message)
		}
	}
	return nil
}

func (s *Service) DataDir() string { return filepath.Clean(s.Store.Root) }
