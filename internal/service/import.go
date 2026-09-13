package service

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/manifest"
	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func taskID(batch, relative string) string {
	h := sha256.Sum256([]byte(batch + "\x00" + relative))
	return "t-" + hex.EncodeToString(h[:12])
}

// 保留所有文件的错误计数，并限制内存中的详细条目数量。
func addIssues(report *model.ValidationReport, issues ...model.ValidationIssue) {
	if report.IssuesByFile == nil {
		report.IssuesByFile = map[string]int64{}
	}
	for _, issue := range issues {
		report.IssueCount++
		report.IssuesByFile[issue.File]++
		if len(report.Issues) < 2000 {
			report.Issues = append(report.Issues, issue)
		} else {
			report.DetailsTruncated = true
		}
	}
}

func (s *Service) stagePath(b model.Batch, id string) string {
	return s.Store.Path("staging", b.ID, strconv.Itoa(b.Generation), id+".txt")
}

func (s *Service) Import(req model.ImportRequest) (model.Batch, error) {
	if !model.ValidID(req.ID) {
		return model.Batch{}, problem(400, "invalid_id", "批次 ID 必须是 1..64 位字母、数字、下划线或短横线")
	}
	var err error
	if req.ManifestDir == "" {
		return model.Batch{}, problem(400, "invalid_directory", "manifestDir 不能为空")
	}
	req.ManifestDir, err = filepath.Abs(req.ManifestDir)
	if err != nil {
		return model.Batch{}, err
	}
	req.Source, err = normalizeFS(req.Source)
	if err != nil {
		return model.Batch{}, problem(400, "invalid_source", err.Error())
	}
	req.Destination, err = normalizeFS(req.Destination)
	if err != nil {
		return model.Batch{}, problem(400, "invalid_destination", err.Error())
	}
	if req.Source == req.Destination {
		return model.Batch{}, problem(400, "same_remote", "源与目标不能相同")
	}
	if req.Pattern == "" {
		req.Pattern = "*.txt"
	}
	if _, err := filepath.Match(req.Pattern, "x"); err != nil {
		return model.Batch{}, problem(400, "invalid_pattern", err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return model.Batch{}, err
	}
	b, exists := s.batches[req.ID]
	if exists && (b.ImportState == "ready" || b.ImportState == "validating") {
		if b.Request != req {
			return b, problem(409, "batch_conflict", "同 ID 批次的导入参数不同；已提交批次不可覆盖")
		}
		return cloneBatch(b), nil
	}
	if exists && b.ImportState == "blocked" {
		return b, problem(409, "batch_blocked", "批次文档损坏，不能覆盖")
	}
	generation := 1
	if exists {
		generation = b.Generation + 1
	}
	b = model.Batch{ID: req.ID, Revision: b.Revision, Request: req, Generation: generation, ImportState: "validating", CreatedAt: time.Now().UTC(), Manifests: []model.Manifest{}}
	if err := s.saveBatchLocked(b); err != nil {
		return b, err
	}
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.importWorker(b, false) }()
	return cloneBatch(s.batches[b.ID]), nil
}

func (s *Service) importWorker(b model.Batch, recovery bool) {
	select {
	case s.importGate <- struct{}{}:
		defer func() { <-s.importGate }()
	case <-s.ctx.Done():
		return
	}
	report := model.ValidationReport{BatchID: b.ID, Generation: b.Generation, Issues: []model.ValidationIssue{}}
	if recovery {
		for _, m := range b.Manifests {
			path := s.stagePath(b, m.TaskID)
			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				path = s.taskPath(m.TaskID, "manifest.txt")
			}
			keys, err := manifest.Load(path, m.SHA256)
			if err != nil || int64(len(keys)) != m.Objects {
				addIssues(&report, model.ValidationIssue{File: m.RelativePath, Code: "snapshot_invalid", Message: fmt.Sprintf("内部快照无法恢复: %v", err)})
			}
			report.CheckedFiles++
			report.TotalObjects += m.Objects
		}
	} else {
		paths, err := manifest.Discover(b.Request.ManifestDir, b.Request.Pattern, b.Request.Recursive)
		if err != nil {
			addIssues(&report, model.ValidationIssue{Code: "discovery", Message: err.Error()})
		} else {
			b.DiscoveredFiles = len(paths)
			if !s.importProgress(b) {
				return
			}
			for _, relative := range paths {
				select {
				case <-s.ctx.Done():
					return
				default:
				}
				id := taskID(b.ID, relative)
				m, issues, err := manifest.Snapshot(s.Store, filepath.Join(b.Request.ManifestDir, filepath.FromSlash(relative)), s.stagePath(b, id), relative)
				m.TaskID = id
				if err != nil {
					var publication *store.WriteError
					var input *manifest.InputError
					if errors.As(err, &publication) && !errors.As(err, &input) {
						s.mu.Lock()
						s.storageFailureLocked(err)
						s.mu.Unlock()
						return
					}
					addIssues(&report, model.ValidationIssue{File: relative, Code: "snapshot_failed", Message: err.Error()})
				} else {
					addIssues(&report, issues...)
					b.Manifests = append(b.Manifests, m)
					report.TotalObjects += m.Objects
				}
				report.CheckedFiles++
				b.CheckedFiles = report.CheckedFiles
				// 每份清单处理完即保存导入进度；所有清单仍由提交标记统一放行。
				if !s.importProgress(b) {
					return
				}
			}
		}
	}
	report.FinishedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.fault != nil {
		return
	}
	if err := s.Store.Write(s.Store.Path("batches", b.ID, "validation.json"), report); err != nil {
		s.storageFailureLocked(err)
		return
	}
	if len(report.Issues) != 0 {
		b.ImportState = "rejected"
		b.Error = fmt.Sprintf("%d 项校验错误", report.IssueCount)
		b.CheckedFiles = report.CheckedFiles
		_ = s.saveBatchLocked(b)
		return
	}
	b.SnapshotsComplete = true
	b.CheckedFiles = report.CheckedFiles
	if err := s.saveBatchLocked(b); err != nil {
		return
	}
	if err := s.commitBatchLocked(b); err != nil {
		s.options.Logger.Error("批次提交失败", "batch", b.ID, "error", err)
	}
}

func (s *Service) importProgress(b model.Batch) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.fault != nil {
		return false
	}
	// 清单索引只在所有快照完成后发布一次，避免目录越大重写成本越高。
	b.Manifests = nil
	return s.saveBatchLocked(cloneBatch(b)) == nil
}

// commitBatchLocked 先发布所有任务，再发布唯一批次提交标记。
func (s *Service) commitBatchLocked(b model.Batch) error {
	now := time.Now().UTC()
	prepared := make([]model.Task, 0, len(b.Manifests))
	for _, m := range b.Manifests {
		if !model.ValidID(m.TaskID) {
			return s.storageFailureLocked(fmt.Errorf("无效快照任务 ID"))
		}
		if previous, ok := s.tasks[m.TaskID]; ok && previous.BatchID != b.ID {
			return problem(409, "task_conflict", "任务 ID 已存在")
		}
		if s.reserved[m.TaskID] {
			return problem(409, "task_busy", "任务 ID 正在由另一个提交使用")
		}
	}
	for _, m := range b.Manifests {
		s.reserved[m.TaskID] = true
	}
	defer func() {
		for _, m := range b.Manifests {
			delete(s.reserved, m.TaskID)
		}
	}()
	// 大量清单的文件发布不占用调度锁，已有任务仍能更新进度和处理控制请求。
	s.mu.Unlock()
	var publishErr error
	for _, m := range b.Manifests {
		if s.ctx.Err() != nil {
			publishErr = s.ctx.Err()
			break
		}
		source := s.stagePath(b, m.TaskID)
		if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
			source = s.taskPath(m.TaskID, "manifest.txt")
		}
		if source != s.taskPath(m.TaskID, "manifest.txt") {
			if err := s.Store.Copy(source, s.taskPath(m.TaskID, "manifest.txt")); err != nil {
				publishErr = err
				break
			}
		}
		t := model.Task{ID: m.TaskID, Revision: 1, BatchID: b.ID, Generation: b.Generation, Name: m.RelativePath, Source: b.Request.Source, Destination: b.Request.Destination, Manifest: m, State: model.Queued, DesiredState: "run", CreatedAt: now, QueuedAt: now, UpdatedAt: now, NextReason: "initial", Attempts: []model.Attempt{}, Counts: model.Counts{TotalObjects: m.Objects, PendingObjects: m.Objects}}
		if err := s.Store.Write(s.taskPath(t.ID, "task.json"), t); err != nil {
			publishErr = err
			break
		}
		prepared = append(prepared, t)
	}
	s.mu.Lock()
	if s.stopping {
		return problem(503, "stopping", "批次尚未提交，服务正在停止")
	}
	if publishErr != nil {
		return s.storageFailureLocked(publishErr)
	}
	if s.fault != nil {
		return problem(503, "storage_unavailable", s.fault.Error())
	}
	b.ImportState = "ready"
	b.Error = ""
	b.CommittedAt = &now
	b.Summary = model.BatchSummary{Tasks: len(prepared), States: map[model.TaskState]int{model.Queued: len(prepared)}, UpdatedAt: now}
	for _, t := range prepared {
		b.Summary.Objects.Add(t.Counts)
	}
	if err := s.saveBatchLocked(b); err != nil {
		return err
	}
	for _, t := range prepared {
		s.tasks[t.ID] = t
	}
	s.notify()
	return nil
}

// SubmitSingle 复用批次提交规则，上传的单份清单也拥有不可变快照。
func (s *Service) SubmitSingle(id, name, source, destination string, input io.Reader) (model.Task, error) {
	s.mu.Lock()
	if err := s.writableLocked(); err != nil {
		s.mu.Unlock()
		return model.Task{}, err
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	if !model.ValidID(id) {
		return model.Task{}, problem(400, "invalid_id", "无效任务 ID")
	}
	var err error
	source, err = normalizeFS(source)
	if err != nil {
		return model.Task{}, problem(400, "invalid_source", err.Error())
	}
	destination, err = normalizeFS(destination)
	if err != nil {
		return model.Task{}, problem(400, "invalid_destination", err.Error())
	}
	if source == destination {
		return model.Task{}, problem(400, "same_remote", "源与目标不能相同")
	}
	if name == "" {
		name = "manifest.txt"
	}
	h := sha256.Sum256([]byte("single:" + id))
	batchID := "single-" + hex.EncodeToString(h[:12])
	// 独立上传临时目录使并发幂等请求不会相互覆盖正在校验的字节。
	upload := s.Store.Path("uploads", model.NewID(), "manifest.txt")
	if err := s.Store.AtomicWrite(upload, func(w io.Writer) error { _, err := io.Copy(w, input); return err }); err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			return model.Task{}, s.PersistenceFailure(err)
		}
		return model.Task{}, err
	}
	defer os.RemoveAll(filepath.Dir(upload))
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writableLocked(); err != nil {
		return model.Task{}, err
	}
	if s.reserved[id] {
		return model.Task{}, problem(409, "task_busy", "同 ID 任务正在提交，请稍后重试")
	}
	b := model.Batch{ID: batchID, Generation: 1, ImportState: "validating", CreatedAt: time.Now().UTC(), Request: model.ImportRequest{ID: batchID, Source: source, Destination: destination}}
	m, issues, err := manifest.Snapshot(s.Store, upload, s.stagePath(b, id), name)
	if err != nil {
		var publication *store.WriteError
		var inputError *manifest.InputError
		if errors.As(err, &publication) && !errors.As(err, &inputError) {
			return model.Task{}, s.storageFailureLocked(err)
		}
		return model.Task{}, err
	}
	if len(issues) > 0 {
		return model.Task{}, problem(400, "invalid_manifest", fmt.Sprintf("第 %d 行: %s", issues[0].Line, issues[0].Message))
	}
	m.TaskID = id
	if old, ok := s.tasks[id]; ok {
		if old.Source == source && old.Destination == destination && old.Manifest.SHA256 == m.SHA256 {
			return s.visibleTaskLocked(old), nil
		}
		return model.Task{}, problem(409, "task_conflict", "同 ID 任务的源、目标或清单不同")
	}
	b.Manifests = []model.Manifest{m}
	b.DiscoveredFiles = 1
	b.CheckedFiles = 1
	b.SnapshotsComplete = true
	if err := s.saveBatchLocked(b); err != nil {
		return model.Task{}, err
	}
	report := model.ValidationReport{BatchID: batchID, Generation: 1, CheckedFiles: 1, TotalObjects: m.Objects, Issues: []model.ValidationIssue{}, FinishedAt: time.Now().UTC()}
	if err := s.Store.Write(s.Store.Path("batches", batchID, "validation.json"), report); err != nil {
		return model.Task{}, s.storageFailureLocked(err)
	}
	if err := s.commitBatchLocked(b); err != nil {
		return model.Task{}, err
	}
	return cloneTask(s.tasks[id]), nil
}
