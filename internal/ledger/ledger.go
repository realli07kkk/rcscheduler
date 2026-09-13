// Package ledger 从持久执行证据恢复按对象去重的结果。
package ledger

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

// 只接受已验证的最终复制记录，不将差异报告或任意包含 Copied 的日志当成成功。
var copiedMessage = regexp.MustCompile(`^(?:Multi-thread )?Copied \((?:Rcat, )?(?:new|replaced existing|server-side copy)\)(?: to: .*)?$`)

type Ledger struct {
	Checkpoint model.Checkpoint
	Keys       []string
	index      map[string]int
}

func New(keys []string, manifestHash string) *Ledger {
	l := &Ledger{Keys: keys, index: make(map[string]int, len(keys))}
	l.Checkpoint = model.Checkpoint{ManifestSHA256: manifestHash, States: make([]model.ObjectState, len(keys)), Errors: map[int]string{}, Cursors: map[string]int64{}}
	for i, key := range keys {
		l.index[key] = i
		l.Checkpoint.States[i] = model.ObjectPending
	}
	l.Recount()
	return l
}

func Load(path string, keys []string, manifestHash string) (*Ledger, error) {
	l := New(keys, manifestHash)
	var cp model.Checkpoint
	if err := store.Read(path, &cp); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return l, nil
		}
		return nil, err
	}
	if cp.ManifestSHA256 != manifestHash || len(cp.States) != len(keys) {
		return nil, fmt.Errorf("对象检查点与清单不匹配")
	}
	for _, state := range cp.States {
		switch state {
		case model.ObjectPending, model.ObjectActive, model.ObjectCopied, model.ObjectSkipped, model.ObjectFailed, model.ObjectUnknown:
		default:
			return nil, fmt.Errorf("未知对象状态 %q", state)
		}
	}
	for _, cursor := range cp.Cursors {
		if cursor < 0 {
			return nil, fmt.Errorf("日志偏移不能为负数")
		}
	}
	if cp.Errors == nil {
		cp.Errors = map[int]string{}
	}
	if cp.Cursors == nil {
		cp.Cursors = map[string]int64{}
	}
	l.Checkpoint = cp
	l.Recount()
	return l, nil
}

func completed(state model.ObjectState) bool {
	return state == model.ObjectCopied || state == model.ObjectSkipped
}

func (l *Ledger) Begin(attemptID string) {
	l.Checkpoint.AttemptID = attemptID
	l.Checkpoint.Progress = model.Progress{AttemptID: attemptID}
	l.Checkpoint.IntegrityError = ""
	l.Checkpoint.LastError = ""
	for i, state := range l.Checkpoint.States {
		if !completed(state) {
			l.Checkpoint.States[i] = model.ObjectPending
			delete(l.Checkpoint.Errors, i+1)
		}
	}
	l.Recount()
}

func (l *Ledger) result(key string, state model.ObjectState, message string) {
	i, ok := l.index[key]
	if !ok {
		l.Checkpoint.IntegrityError = fmt.Sprintf("执行证据出现清单之外的对象 %q", key)
		return
	}
	previous := l.Checkpoint.States[i]
	if previous == model.ObjectCopied {
		return
	}
	if previous == model.ObjectSkipped && state != model.ObjectCopied {
		return
	}
	l.Checkpoint.States[i] = state
	if state == model.ObjectFailed {
		if len(message) > 1024 {
			message = message[:1024]
		}
		l.Checkpoint.Errors[i+1] = strings.ToValidUTF8(message, "�")
	} else {
		delete(l.Checkpoint.Errors, i+1)
	}
}

func (l *Ledger) Sample(p model.Progress) {
	if p.AttemptID != l.Checkpoint.AttemptID || p.SampledAt.Before(l.Checkpoint.Progress.SampledAt) {
		return
	}
	l.Checkpoint.Progress = p
	for i, state := range l.Checkpoint.States {
		if state == model.ObjectActive {
			l.Checkpoint.States[i] = model.ObjectPending
		}
	}
	for _, key := range p.Checking {
		l.active(key)
	}
	for _, tr := range p.Transferring {
		l.active(tr.Name)
	}
	l.Recount()
}

func (l *Ledger) active(key string) {
	i, ok := l.index[key]
	if ok && !completed(l.Checkpoint.States[i]) && l.Checkpoint.States[i] != model.ObjectFailed {
		l.Checkpoint.States[i] = model.ObjectActive
	}
}

type Stats struct {
	LastError    string           `json:"lastError"`
	Bytes        int64            `json:"bytes"`
	Speed        float64          `json:"speed"`
	TotalBytes   int64            `json:"totalBytes"`
	ETA          *float64         `json:"eta"`
	Checking     []string         `json:"checking"`
	Transferring []model.Transfer `json:"transferring"`
}

func (s Stats) Progress(attempt string, now time.Time) model.Progress {
	p := model.Progress{AttemptID: attempt, SampledAt: now, TransferredBytes: s.Bytes, SpeedBytesPerSecond: s.Speed, Checking: s.Checking, Transferring: s.Transferring, ETA: s.ETA}
	if s.TotalBytes > 0 {
		n := s.TotalBytes
		p.TotalBytes = &n
	}
	for _, tr := range s.Transferring {
		if tr.Size < 0 {
			p.TotalBytes = nil
			p.ETA = nil
			break
		}
	}
	return p
}

func (l *Ledger) jsonLine(line []byte, attempt string) error {
	var event struct {
		Time    time.Time `json:"time"`
		Level   string    `json:"level"`
		Message string    `json:"msg"`
		Object  string    `json:"object"`
		Stats   *Stats    `json:"stats"`
	}
	if err := json.Unmarshal(line, &event); err != nil {
		return fmt.Errorf("无法解析 rclone JSON 日志: %w", err)
	}
	if event.Stats != nil && attempt == l.Checkpoint.AttemptID {
		l.Sample(event.Stats.Progress(attempt, event.Time))
		if event.Stats.LastError != "" {
			l.Checkpoint.LastError = event.Stats.LastError
		}
	}
	if strings.EqualFold(event.Level, "info") && copiedMessage.MatchString(strings.TrimSuffix(event.Message, "\n")) {
		if event.Object == "" {
			return fmt.Errorf("复制成功日志缺少 object")
		}
		l.result(event.Object, model.ObjectCopied, "")
	}
	switch strings.ToLower(event.Level) {
	case "error", "critical", "alert", "emergency":
		l.Checkpoint.Counts.ErrorEvents++
		l.Checkpoint.LastError = event.Message
		// --error 不覆盖清单读取失败；这里仅识别明确的对象终态错误。
		if event.Object != "" && (strings.HasPrefix(event.Message, "Failed to copy:") || strings.HasPrefix(event.Message, "--files-from failed to read file:")) {
			l.result(event.Object, model.ObjectFailed, event.Message)
		}
	}
	return nil
}

// Consume 保存每个证据文件的完整行偏移。未写完的尾行会留给下一轮读取。
func (l *Ledger) Consume(attemptDir, attemptID string, final bool) error {
	for _, name := range []string{"rclone.jsonl", "matched.txt", "failed.txt"} {
		key := attemptID + "/" + name
		path := filepath.Join(attemptDir, name)
		f, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		err = l.consumeFile(f, key, name, attemptID, final)
		closeErr := f.Close()
		if err != nil {
			l.Checkpoint.IntegrityError = err.Error()
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	l.Recount()
	return nil
}

func (l *Ledger) consumeFile(f *os.File, cursorKey, name, attemptID string, final bool) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	offset := l.Checkpoint.Cursors[cursorKey]
	if info.Size() < offset {
		return fmt.Errorf("执行证据被截断: %s", f.Name())
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		var line []byte
		for {
			part, err := r.ReadSlice('\n')
			line = append(line, part...)
			if len(line) > 16<<20 {
				return fmt.Errorf("执行证据单行超过 16MiB")
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if errors.Is(err, io.EOF) {
				if len(line) != 0 && final {
					return fmt.Errorf("执行证据存在不完整尾行: %s", f.Name())
				}
				return nil
			}
			if err != nil {
				return err
			}
			break
		}
		raw := line[:len(line)-1]
		switch name {
		case "rclone.jsonl":
			if err := l.jsonLine(raw, attemptID); err != nil {
				return err
			}
		case "matched.txt":
			l.result(string(raw), model.ObjectSkipped, "")
		case "failed.txt":
			l.result(string(raw), model.ObjectFailed, "rclone 对象错误报告；详见本次执行日志")
		}
		offset += int64(len(line))
		l.Checkpoint.Cursors[cursorKey] = offset
	}
}

func (l *Ledger) Finish() {
	for i, state := range l.Checkpoint.States {
		if state == model.ObjectPending || state == model.ObjectActive {
			l.Checkpoint.States[i] = model.ObjectUnknown
		}
	}
	l.Checkpoint.Progress.Checking = nil
	l.Checkpoint.Progress.Transferring = nil
	l.Checkpoint.Progress.SpeedBytesPerSecond = 0
	l.Recount()
}

func (l *Ledger) Recount() model.Counts {
	c := model.Counts{TotalObjects: int64(len(l.Keys)), ErrorEvents: l.Checkpoint.Counts.ErrorEvents}
	for _, state := range l.Checkpoint.States {
		switch state {
		case model.ObjectCopied:
			c.CopiedObjects++
		case model.ObjectSkipped:
			c.SkippedExistingObjects++
		case model.ObjectFailed:
			c.FailedObjects++
		case model.ObjectActive:
			c.ActiveObjects++
		case model.ObjectUnknown:
			c.UnknownObjects++
		default:
			c.PendingObjects++
		}
	}
	c.CompletedObjects = c.CopiedObjects + c.SkippedExistingObjects
	l.Checkpoint.Counts = c
	return c
}

func (l *Ledger) Save(s *store.Store, path string) error {
	l.Recount()
	l.Checkpoint.Revision++
	return s.Write(path, l.Checkpoint)
}

func (l *Ledger) Objects(state string, offset, limit int) ([]model.ObjectView, int) {
	result := make([]model.ObjectView, 0, limit)
	total := 0
	for i, key := range l.Keys {
		if state != "" && string(l.Checkpoint.States[i]) != state {
			continue
		}
		if total >= offset && len(result) < limit {
			result = append(result, model.ObjectView{Line: i + 1, Key: key, State: l.Checkpoint.States[i], Error: l.Checkpoint.Errors[i+1]})
		}
		total++
	}
	return result, total
}
