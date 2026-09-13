// Package store 提供带版本的 JSON 文档和持久原子发布。
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/realli07kkk/rcscheduler/internal/model"
)

type Store struct {
	Root    string
	faultMu sync.RWMutex
	fault   func(stage, path string) error
}

// SetFault 安全更新故障注入点；生产环境不配置故障函数。
func (s *Store) SetFault(fn func(stage, path string) error) {
	s.faultMu.Lock()
	s.fault = fn
	s.faultMu.Unlock()
}

type WriteError struct {
	Path  string
	Cause error
}

func (e *WriteError) Error() string { return fmt.Sprintf("发布 %s: %v", e.Path, e.Cause) }
func (e *WriteError) Unwrap() error { return e.Cause }

type envelope struct {
	Version int             `json:"version"`
	Record  json.RawMessage `json:"record"`
}

func New(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := EnsureDir(abs); err != nil {
		return nil, err
	}
	return &Store{Root: abs}, nil
}

// EnsureDir 同时同步新目录的父目录，覆盖首次创建的持久性。
func EnsureDir(path string) error {
	if st, err := os.Lstat(path); err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("不是普通目录: %s", path)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if parent == path {
		return fmt.Errorf("无法创建根目录 %s", path)
	}
	if err := EnsureDir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return SyncDir(parent)
}

func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s *Store) Path(parts ...string) string {
	return filepath.Join(append([]string{s.Root}, parts...)...)
}

func (s *Store) check(stage, path string) error {
	s.faultMu.RLock()
	fn := s.fault
	s.faultMu.RUnlock()
	if fn != nil {
		return fn(stage, path)
	}
	return nil
}

// AtomicWrite 的任何错误均须视作提交状态不确定，调用者不得盲目回滚后继续调度。
func (s *Store) AtomicWrite(path string, write func(io.Writer) error) (err error) {
	defer func() {
		if err != nil {
			err = &WriteError{Path: path, Cause: err}
		}
	}()
	if err = EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".publish-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { f.Close(); os.Remove(tmp) }()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if err = s.check("before_write", path); err != nil {
		return err
	}
	if err = write(f); err != nil {
		return err
	}
	if err = s.check("before_file_sync", path); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = s.check("before_rename", path); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	if err = s.check("after_rename", path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}

func (s *Store) Write(path string, value any) error {
	return s.AtomicWrite(path, func(w io.Writer) error {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(struct {
			Version int `json:"version"`
			Record  any `json:"record"`
		}{model.FormatVersion, value})
	})
}

func Read(path string, into any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 256<<20))
	decoder.DisallowUnknownFields()
	var e envelope
	if err := decoder.Decode(&e); err != nil {
		return fmt.Errorf("读取 %s: %w", path, err)
	}
	if e.Version != model.FormatVersion {
		return fmt.Errorf("%s 的格式版本 %d 不受支持", path, e.Version)
	}
	if len(e.Record) == 0 || string(e.Record) == "null" {
		return fmt.Errorf("%s 缺少 record", path)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s 包含多余或过大的 JSON 内容", path)
	}
	if err := json.Unmarshal(e.Record, into); err != nil {
		return fmt.Errorf("读取 %s record: %w", path, err)
	}
	return nil
}

func (s *Store) Copy(source, target string) error {
	return s.AtomicWrite(target, func(w io.Writer) error {
		f, err := os.Open(source)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	})
}
