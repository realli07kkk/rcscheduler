// Package manifest 校验和快照行式对象清单，保持原始对象名。
package manifest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

const MaxObjects = 100000
const MaxManifests = 10000
const MaxBytes = 128 << 20

type InputError struct{ Cause error }

func (e *InputError) Error() string { return e.Cause.Error() }
func (e *InputError) Unwrap() error { return e.Cause }

type checkedWriter struct {
	writer io.Writer
	err    error
}

func (w *checkedWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func Discover(directory, pattern string, recursive bool) ([]string, error) {
	if _, err := filepath.Match(pattern, "x"); err != nil {
		return nil, fmt.Errorf("无效匹配模式: %w", err)
	}
	st, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("清单目录必须是普通目录")
	}
	var paths []string
	err = filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == directory {
			return nil
		}
		if entry.IsDir() {
			if !recursive {
				return filepath.SkipDir
			}
			return nil
		}
		match, _ := filepath.Match(pattern, entry.Name())
		if !match {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("匹配的清单不是普通文件: %s", path)
		}
		rel, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(rel))
		if len(paths) > MaxManifests {
			return fmt.Errorf("单批次最多 %d 份清单", MaxManifests)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("目录内没有匹配 %q 的清单", pattern)
	}
	sort.Strings(paths)
	return paths, nil
}

func Inspect(r io.Reader, name string) (keys []string, issues []model.ValidationIssue) {
	scanner := bufio.NewScanner(r) // 与 rclone 的行长度及 CRLF 处理一致。
	seen := make(map[string]int)
	line := 0
	add := func(code, message string, first int) {
		issues = append(issues, model.ValidationIssue{File: name, Line: line, FirstLine: first, Code: code, Message: message})
	}
	for scanner.Scan() {
		line++
		key := scanner.Text()
		switch {
		case !utf8.ValidString(key):
			add("invalid_utf8", "对象名不是有效 UTF-8", 0)
		case key == "":
			add("empty_line", "清单不能包含空行", 0)
		case strings.ContainsRune(key, 0):
			add("nul", "对象名包含 NUL", 0)
		case strings.ContainsRune(key, '\uFEFF'):
			add("bom", "清单不能包含 BOM", 0)
		case strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/"):
			add("slash", "首尾斜杠会被 rclone 隐式移除", 0)
		default:
			if first, ok := seen[key]; ok {
				add("duplicate", "同一清单内对象重复", first)
			} else {
				seen[key] = line
			}
		}
		keys = append(keys, key)
		if line > MaxObjects {
			add("too_many_objects", fmt.Sprintf("单清单最多 %d 个对象", MaxObjects), 0)
			break
		}
	}
	if err := scanner.Err(); err != nil {
		line++
		add("read_error", "清单无法读取或行过长: "+err.Error(), 0)
	}
	if line == 0 {
		add("empty_manifest", "清单不能为空", 0)
	}
	return keys, issues
}

// Snapshot 校验实际复制的字节，避免校验和后续执行读取不同版本。
func Snapshot(s *store.Store, source, target, relative string) (model.Manifest, []model.ValidationIssue, error) {
	info := model.Manifest{RelativePath: relative}
	before, err := os.Lstat(source)
	if err != nil {
		return info, nil, err
	}
	if !before.Mode().IsRegular() {
		return info, nil, fmt.Errorf("清单不是普通文件")
	}
	if before.Size() > MaxBytes {
		return info, nil, &InputError{fmt.Errorf("清单超过 128MiB")}
	}
	var issues []model.ValidationIssue
	err = s.AtomicWrite(target, func(w io.Writer) error {
		f, err := os.Open(source)
		if err != nil {
			return &InputError{err}
		}
		defer f.Close()
		opened, err := f.Stat()
		if err != nil {
			return &InputError{err}
		}
		if !os.SameFile(before, opened) {
			return &InputError{fmt.Errorf("读取前清单已被替换")}
		}
		h := sha256.New()
		limited := &io.LimitedReader{R: f, N: MaxBytes + 1}
		checked := &checkedWriter{writer: w}
		tee := io.TeeReader(limited, io.MultiWriter(checked, h))
		keys, found := Inspect(tee, relative)
		issues = found
		// 校验提早停止时仍复制余下内容，使保存的快照与摘要对应完整原文。
		if checked.err != nil {
			return checked.err
		}
		if _, err := io.Copy(io.MultiWriter(checked, h), limited); err != nil {
			return err
		}
		if limited.N == 0 {
			return &InputError{fmt.Errorf("清单读取期间超过 128MiB")}
		}
		after, err := f.Stat()
		if err != nil {
			return &InputError{err}
		}
		current, err := os.Lstat(source)
		if err != nil {
			return &InputError{err}
		}
		if !os.SameFile(before, current) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || before.ModTime() != current.ModTime() {
			return &InputError{fmt.Errorf("导入期间清单发生变化")}
		}
		info.SHA256 = hex.EncodeToString(h.Sum(nil))
		info.Objects = int64(len(keys))
		info.Bytes = after.Size()
		return nil
	})
	return info, issues, err
}

func Load(path, expectedHash string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > MaxBytes {
		return nil, fmt.Errorf("已存清单超过 128MiB")
	}
	h := sha256.New()
	keys, issues := Inspect(io.TeeReader(f, h), path)
	if len(issues) != 0 {
		return nil, fmt.Errorf("已存清单损坏: %s", issues[0].Message)
	}
	if hex.EncodeToString(h.Sum(nil)) != expectedHash {
		return nil, fmt.Errorf("清单 SHA-256 不匹配")
	}
	return keys, nil
}
