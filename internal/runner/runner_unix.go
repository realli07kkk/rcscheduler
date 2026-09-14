//go:build linux || darwin

// Package runner 管理独立 rclone 进程及其私有 RC 连接。
package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/ledger"
	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

const TestedVersion = "v1.75.1"

type Launch struct {
	Task         model.Task
	Attempt      model.Attempt
	ManifestPath string
	AttemptDir   string
	LockPath     string
}

type Result struct {
	ExitCode int
	Error    string
}

type Process interface {
	PID() int
	Done() <-chan struct{}
	Result() Result
	Stop(context.Context) error
	Stats(context.Context) (ledger.Stats, error)
}

type Engine interface {
	Version() string
	Socket(string) string
	Start(Launch) (Process, error)
	Recover(context.Context, string, string) error
}

type Rclone struct {
	Binary      string
	Config      string
	RuntimeDir  string
	version     string
	StopTimeout time.Duration
}

// Environment 禁止外部 RCLONE 过滤/删除/重命名选项改变受控命令；保留远端凭据变量。
func Environment() []string {
	var env []string
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(name, "RCLONE_") && !strings.HasPrefix(name, "RCLONE_CONFIG_") {
			continue
		}
		env = append(env, item)
	}
	return env
}

func New(ctx context.Context, binary, config, dataRoot, runtimeDir string, allowUntested bool) (*Rclone, error) {
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, fmt.Errorf("找不到 rclone 二进制: %w", err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	config, err = filepath.Abs(config)
	if err != nil {
		return nil, err
	}
	if st, err := os.Stat(config); err != nil || !st.Mode().IsRegular() {
		return nil, fmt.Errorf("rclone 配置文件不可读: %s", config)
	}
	if runtimeDir == "" {
		h := sha256.Sum256([]byte(dataRoot))
		tempRoot, err := filepath.EvalSymlinks("/tmp")
		if err != nil {
			return nil, err
		}
		// Unix socket 路径长度受限，深层数据目录不能直接用作 socket 目录。
		runtimeDir = filepath.Join(tempRoot, fmt.Sprintf("rcs-%d-%s", os.Geteuid(), hex.EncodeToString(h[:6])))
	}
	runtimeDir, err = filepath.Abs(runtimeDir)
	if err != nil {
		return nil, err
	}
	if err := store.EnsureDir(runtimeDir); err != nil {
		return nil, err
	}
	st, err := os.Lstat(runtimeDir)
	if err != nil {
		return nil, err
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("RC 目录必须归当前用户所有且权限为 0700: %s", runtimeDir)
	}
	r := &Rclone{Binary: path, Config: config, RuntimeDir: runtimeDir, StopTimeout: 30 * time.Second}
	if len(r.Socket(strings.Repeat("a", 24))) >= 100 {
		return nil, fmt.Errorf("RC socket 路径过长，请设置更短的 --runtime-dir")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, path, "version", "--config", config)
	cmd.Env = Environment()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("rclone version 检查失败: %w: %s", err, out)
	}
	line, _, _ := strings.Cut(string(out), "\n")
	r.version = strings.TrimSpace(strings.TrimPrefix(line, "rclone "))
	if r.version != TestedVersion && !allowUntested {
		return nil, fmt.Errorf("对象统计适配器要求 rclone %s，当前为 %s；通过兼容性测试后可使用 --allow-untested-rclone", TestedVersion, r.version)
	}
	return r, nil
}

func (r *Rclone) Version() string              { return r.version }
func (r *Rclone) Socket(attempt string) string { return filepath.Join(r.RuntimeDir, attempt+".sock") }

func (r *Rclone) Args(spec Launch) []string {
	a := spec.Attempt
	args := []string{"copy", spec.Task.Source, spec.Task.Destination,
		"--config", r.Config, "--files-from-raw", spec.ManifestPath,
		"--no-traverse", "--ignore-existing", "--disable", "Copy", "--multi-thread-streams", "0",
		"--bwlimit", strconv.FormatInt(a.BandwidthBytesPerSecond, 10) + "B",
		"--transfers", strconv.Itoa(a.Transfers), "--checkers", strconv.Itoa(a.Checkers), "--retries", "1",
		"--use-json-log", "--log-level", "INFO", "--stats", "2s", "--stats-log-level", "INFO",
		"--log-file", filepath.Join(spec.AttemptDir, "rclone.jsonl"),
		"--match", filepath.Join(spec.AttemptDir, "matched.txt"), "--error", filepath.Join(spec.AttemptDir, "failed.txt"),
		"--rc", "--rc-addr", "unix://" + a.Socket, "--rc-no-auth"}
	if a.UserAgent != "" {
		args = append(args, "--user-agent", a.UserAgent)
	}
	if a.S3UploadConcurrency > 0 {
		args = append(args, "--s3-upload-concurrency", strconv.Itoa(a.S3UploadConcurrency))
	}
	return args
}

func (r *Rclone) Start(spec Launch) (Process, error) {
	lock, err := store.Acquire(spec.LockPath)
	if err != nil {
		return nil, err
	}
	defer lock.Close() // 子进程继承后，父进程只关闭副本，不解锁。
	if err := store.EnsureDir(spec.AttemptDir); err != nil {
		return nil, err
	}
	stderr, err := os.OpenFile(filepath.Join(spec.AttemptDir, "stderr.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer stderr.Close()
	cmd := exec.Command(r.Binary, r.Args(spec)...)
	cmd.Env = Environment()
	cmd.Stdin = nil
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	cmd.ExtraFiles = []*os.File{lock.File}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &process{cmd: cmd, socket: spec.Attempt.Socket, done: make(chan struct{}), timeout: r.StopTimeout}
	go func() {
		err := cmd.Wait()
		result := Result{ExitCode: cmd.ProcessState.ExitCode()}
		if err != nil {
			result.Error = err.Error()
		}
		p.mu.Lock()
		p.result = result
		close(p.done)
		p.mu.Unlock()
	}()
	return p, nil
}

// Recover 仅联系保存的私有 RC socket，不根据可能已复用的 PID 发送信号。
func (r *Rclone) Recover(ctx context.Context, lockPath, socket string) error {
	if lock, err := store.Acquire(lockPath); err == nil {
		lock.Close()
		return nil
	}
	if filepath.Dir(socket) != r.RuntimeDir || filepath.Ext(socket) != ".sock" {
		return fmt.Errorf("旧执行的 RC socket 不属于当前实例")
	}
	deadline := time.NewTimer(r.StopTimeout + 5*time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	quitSent := false
	for {
		if lock, err := store.Acquire(lockPath); err == nil {
			lock.Close()
			os.Remove(socket)
			return nil
		}
		if !quitSent {
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := Call(callCtx, socket, "core/quit", map[string]any{"exitCode": 1}, nil)
			cancel()
			quitSent = err == nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("无法确认旧执行退出；保留执行锁并阻止重启")
		case <-ticker.C:
		}
	}
}

type process struct {
	cmd            *exec.Cmd
	socket         string
	done           chan struct{}
	mu             sync.Mutex
	result         Result
	timeout        time.Duration
	terminateOnce  sync.Once
	terminateError error
}

func (p *process) PID() int              { return p.cmd.Process.Pid }
func (p *process) Done() <-chan struct{} { return p.done }
func (p *process) Result() Result        { p.mu.Lock(); defer p.mu.Unlock(); return p.result }

func (p *process) Stop(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	// os.Process 检查进程是否已回收，避免给已复用的 PID 发信号。
	p.terminateOnce.Do(func() { p.terminateError = p.cmd.Process.Signal(syscall.SIGTERM) })
	if p.terminateError != nil && !errors.Is(p.terminateError, os.ErrProcessDone) {
		return p.terminateError
	}
	timer := time.NewTimer(p.timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return nil
	case <-timer.C:
		if err := p.cmd.Process.Signal(syscall.Signal(0)); err == nil {
			if err := syscall.Kill(-p.PID(), syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
		}
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *process) Stats(ctx context.Context) (ledger.Stats, error) {
	var out ledger.Stats
	err := Call(ctx, p.socket, "core/stats", map[string]any{}, &out)
	return out, err
}

func Call(ctx context.Context, socket, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://rclone/"+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("RC %s: %d %s", path, resp.StatusCode, b)
	}
	if output == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(output)
}
