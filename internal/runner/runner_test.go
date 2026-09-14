//go:build linux || darwin

package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func TestRcloneWorkflowArguments(t *testing.T) {
	r := Rclone{Config: "/config"}
	spec := Launch{
		Task:         model.Task{Source: "source,no_head_object=true:example-bucket", Destination: "destination:example-bucket"},
		Attempt:      model.Attempt{BandwidthBytesPerSecond: 40 << 20, Transfers: 64, Checkers: 64, UserAgent: "custom agent/1.0", S3UploadConcurrency: 8},
		ManifestPath: "/lists/task with spaces.txt",
	}
	args := r.Args(spec)
	if !slices.Equal(args[:3], []string{"copy", spec.Task.Source, spec.Task.Destination}) {
		t.Fatalf("源和目标发生改变: %q", args)
	}
	for flag, want := range map[string]string{
		"--user-agent": "custom agent/1.0", "--s3-upload-concurrency": "8",
		"--bwlimit": "41943040B", "--transfers": "64", "--checkers": "64",
		"--files-from-raw": spec.ManifestPath, "--disable": "Copy", "--multi-thread-streams": "0",
	} {
		i := slices.Index(args, flag)
		if i < 0 || i+1 >= len(args) || args[i+1] != want || slices.Contains(args[i+1:], flag) {
			t.Fatalf("参数 %s 不符合预期: %q", flag, args)
		}
	}
	for _, flag := range []string{"--no-traverse", "--ignore-existing"} {
		if !slices.Contains(args, flag) {
			t.Fatalf("缺少 %s", flag)
		}
	}
	spec.Attempt.UserAgent, spec.Attempt.S3UploadConcurrency = "", 0
	args = r.Args(spec)
	if slices.Contains(args, "--user-agent") || slices.Contains(args, "--s3-upload-concurrency") {
		t.Fatalf("默认配置覆盖了 rclone 参数: %q", args)
	}
}

func TestControlledArgumentsAndEnvironment(t *testing.T) {
	t.Setenv("RCLONE_FILES_FROM", "unrelated.txt")
	t.Setenv("RCLONE_CONFIG_TEST_ACCESS_KEY_ID", "credential")
	env := strings.Join(Environment(), "\n")
	if strings.Contains(env, "RCLONE_FILES_FROM=") || !strings.Contains(env, "RCLONE_CONFIG_TEST_ACCESS_KEY_ID=") {
		t.Fatal("未正确隔离执行环境")
	}
	r := Rclone{Config: "/config"}
	args := strings.Join(r.Args(Launch{Task: model.Task{Source: "source:b", Destination: "target:b"}, Attempt: model.Attempt{BandwidthBytesPerSecond: 10 << 20, Transfers: 4, Checkers: 8, Socket: "/socket"}, AttemptDir: "/logs", ManifestPath: "/manifest"}), " ")
	for _, want := range []string{"--bwlimit 10485760B", "--retries 1", "--files-from-raw /manifest", "--match /logs/matched.txt", "--error /logs/failed.txt", "--multi-thread-streams 0"} {
		if !strings.Contains(args, want) {
			t.Fatal(args)
		}
	}
}

func TestRealInheritedLockAndRecovery(t *testing.T) {
	binary := os.Getenv("RCSCHEDULER_TEST_RCLONE")
	if binary == "" {
		t.Skip("需要 RCSCHEDULER_TEST_RCLONE")
	}
	root := t.TempDir()
	st, err := store.New(filepath.Join(root, "data"))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	os.Mkdir(source, 0700)
	os.Mkdir(destination, 0700)
	f, err := os.Create(filepath.Join(source, "large"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	manifest := filepath.Join(root, "list.txt")
	os.WriteFile(manifest, []byte("large\n"), 0600)
	config := filepath.Join(root, "rclone.conf")
	os.WriteFile(config, nil, 0600)
	r, err := New(context.Background(), binary, config, st.Root, "", false)
	if err != nil {
		t.Fatal(err)
	}
	r.StopTimeout = 3 * time.Second
	id := model.NewID()
	lockPath := st.Path("runtime", "task.lock")
	p, err := r.Start(Launch{Task: model.Task{ID: "task", Source: source, Destination: destination}, Attempt: model.Attempt{ID: id, Socket: r.Socket(id), BandwidthBytesPerSecond: 128 << 10, Transfers: 1, Checkers: 1}, ManifestPath: manifest, AttemptDir: st.Path("attempt"), LockPath: lockPath})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop(context.Background())
	if lock, err := store.Acquire(lockPath); err == nil {
		lock.Close()
		t.Fatal("父进程关闭 FD 后子进程没有继承执行锁")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Recover(ctx, lockPath, r.Socket(id)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	lock, err := store.Acquire(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	if _, err := os.Stat(r.Socket(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}
