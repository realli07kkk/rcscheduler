package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
)

func TestRealCLIImportAndCrashRecovery(t *testing.T) {
	rclone := os.Getenv("RCSCHEDULER_TEST_RCLONE")
	if rclone == "" {
		t.Skip("设置 RCSCHEDULER_TEST_RCLONE 运行 CLI 端到端测试")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "rcscheduler")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/rcscheduler")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	data := filepath.Join(dir, "data")
	source := filepath.Join(dir, "source")
	dest := filepath.Join(dir, "destination")
	lists := filepath.Join(dir, "lists")
	for _, d := range []string{source, dest, lists} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(dir, "rclone.conf")
	os.WriteFile(config, nil, 0600)
	for i := 0; i < 3; i++ {
		var keys []string
		for j := 0; j < 2; j++ {
			key := fmt.Sprintf("file-%d-%d", i, j)
			keys = append(keys, key)
			os.WriteFile(filepath.Join(source, key), []byte(key), 0600)
		}
		os.WriteFile(filepath.Join(lists, fmt.Sprintf("%03d.txt", i)), []byte(strings.Join(keys, "\n")+"\n"), 0600)
	}
	type session struct {
		cmd  *exec.Cmd
		done chan struct{}
		err  error
	}
	start := func(number int) *session {
		logPath := filepath.Join(dir, fmt.Sprintf("server-%d.log", number))
		f, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		p := &session{cmd: exec.Command(binary, "--data-dir", data, "serve", "--rclone", rclone, "--rclone-config", config, "--listen", "127.0.0.1:0"), done: make(chan struct{})}
		p.cmd.Stdout = f
		p.cmd.Stderr = f
		if err := p.cmd.Start(); err != nil {
			f.Close()
			t.Fatal(err)
		}
		go func() { p.err = p.cmd.Wait(); f.Close(); close(p.done) }()
		t.Cleanup(func() {
			select {
			case <-p.done:
				return
			default:
			}
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(40 * time.Second):
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		})
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			b, _ := os.ReadFile(logPath)
			if strings.Contains(string(b), "已启动") {
				return p
			}
			select {
			case <-p.done:
				t.Fatalf("serve: %v %s", p.err, b)
			default:
			}
			time.Sleep(30 * time.Millisecond)
		}
		b, _ := os.ReadFile(logPath)
		t.Fatalf("服务启动超时: %s", b)
		return nil
	}
	command := func(into any, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, append([]string{"--data-dir", data, "--json"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v %s", args, err, out)
		}
		if into != nil {
			if err := json.Unmarshal(out, into); err != nil {
				t.Fatalf("输出非 JSON: %s %v", out, err)
			}
		}
	}
	waitTask := func(id string, state model.TaskState) model.Task {
		t.Helper()
		var task model.Task
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) {
			command(&task, "task", "show", id)
			if task.State == state {
				return task
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("任务状态不符合预期 %s: %+v", state, task)
		return task
	}
	p := start(1)
	command(nil, "scheduler", "pause")
	command(nil, "scheduler", "set", "--max-running", "1", "--bwlimit", "40M", "--transfers", "64", "--checkers", "64", "--user-agent", "aws-sdk-go-v2/1.41.4", "--s3-upload-concurrency", "8")
	var batch model.Batch
	command(&batch, "batch", "import", "--id", "initial", "--manifest-dir", lists, "--source", source, "--destination", dest)
	if batch.ImportState != "ready" || batch.Summary.Tasks != 3 {
		t.Fatalf("%+v", batch)
	}
	os.WriteFile(filepath.Join(lists, "000.txt"), []byte("outside-snapshot\n"), 0600)
	command(nil, "scheduler", "resume")
	for _, m := range batch.Manifests {
		task := waitTask(m.TaskID, model.Succeeded)
		if task.Counts.CopiedObjects != 2 {
			t.Fatalf("%+v", task.Counts)
		}
		if len(task.Attempts) != 1 || task.Attempts[0].UserAgent != "aws-sdk-go-v2/1.41.4" || task.Attempts[0].S3UploadConcurrency != 8 || task.Attempts[0].BandwidthBytesPerSecond != 40<<20 {
			t.Fatalf("迁移配置快照错误: %+v", task.Attempts)
		}
	}
	command(&batch, "batch", "show", "initial")
	if batch.Summary.Objects.CompletedObjects != 6 {
		t.Fatalf("%+v", batch.Summary)
	}
	command(nil, "scheduler", "pause")
	command(nil, "scheduler", "set", "--max-running", "1", "--bwlimit", "128K")
	large, err := os.Create(filepath.Join(source, "large"))
	if err != nil {
		t.Fatal(err)
	}
	if err := large.Truncate(8 << 20); err != nil {
		t.Fatal(err)
	}
	large.Close()
	os.WriteFile(filepath.Join(lists, "slow.txt"), []byte("large\n"), 0600)
	command(&batch, "batch", "import", "--id", "slow", "--manifest-dir", lists, "--pattern", "slow.txt", "--source", source, "--destination", dest)
	id := batch.Manifests[0].TaskID
	command(nil, "scheduler", "resume")
	running := waitTask(id, model.Running)
	oldSocket := running.Attempts[0].Socket
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = runner.Call(ctx, oldSocket, "core/quit", map[string]any{"exitCode": 1}, nil)
	})
	command(nil, "scheduler", "pause") // 保留暂停派发配置，重启后先检查恢复结果。
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-p.done
	_ = start(2)
	var settings model.Settings
	command(&settings, "scheduler", "show")
	if settings.UserAgent != "aws-sdk-go-v2/1.41.4" || settings.S3UploadConcurrency != 8 {
		t.Fatalf("重启丢失 rclone 配置: %+v", settings)
	}
	recovered := waitTask(id, model.Queued)
	if len(recovered.Attempts) != 1 || recovered.Attempts[0].Outcome != "interrupted" {
		t.Fatalf("未恢复中断记录: %+v", recovered)
	}
	command(nil, "scheduler", "set", "--bwlimit", "40M", "--user-agent", "recovery-agent", "--s3-upload-concurrency", "4")
	command(nil, "scheduler", "resume")
	finished := waitTask(id, model.Succeeded)
	if len(finished.Attempts) != 2 || finished.Attempts[0].BandwidthBytesPerSecond != 128<<10 || finished.Attempts[1].BandwidthBytesPerSecond != 40<<20 {
		t.Fatalf("重启参数快照错误: %+v", finished.Attempts)
	}
	if finished.Attempts[0].UserAgent != "aws-sdk-go-v2/1.41.4" || finished.Attempts[0].S3UploadConcurrency != 8 || finished.Attempts[1].UserAgent != "recovery-agent" || finished.Attempts[1].S3UploadConcurrency != 4 {
		t.Fatalf("恢复未保留新旧参数快照: %+v", finished.Attempts)
	}
	if finished.Counts.CopiedObjects != 1 || finished.Counts.CompletedObjects != 1 {
		t.Fatalf("恢复后计数错误: %+v", finished.Counts)
	}
}
