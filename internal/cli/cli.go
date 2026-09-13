// Package cli 提供服务入口和通过 HTTP 操作的客户端命令。
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

const Version = "0.1.0"

func flags(name string, errOut io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(errOut)
	return f
}
func printJSON(w io.Writer, v any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(v)
}

func Run(ctx context.Context, args []string, out, errOut io.Writer) error {
	err := run(ctx, args, out, errOut)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

func run(ctx context.Context, args []string, out, errOut io.Writer) error {
	f := flags("rcscheduler", errOut)
	defaultDir := os.Getenv("RCSCHEDULER_DATA_DIR")
	if defaultDir == "" {
		defaultDir = "./data"
	}
	dataDir := f.String("data-dir", defaultDir, "本地数据目录")
	server := f.String("server", "", "覆盖客户端服务 URL")
	token := f.String("token", "", "覆盖客户端 Bearer token")
	jsonOutput := f.Bool("json", false, "输出 JSON")
	f.Usage = func() {
		fmt.Fprintln(errOut, "用法: rcscheduler [--data-dir DIR] [--json] serve|batch|task|scheduler|status|version\n\n批次导入: batch import --id ID --manifest-dir DIR --source SRC --destination DST\n批次查询: batch show|watch|validation ID\n任务操作: task add|list|show|watch|objects|history|pause|resume|cancel|retry|priority\n调度配置: scheduler show|set|pause|resume\n运行服务: serve --rclone PATH --rclone-config PATH")
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	rest := f.Args()
	if len(rest) == 0 {
		f.Usage()
		return nil
	}
	root, err := filepath.Abs(*dataDir)
	if err != nil {
		return err
	}
	if rest[0] == "version" {
		fmt.Fprintln(out, "rcscheduler", Version)
		return nil
	}
	if rest[0] == "serve" {
		return serve(ctx, root, rest[1:], out, errOut)
	}
	var conn model.Connection
	if err := store.Read(filepath.Join(root, "client.json"), &conn); err != nil && (*server == "" || *token == "") {
		return fmt.Errorf("无法读取本地连接信息，请先启动 serve: %w", err)
	}
	if *server != "" {
		conn.URL = *server
	}
	if *token != "" {
		conn.Token = *token
	}
	c := &client{connection: conn, http: &http.Client{Timeout: 45 * time.Second}}
	switch rest[0] {
	case "batch":
		return batch(ctx, c, rest[1:], out, errOut, *jsonOutput)
	case "task":
		return task(ctx, c, rest[1:], out, errOut, *jsonOutput)
	case "scheduler":
		return scheduler(ctx, c, rest[1:], out, errOut)
	case "status":
		var v any
		if err := c.request(ctx, "GET", "/v1/status", nil, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	default:
		return fmt.Errorf("未知命令 %q", rest[0])
	}
}

func batch(ctx context.Context, c *client, args []string, out, errOut io.Writer, jsonOutput bool) error {
	if len(args) == 0 {
		return fmt.Errorf("需要 batch import|show|watch|validation")
	}
	action := args[0]
	if action == "import" {
		f := flags("batch import", errOut)
		id := f.String("id", "", "稳定批次 ID")
		dir := f.String("manifest-dir", "", "服务所在机器的清单目录")
		src := f.String("source", "", "rclone 源")
		dst := f.String("destination", "", "rclone 目标")
		pattern := f.String("pattern", "*.txt", "文件名匹配模式")
		recursive := f.Bool("recursive", false, "递归读取子目录")
		noWait := f.Bool("no-wait", false, "立即返回导入状态")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 {
			return fmt.Errorf("多余参数: %v", f.Args())
		}
		if *id == "" {
			*id = model.NewID()
		}
		var b model.Batch
		if err := c.request(ctx, "POST", "/v1/batches/import", model.ImportRequest{ID: *id, ManifestDir: *dir, Source: *src, Destination: *dst, Pattern: *pattern, Recursive: *recursive}, &b); err != nil {
			return err
		}
		if !*noWait {
			for b.ImportState == "validating" {
				if !jsonOutput {
					fmt.Fprintf(errOut, "批次 %s: 已校验 %d/%d 份清单\n", b.ID, b.CheckedFiles, b.DiscoveredFiles)
				}
				if !waitTick(ctx, time.Second) {
					return ctx.Err()
				}
				if err := c.request(ctx, "GET", "/v1/batches/"+url.PathEscape(b.ID), nil, &b); err != nil {
					return err
				}
			}
		}
		if err := printJSON(out, b); err != nil {
			return err
		}
		if b.ImportState == "rejected" || b.ImportState == "blocked" {
			return fmt.Errorf("批次未提交: %s；使用 batch validation %s 查看错误", b.Error, b.ID)
		}
		return nil
	}
	if len(args) != 2 {
		return fmt.Errorf("用法: batch %s ID", action)
	}
	path := "/v1/batches/" + url.PathEscape(args[1])
	if action == "validation" {
		path += "/validation"
	}
	if action != "show" && action != "watch" && action != "validation" {
		return fmt.Errorf("未知批次操作")
	}
	if action != "watch" {
		var v any
		if err := c.request(ctx, "GET", path, nil, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	}
	for {
		var b model.Batch
		if err := c.request(ctx, "GET", path, nil, &b); err != nil {
			return err
		}
		if jsonOutput {
			if err := printJSON(out, b); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(out, "%s  %s  任务:%d  运行:%d  复制:%d  跳过:%d  失败:%d  未知:%d  完成:%d/%d\n", time.Now().Format("15:04:05"), b.ID, b.Summary.Tasks, b.Summary.States[model.Running], b.Summary.Objects.CopiedObjects, b.Summary.Objects.SkippedExistingObjects, b.Summary.Objects.FailedObjects, b.Summary.Objects.UnknownObjects, b.Summary.Objects.CompletedObjects, b.Summary.Objects.TotalObjects)
		}
		if b.ImportState != "validating" && b.Summary.States[model.Queued]+b.Summary.States[model.Starting]+b.Summary.States[model.Running]+b.Summary.States[model.RetryWait]+b.Summary.States[model.Pausing]+b.Summary.States[model.Cancelling] == 0 {
			return nil
		}
		if !waitTick(ctx, 2*time.Second) {
			return nil
		}
	}
}

func scheduler(ctx context.Context, c *client, args []string, out, errOut io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("需要 scheduler show|set|pause|resume")
	}
	var result any
	switch args[0] {
	case "show":
		if err := c.request(ctx, "GET", "/v1/scheduler", nil, &result); err != nil {
			return err
		}
	case "pause", "resume":
		if err := c.request(ctx, "POST", "/v1/scheduler/"+args[0], nil, &result); err != nil {
			return err
		}
	case "set":
		f := flags("scheduler set", errOut)
		maxRunning := f.Int("max-running", 0, "全局任务并发数")
		bw := f.String("bwlimit", "", "新执行的带宽分配基准")
		transfers := f.Int("transfers", 0, "新执行的文件传输并发数")
		checkers := f.Int("checkers", 0, "新执行的检查并发数")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 {
			return fmt.Errorf("多余参数: %v", f.Args())
		}
		p := model.SettingsPatch{}
		f.Visit(func(v *flag.Flag) {
			switch v.Name {
			case "max-running":
				p.MaxRunning = maxRunning
			case "bwlimit":
				p.BandwidthBudget = bw
			case "transfers":
				p.Transfers = transfers
			case "checkers":
				p.Checkers = checkers
			}
		})
		if f.NFlag() == 0 {
			return fmt.Errorf("至少指定一个调度参数")
		}
		if err := c.request(ctx, "PATCH", "/v1/scheduler", p, &result); err != nil {
			return err
		}
	default:
		return fmt.Errorf("未知调度操作")
	}
	return printJSON(out, result)
}

func task(ctx context.Context, c *client, args []string, out, errOut io.Writer, jsonOutput bool) error {
	if len(args) == 0 {
		return fmt.Errorf("需要任务子命令")
	}
	action := args[0]
	if action == "add" {
		f := flags("task add", errOut)
		id := f.String("id", "", "任务 ID")
		name := f.String("name", "", "名称")
		src := f.String("source", "", "源")
		dst := f.String("destination", "", "目标")
		file := f.String("files-from-raw", "", "本机清单文件")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *file == "" {
			return fmt.Errorf("需要 --files-from-raw")
		}
		if *id == "" {
			*id = model.NewID()
		}
		if *name == "" {
			*name = filepath.Base(*file)
		}
		t, err := c.upload(ctx, *file, map[string]string{"id": *id, "name": *name, "source": *src, "destination": *dst})
		if err != nil {
			return err
		}
		return printJSON(out, t)
	}
	if action == "list" {
		f := flags("task list", errOut)
		batchID := f.String("batch-id", "", "批次筛选")
		state := f.String("state", "", "状态筛选")
		offset := f.Int("offset", 0, "分页偏移")
		limit := f.Int("limit", 100, "每页数量")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		q := url.Values{"batchId": {*batchID}, "state": {*state}, "offset": {strconv.Itoa(*offset)}, "limit": {strconv.Itoa(*limit)}}
		var result struct {
			Items  []model.Task `json:"items"`
			Total  int          `json:"total"`
			Offset int          `json:"offset"`
			Limit  int          `json:"limit"`
		}
		if err := c.request(ctx, "GET", "/v1/tasks?"+q.Encode(), nil, &result); err != nil {
			return err
		}
		if jsonOutput {
			return printJSON(out, result)
		}
		w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\t清单\t状态\t复制\t跳过\t失败\t未知\t完成/总数\t本次限速 B/s")
		for _, t := range result.Items {
			var bw int64
			if len(t.Attempts) > 0 {
				bw = t.Attempts[len(t.Attempts)-1].BandwidthBytesPerSecond
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d/%d\t%d\n", t.ID, t.Name, t.State, t.Counts.CopiedObjects, t.Counts.SkippedExistingObjects, t.Counts.FailedObjects, t.Counts.UnknownObjects, t.Counts.CompletedObjects, t.Counts.TotalObjects, bw)
		}
		w.Flush()
		fmt.Fprintf(out, "共 %d 个任务，当前从 %d 开始显示 %d 个\n", result.Total, result.Offset, len(result.Items))
		return nil
	}
	if len(args) < 2 {
		return fmt.Errorf("用法: task %s ID", action)
	}
	path := "/v1/tasks/" + url.PathEscape(args[1])
	if action == "objects" {
		f := flags("task objects", errOut)
		state := f.String("state", "", "对象状态")
		offset := f.Int("offset", 0, "分页偏移")
		limit := f.Int("limit", 100, "每页数量")
		if err := f.Parse(args[2:]); err != nil {
			return err
		}
		q := url.Values{"state": {*state}, "offset": {strconv.Itoa(*offset)}, "limit": {strconv.Itoa(*limit)}}
		var v any
		if err := c.request(ctx, "GET", path+"/objects?"+q.Encode(), nil, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	}
	if action == "priority" {
		if len(args) != 3 {
			return fmt.Errorf("用法: task priority ID NUMBER")
		}
		p, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		var v any
		if err := c.request(ctx, "PATCH", path, map[string]int{"priority": p}, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	}
	if len(args) != 2 {
		return fmt.Errorf("多余参数")
	}
	switch action {
	case "show", "history":
		if action == "history" {
			path += "/attempts"
		}
		var v any
		if err := c.request(ctx, "GET", path, nil, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	case "pause", "resume", "cancel", "retry":
		var v any
		if err := c.request(ctx, "POST", path+"/"+action, nil, &v); err != nil {
			return err
		}
		return printJSON(out, v)
	case "watch":
		for {
			var t model.Task
			if err := c.request(ctx, "GET", path, nil, &t); err != nil {
				return err
			}
			if jsonOutput {
				if err := printJSON(out, t); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(out, "%s  %s  %s  复制:%d  跳过:%d  失败:%d  未知:%d  完成:%d/%d\n", time.Now().Format("15:04:05"), t.ID, t.State, t.Counts.CopiedObjects, t.Counts.SkippedExistingObjects, t.Counts.FailedObjects, t.Counts.UnknownObjects, t.Counts.CompletedObjects, t.Counts.TotalObjects)
			}
			if !t.State.Active() && t.State != model.Queued && t.State != model.RetryWait {
				return nil
			}
			if !waitTick(ctx, 2*time.Second) {
				return nil
			}
		}
	default:
		return fmt.Errorf("未知任务操作 %q", action)
	}
}
