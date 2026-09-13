package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/httpapi"
	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
	"github.com/realli07kkk/rcscheduler/internal/service"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

func serve(ctx context.Context, root string, args []string, out, errOut io.Writer) error {
	f := flags("serve", errOut)
	binary := f.String("rclone", "rclone", "rclone 二进制路径")
	config := f.String("rclone-config", "", "rclone 配置文件路径（必填）")
	listen := f.String("listen", "127.0.0.1:8787", "HTTP 监听地址")
	runtimeDir := f.String("runtime-dir", "", "私有 Unix socket 目录，默认使用短临时路径")
	allowUntested := f.Bool("allow-untested-rclone", false, "允许尚未通过固定版本契约测试的 rclone")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("多余参数: %v", f.Args())
	}
	if *config == "" {
		return fmt.Errorf("需要 --rclone-config，服务不会猜测云凭据配置位置")
	}
	s, err := store.New(root)
	if err != nil {
		return err
	}
	engine, err := runner.New(ctx, *binary, *config, root, *runtimeDir, *allowUntested)
	if err != nil {
		return err
	}
	if err := store.EnsureDir(s.Path("logs")); err != nil {
		return err
	}
	logFile, err := os.OpenFile(s.Path("logs", "controller.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	logger := slog.New(slog.NewJSONHandler(io.MultiWriter(logFile, errOut), nil))
	svc, err := service.New(s, engine, service.Options{Logger: logger})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		if err := svc.Close(closeCtx); err != nil {
			logger.Error("服务停止失败", "error", err)
		}
	}()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	var conn model.Connection
	if err := store.Read(s.Path("client.json"), &conn); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if conn.Token == "" {
		conn.Token = model.NewID() + model.NewID()
	}
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	if host == "::" {
		host = "::1"
	}
	conn.URL = "http://" + net.JoinHostPort(host, port)
	if err := s.Write(s.Path("client.json"), conn); err != nil {
		return err
	}
	if err := svc.Start(ctx); err != nil {
		return err
	}
	server := &http.Server{Handler: httpapi.New(svc, conn.Token), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 << 10}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	fmt.Fprintf(out, "rcscheduler %s 已启动：%s\n数据目录：%s\nrclone：%s\n", Version, conn.URL, filepath.Clean(root), engine.Version())
	select {
	case <-ctx.Done():
		svc.BeginShutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
