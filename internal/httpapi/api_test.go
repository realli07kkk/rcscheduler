package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/runner"
	"github.com/realli07kkk/rcscheduler/internal/service"
	"github.com/realli07kkk/rcscheduler/internal/store"
)

type unusedEngine struct{}

func (unusedEngine) Version() string                               { return "test" }
func (unusedEngine) Socket(id string) string                       { return "/tmp/unused-" + id + ".sock" }
func (unusedEngine) Recover(context.Context, string, string) error { return nil }
func (unusedEngine) Start(runner.Launch) (runner.Process, error) {
	panic("测试中的调度器必须保持暂停")
}

func testAPI(t *testing.T) (http.Handler, *service.Service) {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := service.New(st, unusedEngine{}, service.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	paused := true
	if _, err := s.UpdateSettings(model.SettingsPatch{Paused: &paused}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return New(s, "test-token"), s
}
func call(h http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAuthenticationSettingsAndPagination(t *testing.T) {
	h, s := testAPI(t)
	if w := call(h, "GET", "/v1/scheduler", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := call(h, "PATCH", "/v1/scheduler", `{"maxRunning":0}`, "test-token"); w.Code != 400 {
		t.Fatal(w.Body.String())
	}
	w := call(h, "PATCH", "/v1/scheduler", `{"maxRunning":4,"bandwidthBudget":"40M"}`, "test-token")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var result struct {
		Next int64 `json:"nextTaskBandwidthBytesPerSecond"`
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if result.Next != 10<<20 {
		t.Fatal(w.Body.String())
	}
	if s.Settings().BandwidthBudget != "40M" {
		t.Fatal("更新未生效")
	}
	for _, q := range []string{"?offset=-1", "?limit=0", "?limit=1001", "?state=invalid"} {
		if w := call(h, "GET", "/v1/tasks"+q, "", "test-token"); w.Code != 400 {
			t.Fatal(w.Body.String())
		}
	}
	if w := call(h, "PATCH", "/v1/scheduler", `{"revision":1,"maxRunning":2}`, "test-token"); w.Code != 409 {
		t.Fatal(w.Body.String())
	}
}

func TestSchedulerRcloneOverrides(t *testing.T) {
	h, s := testAPI(t)
	patch := func(body string, status int) {
		t.Helper()
		w := call(h, "PATCH", "/v1/scheduler", body, "test-token")
		if w.Code != status {
			t.Fatalf("%s: %d %s", body, w.Code, w.Body.String())
		}
	}
	patch(`{"userAgent":"aws-sdk-go-v2/1.41.4","s3UploadConcurrency":8}`, 200)
	w := call(h, "GET", "/v1/scheduler", "", "test-token")
	var got model.Settings
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 200 || got.UserAgent != "aws-sdk-go-v2/1.41.4" || got.S3UploadConcurrency != 8 {
		t.Fatalf("读取配置失败: %s %v", w.Body.String(), err)
	}
	before := s.Settings()
	patch(`{"userAgent":"invalid\nagent"}`, 400)
	patch(`{"s3UploadConcurrency":-1}`, 400)
	patch(`{"s3UploadConcurrency":1.5}`, 400)
	patch(`{"revision":1,"userAgent":"conflict"}`, 409)
	if s.Settings() != before {
		t.Fatal("被拒绝的更新改变了配置")
	}
	patch(`{"bandwidthBudget":"40M"}`, 200)
	if s.Settings().UserAgent != before.UserAgent || s.Settings().S3UploadConcurrency != 8 {
		t.Fatal("部分更新覆盖了未指定字段")
	}
	patch(`{"userAgent":"","s3UploadConcurrency":0}`, 200)
	if s.Settings().UserAgent != "" || s.Settings().S3UploadConcurrency != 0 {
		t.Fatal("未清除覆盖值")
	}
}

func TestDirectoryImportAndNativeQueries(t *testing.T) {
	h, s := testAPI(t)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "001.txt"), []byte("a\n#literal\n"), 0600)
	body, _ := json.Marshal(model.ImportRequest{ID: "batch", ManifestDir: dir, Source: "src:bucket", Destination: "dst:bucket"})
	w := call(h, "POST", "/v1/batches/import", string(body), "test-token")
	if w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	var b model.Batch
	for time.Now().Before(deadline) {
		b, _ = s.Batch("batch")
		if b.ImportState == "ready" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b.ImportState != "ready" {
		t.Fatalf("%+v", b)
	}
	id := b.Manifests[0].TaskID
	for _, path := range []string{"/v1/batches/batch", "/v1/batches/batch/validation", "/v1/tasks?batchId=batch", "/v1/tasks/" + id + "/objects?state=pending", "/v1/tasks/" + id + "/attempts"} {
		if w := call(h, "GET", path, "", "test-token"); w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	}
	if w := call(h, "POST", "/v1/tasks/"+id+"/pause", "", "test-token"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := call(h, "POST", "/v1/tasks/"+id+"/retry", "", "test-token"); w.Code != 409 {
		t.Fatal(w.Body.String())
	}
}

func TestSingleUploadIsIdempotentAndImmutable(t *testing.T) {
	h, _ := testAPI(t)
	submit := func(content string) *httptest.ResponseRecorder {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		mw.WriteField("metadata", `{"id":"one","source":"src:bucket","destination":"dst:bucket"}`)
		part, _ := mw.CreateFormFile("manifest", "input.txt")
		io.WriteString(part, content)
		mw.Close()
		r := httptest.NewRequest("POST", "/v1/tasks", &body)
		r.Header.Set("Content-Type", mw.FormDataContentType())
		r.Header.Set("Authorization", "Bearer test-token")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := submit("a\n"); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := submit("a\n"); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := submit("different\n"); w.Code != 409 {
		t.Fatal(w.Body.String())
	}
}
