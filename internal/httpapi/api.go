// Package httpapi 提供鉴权后的任务与批次 HTTP API。
package httpapi

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/realli07kkk/rcscheduler/internal/model"
	"github.com/realli07kkk/rcscheduler/internal/service"
)

type API struct {
	service *service.Service
	token   string
	mux     *http.ServeMux
}

func New(s *service.Service, token string) http.Handler {
	a := &API{service: s, token: token, mux: http.NewServeMux()}
	a.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]any{"ok": true}) })
	a.mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		h := s.Health()
		code := 200
		if h["ready"] != true {
			code = 503
		}
		respond(w, code, map[string]any{"ready": h["ready"]})
	})
	a.mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, s.Health()) })
	a.mux.HandleFunc("GET /v1/scheduler", func(w http.ResponseWriter, r *http.Request) { a.settings(w, s.Settings()) })
	a.mux.HandleFunc("PATCH /v1/scheduler", a.updateSettings)
	a.mux.HandleFunc("POST /v1/scheduler/{action}", a.schedulerAction)
	a.mux.HandleFunc("POST /v1/batches/import", a.importBatch)
	a.mux.HandleFunc("GET /v1/batches/{id}", a.batch)
	a.mux.HandleFunc("GET /v1/batches/{id}/validation", a.validation)
	a.mux.HandleFunc("GET /v1/tasks", a.tasks)
	a.mux.HandleFunc("POST /v1/tasks", a.submitSingle)
	a.mux.HandleFunc("GET /v1/tasks/{id}", a.task)
	a.mux.HandleFunc("GET /v1/tasks/{id}/attempts", a.attempts)
	a.mux.HandleFunc("GET /v1/tasks/{id}/objects", a.objects)
	a.mux.HandleFunc("PATCH /v1/tasks/{id}", a.priority)
	a.mux.HandleFunc("POST /v1/tasks/{id}/{action}", a.action)
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" {
		authorization := r.Header.Get("Authorization")
		provided := strings.TrimPrefix(authorization, "Bearer ")
		if !strings.HasPrefix(authorization, "Bearer ") || a.token == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(a.token)) != 1 {
			respond(w, 401, map[string]any{"error": map[string]string{"code": "unauthorized", "message": "需要有效的 Bearer token"}})
			return
		}
	}
	a.mux.ServeHTTP(w, r)
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, err error) {
	status := 500
	code := "internal_error"
	var p *service.Error
	if errors.As(err, &p) {
		status = p.Status
		code = p.Code
	}
	respond(w, status, map[string]any{"error": map[string]string{"code": code, "message": err.Error()}})
}

func decode(w http.ResponseWriter, r *http.Request, into any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(into); err != nil {
		return &service.Error{Status: 400, Code: "invalid_json", Message: err.Error()}
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return &service.Error{Status: 400, Code: "invalid_json", Message: "请求必须只包含一个 JSON 对象"}
	}
	return nil
}

func (a *API) settings(w http.ResponseWriter, s model.Settings) {
	b, _ := model.BandwidthBytes(s.BandwidthBudget)
	respond(w, 200, struct {
		model.Settings
		NextBandwidth int64 `json:"nextTaskBandwidthBytesPerSecond"`
	}{s, b / int64(s.MaxRunning)})
}
func (a *API) updateSettings(w http.ResponseWriter, r *http.Request) {
	var p model.SettingsPatch
	if err := decode(w, r, &p); err != nil {
		failure(w, err)
		return
	}
	s, err := a.service.UpdateSettings(p)
	if err != nil {
		failure(w, err)
		return
	}
	a.settings(w, s)
}
func (a *API) schedulerAction(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	if action != "pause" && action != "resume" {
		failure(w, &service.Error{Status: 400, Code: "invalid_action", Message: "只支持 pause/resume"})
		return
	}
	paused := action == "pause"
	s, err := a.service.UpdateSettings(model.SettingsPatch{Paused: &paused})
	if err != nil {
		failure(w, err)
		return
	}
	a.settings(w, s)
}
func (a *API) importBatch(w http.ResponseWriter, r *http.Request) {
	var req model.ImportRequest
	if err := decode(w, r, &req); err != nil {
		failure(w, err)
		return
	}
	b, err := a.service.Import(req)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 202, b)
}
func (a *API) batch(w http.ResponseWriter, r *http.Request) {
	b, err := a.service.Batch(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, b)
}
func (a *API) validation(w http.ResponseWriter, r *http.Request) {
	v, err := a.service.Validation(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, v)
}
func (a *API) task(w http.ResponseWriter, r *http.Request) {
	t, err := a.service.Task(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, t)
}
func (a *API) attempts(w http.ResponseWriter, r *http.Request) {
	t, err := a.service.Task(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, t.Attempts)
}
func (a *API) action(w http.ResponseWriter, r *http.Request) {
	t, err := a.service.Action(r.PathValue("id"), r.PathValue("action"))
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 202, t)
}
func (a *API) priority(w http.ResponseWriter, r *http.Request) {
	var p struct {
		Priority *int `json:"priority"`
	}
	if err := decode(w, r, &p); err != nil {
		failure(w, err)
		return
	}
	if p.Priority == nil {
		failure(w, &service.Error{Status: 400, Code: "invalid_priority", Message: "priority 必填"})
		return
	}
	t, err := a.service.Priority(r.PathValue("id"), *p.Priority)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, t)
}

func pagination(r *http.Request) (int, int, error) {
	offset, limit := 0, 100
	var err error
	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil {
			return 0, 0, err
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil {
			return 0, 0, err
		}
	}
	if offset < 0 || limit < 1 || limit > 1000 {
		return 0, 0, fmt.Errorf("offset 必须非负，limit 必须在 1..1000 内")
	}
	return offset, limit, nil
}

func (a *API) tasks(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		failure(w, &service.Error{Status: 400, Code: "pagination", Message: err.Error()})
		return
	}
	state := r.URL.Query().Get("state")
	if state != "" && !model.TaskState(state).Valid() {
		failure(w, &service.Error{Status: 400, Code: "invalid_state", Message: "无效任务状态"})
		return
	}
	items, total := a.service.Tasks(r.URL.Query().Get("batchId"), state, offset, limit)
	respond(w, 200, map[string]any{"items": items, "total": total, "offset": offset, "limit": limit})
}

func (a *API) objects(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		failure(w, &service.Error{Status: 400, Code: "pagination", Message: err.Error()})
		return
	}
	state := r.URL.Query().Get("state")
	switch model.ObjectState(state) {
	case "", model.ObjectPending, model.ObjectActive, model.ObjectCopied, model.ObjectSkipped, model.ObjectFailed, model.ObjectUnknown:
	default:
		failure(w, &service.Error{Status: 400, Code: "invalid_state", Message: "无效对象状态"})
		return
	}
	items, total, err := a.service.Objects(r.PathValue("id"), state, offset, limit)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 200, map[string]any{"items": items, "total": total, "offset": offset, "limit": limit})
}

func (a *API) submitSingle(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 129<<20)
	mr, err := r.MultipartReader()
	if err != nil {
		failure(w, &service.Error{Status: 400, Code: "invalid_multipart", Message: err.Error()})
		return
	}
	var metadata struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Source      string `json:"source"`
		Destination string `json:"destination"`
	}
	var upload *os.File
	hasMetadata := false
	defer func() {
		if upload != nil {
			upload.Close()
			os.Remove(upload.Name())
		}
	}()
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			failure(w, &service.Error{Status: 400, Code: "upload_failed", Message: err.Error()})
			return
		}
		switch part.FormName() {
		case "metadata":
			if hasMetadata {
				failure(w, &service.Error{Status: 400, Code: "duplicate_part", Message: "metadata 重复"})
				return
			}
			data, err := io.ReadAll(io.LimitReader(part, (1<<20)+1))
			if err != nil || len(data) > 1<<20 {
				failure(w, &service.Error{Status: 400, Code: "invalid_metadata", Message: "metadata 过大或不可读"})
				return
			}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&metadata); err != nil {
				failure(w, &service.Error{Status: 400, Code: "invalid_metadata", Message: err.Error()})
				return
			}
			if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
				failure(w, &service.Error{Status: 400, Code: "invalid_metadata", Message: "metadata 必须只包含一个对象"})
				return
			}
			hasMetadata = true
		case "manifest":
			if upload != nil {
				failure(w, &service.Error{Status: 400, Code: "duplicate_part", Message: "manifest 重复"})
				return
			}
			upload, err = os.CreateTemp(a.service.Store.Path("uploads"), "http-*")
			if err != nil {
				failure(w, a.service.PersistenceFailure(err))
				return
			}
			n, err := io.Copy(upload, io.LimitReader(part, (128<<20)+1))
			if err != nil || n > 128<<20 {
				var pathError *os.PathError
				if errors.As(err, &pathError) {
					failure(w, a.service.PersistenceFailure(err))
					return
				}
				failure(w, &service.Error{Status: 400, Code: "upload_failed", Message: "清单超过 128MiB 或上传失败"})
				return
			}
		default:
			failure(w, &service.Error{Status: 400, Code: "unknown_part", Message: "只接受 metadata 和 manifest"})
			return
		}
		part.Close()
	}
	if !hasMetadata || upload == nil {
		failure(w, &service.Error{Status: 400, Code: "missing_part", Message: "需要 metadata 和 manifest"})
		return
	}
	if metadata.ID == "" {
		metadata.ID = model.NewID()
	}
	if _, err := upload.Seek(0, io.SeekStart); err != nil {
		failure(w, err)
		return
	}
	t, err := a.service.SubmitSingle(metadata.ID, metadata.Name, metadata.Source, metadata.Destination, upload)
	if err != nil {
		failure(w, err)
		return
	}
	respond(w, 201, t)
}
