package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/realli07kkk/rcscheduler/internal/model"
)

type client struct {
	connection model.Connection
	http       *http.Client
}

func (c *client) request(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.connection.URL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, output)
}
func (c *client) do(req *http.Request, output any) error {
	req.Header.Set("Authorization", "Bearer "+c.connection.Token)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("无法连接控制器: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct{ Code, Message string }
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if json.Unmarshal(b, &e) == nil && e.Error.Message != "" {
			return fmt.Errorf("%s: %s", e.Error.Code, e.Error.Message)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	if output == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(output)
}

func (c *client) upload(ctx context.Context, path string, metadata any) (model.Task, error) {
	f, err := os.Open(path)
	if err != nil {
		return model.Task{}, err
	}
	defer f.Close()
	r, w := io.Pipe()
	defer r.Close()
	mw := multipart.NewWriter(w)
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := func() error {
			b, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			if err := mw.WriteField("metadata", string(b)); err != nil {
				return err
			}
			part, err := mw.CreateFormFile("manifest", "manifest.txt")
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, f); err != nil {
				return err
			}
			return mw.Close()
		}()
		w.CloseWithError(err)
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.connection.URL, "/")+"/v1/tasks", r)
	if err != nil {
		r.Close()
		<-done
		return model.Task{}, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	var t model.Task
	err = c.do(req, &t)
	r.Close()
	<-done
	return t, err
}

func waitTick(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
