package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestSchedulerRcloneOverrides(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want map[string]any
	}{
		{"set", []string{"--user-agent", "aws-sdk-go-v2/1.41.4", "--s3-upload-concurrency", "8"}, map[string]any{"userAgent": "aws-sdk-go-v2/1.41.4", "s3UploadConcurrency": float64(8)}},
		{"clear", []string{"--user-agent", "", "--s3-upload-concurrency", "0"}, map[string]any{"userAgent": "", "s3UploadConcurrency": float64(0)}},
		{"partial", []string{"--user-agent", "custom agent"}, map[string]any{"userAgent": "custom agent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &client{http: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(body, tc.want) {
					t.Fatalf("请求字段错误: %+v", body)
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`)), Header: make(http.Header)}, nil
			})}}
			c.connection.URL = "http://local"
			if err := scheduler(context.Background(), c, append([]string{"set"}, tc.args...), io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHelpAndVersion(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"serve", "--help"}, {"version"}} {
		var out, errOut bytes.Buffer
		if err := Run(context.Background(), args, &out, &errOut); err != nil {
			t.Fatal(err)
		}
		if out.Len()+errOut.Len() == 0 {
			t.Fatal("没有帮助输出")
		}
	}
}

func TestSchedulerSetOnlySendsProvidedFields(t *testing.T) {
	c := &client{http: &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if r.Method != "PATCH" || r.URL.Path != "/v1/scheduler" || len(body) != 1 || body["bandwidthBudget"] != "40M" {
			t.Fatalf("%s %+v", r.URL, body)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"revision":2}`)), Header: make(http.Header)}, nil
	})}}
	c.connection.URL = "http://local"
	c.connection.Token = "test"
	if err := scheduler(context.Background(), c, []string{"set", "--bwlimit", "40M"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}
