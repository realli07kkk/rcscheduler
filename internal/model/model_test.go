package model

import (
	"encoding/json"
	"testing"
)

func TestRcloneSettingsValidationAndLegacyDefaults(t *testing.T) {
	var legacy Settings
	if err := json.Unmarshal([]byte(`{"revision":1,"maxRunning":1,"bandwidthBudget":"40M","transfers":64,"checkers":64,"paused":false}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Validate(); err != nil || legacy.UserAgent != "" || legacy.S3UploadConcurrency != 0 {
		t.Fatalf("旧配置不兼容: %+v %v", legacy, err)
	}
	for _, ua := range []string{"", "aws-sdk-go-v2/1.41.4", "custom agent/1.0"} {
		s := DefaultSettings()
		s.UserAgent, s.S3UploadConcurrency = ua, 8
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, ua := range []string{"a\x00b", "a\nb", "a\rb", "a\tb", "a\x7fb", "a\u0085b"} {
		s := DefaultSettings()
		s.UserAgent = ua
		if s.Validate() == nil {
			t.Fatalf("接受了控制字符: %q", ua)
		}
	}
	s := DefaultSettings()
	s.S3UploadConcurrency = -1
	if s.Validate() == nil {
		t.Fatal("接受了负上传并发数")
	}
	var attempt Attempt
	if err := json.Unmarshal([]byte(`{"id":"old","transfers":64,"checkers":64}`), &attempt); err != nil || attempt.UserAgent != "" || attempt.S3UploadConcurrency != 0 {
		t.Fatalf("旧执行记录不兼容: %+v %v", attempt, err)
	}
}

func TestBandwidthValidation(t *testing.T) {
	for _, v := range []string{"0B", "-1M", "20", "NaNM", "InfM", "1e999G"} {
		if _, err := BandwidthBytes(v); err == nil {
			t.Fatalf("接受了 %q", v)
		}
	}
	b, err := BandwidthBytes("20M")
	if err != nil || b != 20*1024*1024 {
		t.Fatalf("%d %v", b, err)
	}
	s := DefaultSettings()
	s.BandwidthBudget = "1B"
	if s.Validate() == nil {
		t.Fatal("不能分配到 0B 导致取消限速")
	}
}
