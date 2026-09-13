package model

import "testing"

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
