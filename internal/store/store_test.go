package store

import (
	"errors"
	"os"
	"testing"
)

func TestAtomicPublicationFailurePoints(t *testing.T) {
	for _, stage := range []string{"before_write", "before_file_sync", "before_rename", "after_rename"} {
		t.Run(stage, func(t *testing.T) {
			s, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			path := s.Path("task.json")
			if err := s.Write(path, map[string]int{"value": 1}); err != nil {
				t.Fatal(err)
			}
			s.SetFault(func(at, path string) error {
				if at == stage {
					return errors.New("injected disk failure")
				}
				return nil
			})
			if err := s.Write(path, map[string]int{"value": 2}); err == nil {
				t.Fatal("必须报告提交失败")
			}
			var v map[string]int
			if err := Read(path, &v); err != nil {
				t.Fatal(err)
			}
			want := 1
			if stage == "after_rename" {
				want = 2
			}
			if v["value"] != want {
				t.Fatalf("value=%d", v["value"])
			}
			entries, _ := os.ReadDir(s.Root)
			if len(entries) != 1 {
				t.Fatalf("临时文件未清理: %v", entries)
			}
		})
	}
}

func TestLockAndFormat(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l, err := Acquire(s.Path("controller.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Acquire(s.Path("controller.lock")); err == nil {
		second.Close()
		t.Fatal("允许了第二个写入者")
	}
	l.Close()
	l, err = Acquire(s.Path("controller.lock"))
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	for _, input := range []string{`{"version":99,"record":{}}`, `{"version":1,"record":null}`, `{"version":1,"record":{}} {}`, `broken`} {
		if err := os.WriteFile(s.Path("bad.json"), []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if err := Read(s.Path("bad.json"), new(map[string]any)); err == nil {
			t.Fatalf("接受坏文档 %s", input)
		}
	}
}
