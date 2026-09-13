package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/realli07kkk/rcscheduler/internal/store"
)

func TestRawNamesAndValidation(t *testing.T) {
	valid := "中文/合同.pdf\r\n leading space \r\n#literal\r\n;literal\r\nfile with spaces"
	keys, issues := Inspect(strings.NewReader(valid), "task.txt")
	if len(issues) != 0 || len(keys) != 5 || keys[1] != " leading space " || keys[2] != "#literal" {
		t.Fatalf("keys=%q issues=%+v", keys, issues)
	}
	for _, tc := range []struct{ name, text, code string }{
		{"duplicate", "a\nb\na\n", "duplicate"}, {"blank", "a\n\nb\n", "empty_line"}, {"bom", "\ufeffa\n", "bom"}, {"nul", "a\x00b\n", "nul"}, {"utf8", "a\xff\n", "invalid_utf8"}, {"leading slash", "/a\n", "slash"}, {"trailing slash", "a/\n", "slash"}, {"empty", "", "empty_manifest"}, {"long", strings.Repeat("a", 65536), "read_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, issues := Inspect(strings.NewReader(tc.text), "x.txt")
			if len(issues) == 0 || issues[0].Code != tc.code {
				t.Fatalf("%+v", issues)
			}
			if tc.code == "duplicate" && (issues[0].Line != 3 || issues[0].FirstLine != 1) {
				t.Fatalf("%+v", issues)
			}
		})
	}
}

func TestSnapshotImmutableAndChangedInput(t *testing.T) {
	dir := t.TempDir()
	s, err := store.New(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "input.txt")
	if err := os.WriteFile(src, []byte("a\r\n#b\n c "), 0600); err != nil {
		t.Fatal(err)
	}
	dst := s.Path("manifest.txt")
	info, issues, err := Snapshot(s, src, dst, "input.txt")
	if err != nil || len(issues) != 0 {
		t.Fatalf("%v %+v", err, issues)
	}
	if err := os.WriteFile(src, []byte("changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := Load(dst, info.SHA256)
	if err != nil || len(keys) != 3 || keys[2] != " c " {
		t.Fatalf("%q %v", keys, err)
	}
	s.SetFault(func(stage, path string) error {
		if stage == "before_write" {
			return os.WriteFile(src, []byte("changed again and longer\n"), 0600)
		}
		return nil
	})
	if _, _, err := Snapshot(s, src, s.Path("other.txt"), "input.txt"); err == nil {
		t.Fatal("未检测到导入期间变化")
	}
}

func TestDiscoverSnapshotOrderAndRecursion(t *testing.T) {
	dir := t.TempDir()
	os.Mkdir(filepath.Join(dir, "nested"), 0700)
	for _, path := range []string{"002.txt", "001.txt", "ignored.csv", "nested/003.txt"} {
		if err := os.WriteFile(filepath.Join(dir, path), []byte("key\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	paths, err := Discover(dir, "*.txt", false)
	if err != nil || strings.Join(paths, ",") != "001.txt,002.txt" {
		t.Fatalf("%v %v", paths, err)
	}
	paths, err = Discover(dir, "*.txt", true)
	if err != nil || len(paths) != 3 {
		t.Fatalf("%v %v", paths, err)
	}
	if err := os.Symlink(filepath.Join(dir, "001.txt"), filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := Discover(dir, "*.txt", false); err == nil {
		t.Fatal("不应跟随匹配的符号链接")
	}
}
