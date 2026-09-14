// Package model 定义持久化文档和 HTTP 数据类型。
package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const FormatVersion = 1

var safeID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func ValidID(id string) bool { return safeID.MatchString(id) }

func NewID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type Settings struct {
	Revision            int64  `json:"revision"`
	MaxRunning          int    `json:"maxRunning"`
	BandwidthBudget     string `json:"bandwidthBudget"`
	Transfers           int    `json:"transfers"`
	Checkers            int    `json:"checkers"`
	UserAgent           string `json:"userAgent"`
	S3UploadConcurrency int    `json:"s3UploadConcurrency"`
	Paused              bool   `json:"paused"`
}

func DefaultSettings() Settings {
	return Settings{Revision: 1, MaxRunning: 4, BandwidthBudget: "20M", Transfers: 4, Checkers: 8}
}

// BandwidthBytes 仅接受固定正数速率，避免 0 被 rclone 解释为不限速。
func BandwidthBytes(value string) (int64, error) {
	s := strings.ToUpper(strings.TrimSpace(value))
	multiplier := float64(1)
	if len(s) == 0 {
		return 0, fmt.Errorf("带宽不能为空")
	}
	switch s[len(s)-1] {
	case 'B':
		s = s[:len(s)-1]
	case 'K':
		multiplier = 1 << 10
		s = s[:len(s)-1]
	case 'M':
		multiplier = 1 << 20
		s = s[:len(s)-1]
	case 'G':
		multiplier = 1 << 30
		s = s[:len(s)-1]
	case 'T':
		multiplier = 1 << 40
		s = s[:len(s)-1]
	default:
		return 0, fmt.Errorf("带宽必须带 B/K/M/G/T 单位，例如 20M")
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n*multiplier < 1 || n*multiplier >= float64(math.MaxInt64) {
		return 0, fmt.Errorf("无效带宽 %q", value)
	}
	return int64(n * multiplier), nil
}

func (s Settings) Validate() error {
	if strings.ContainsFunc(s.UserAgent, unicode.IsControl) {
		return fmt.Errorf("userAgent 不能包含控制字符")
	}
	if s.S3UploadConcurrency < 0 {
		return fmt.Errorf("s3UploadConcurrency 不能为负数，0 表示不覆盖 rclone 配置")
	}
	if s.MaxRunning < 1 || s.MaxRunning > 256 {
		return fmt.Errorf("maxRunning 必须在 1..256 内")
	}
	if s.Transfers < 1 || s.Transfers > 256 || s.Checkers < 1 || s.Checkers > 1024 {
		return fmt.Errorf("transfers 必须在 1..256，checkers 必须在 1..1024 内")
	}
	b, err := BandwidthBytes(s.BandwidthBudget)
	if err != nil {
		return err
	}
	if b/int64(s.MaxRunning) < 1 {
		return fmt.Errorf("分配后的每任务带宽必须至少为 1B/s")
	}
	return nil
}

type SettingsPatch struct {
	Revision            *int64  `json:"revision,omitempty"`
	MaxRunning          *int    `json:"maxRunning,omitempty"`
	BandwidthBudget     *string `json:"bandwidthBudget,omitempty"`
	Transfers           *int    `json:"transfers,omitempty"`
	Checkers            *int    `json:"checkers,omitempty"`
	UserAgent           *string `json:"userAgent,omitempty"`
	S3UploadConcurrency *int    `json:"s3UploadConcurrency,omitempty"`
	Paused              *bool   `json:"paused,omitempty"`
}

type ImportRequest struct {
	ID          string `json:"id"`
	ManifestDir string `json:"manifestDir"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Pattern     string `json:"pattern"`
	Recursive   bool   `json:"recursive"`
}

type Manifest struct {
	TaskID       string `json:"taskId"`
	RelativePath string `json:"relativePath"`
	SHA256       string `json:"sha256"`
	Objects      int64  `json:"objects"`
	Bytes        int64  `json:"bytes"`
}

type ValidationIssue struct {
	File      string `json:"file"`
	Line      int    `json:"line,omitempty"`
	FirstLine int    `json:"firstLine,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

type ValidationReport struct {
	BatchID          string            `json:"batchId"`
	Generation       int               `json:"generation"`
	CheckedFiles     int               `json:"checkedFiles"`
	TotalObjects     int64             `json:"totalObjects"`
	Issues           []ValidationIssue `json:"issues"`
	IssueCount       int64             `json:"issueCount"`
	IssuesByFile     map[string]int64  `json:"issuesByFile,omitempty"`
	DetailsTruncated bool              `json:"detailsTruncated,omitempty"`
	FinishedAt       time.Time         `json:"finishedAt"`
}

type Batch struct {
	ID                string        `json:"id"`
	Revision          int64         `json:"revision"`
	Request           ImportRequest `json:"request"`
	Generation        int           `json:"generation"`
	ImportState       string        `json:"importState"`
	SnapshotsComplete bool          `json:"snapshotsComplete"`
	DiscoveredFiles   int           `json:"discoveredFiles"`
	CheckedFiles      int           `json:"checkedFiles"`
	Manifests         []Manifest    `json:"manifests"`
	Error             string        `json:"error,omitempty"`
	CreatedAt         time.Time     `json:"createdAt"`
	CommittedAt       *time.Time    `json:"committedAt,omitempty"`
	Summary           BatchSummary  `json:"summary"`
}

type BatchSummary struct {
	Tasks     int               `json:"tasks"`
	States    map[TaskState]int `json:"states"`
	Objects   Counts            `json:"objects"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

type TaskState string

const (
	Queued      TaskState = "queued"
	Starting    TaskState = "starting"
	Running     TaskState = "running"
	RetryWait   TaskState = "retry_wait"
	Pausing     TaskState = "pausing"
	Paused      TaskState = "paused"
	Cancelling  TaskState = "cancelling"
	Cancelled   TaskState = "cancelled"
	Succeeded   TaskState = "succeeded"
	Failed      TaskState = "failed"
	NeedsReview TaskState = "needs_review"
	Blocked     TaskState = "blocked"
)

func (s TaskState) Active() bool {
	return s == Starting || s == Running || s == Pausing || s == Cancelling
}
func (s TaskState) Valid() bool {
	switch s {
	case Queued, Starting, Running, RetryWait, Pausing, Paused, Cancelling, Cancelled, Succeeded, Failed, NeedsReview, Blocked:
		return true
	}
	return false
}

type Task struct {
	ID              string     `json:"id"`
	Revision        int64      `json:"revision"`
	BatchID         string     `json:"batchId"`
	Generation      int        `json:"generation"`
	Name            string     `json:"name"`
	Source          string     `json:"source"`
	Destination     string     `json:"destination"`
	Manifest        Manifest   `json:"manifest"`
	State           TaskState  `json:"state"`
	DesiredState    string     `json:"desiredState"`
	Priority        int        `json:"priority"`
	CreatedAt       time.Time  `json:"createdAt"`
	QueuedAt        time.Time  `json:"queuedAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
	NextRunAt       *time.Time `json:"nextRunAt,omitempty"`
	FailuresInCycle int        `json:"failuresInCycle"`
	CurrentAttempt  string     `json:"currentAttempt,omitempty"`
	NextReason      string     `json:"nextReason,omitempty"`
	Attempts        []Attempt  `json:"attempts"`
	Counts          Counts     `json:"counts"`
	Error           string     `json:"error,omitempty"`
}

type Attempt struct {
	ID                      string     `json:"id"`
	Reason                  string     `json:"reason"`
	SettingsRevision        int64      `json:"settingsRevision"`
	BandwidthBudget         string     `json:"bandwidthBudget"`
	MaxRunning              int        `json:"maxRunning"`
	BandwidthBytesPerSecond int64      `json:"bandwidthBytesPerSecond"`
	Transfers               int        `json:"transfers"`
	Checkers                int        `json:"checkers"`
	UserAgent               string     `json:"userAgent"`
	S3UploadConcurrency     int        `json:"s3UploadConcurrency"`
	RcloneVersion           string     `json:"rcloneVersion"`
	PID                     int        `json:"pid,omitempty"`
	Socket                  string     `json:"socket"`
	StartedAt               time.Time  `json:"startedAt"`
	EndedAt                 *time.Time `json:"endedAt,omitempty"`
	ExitCode                *int       `json:"exitCode,omitempty"`
	Outcome                 string     `json:"outcome,omitempty"`
	Error                   string     `json:"error,omitempty"`
	Counts                  Counts     `json:"counts"`
	Progress                Progress   `json:"progress"`
}

type ObjectState string

const (
	ObjectPending ObjectState = "pending"
	ObjectActive  ObjectState = "active"
	ObjectCopied  ObjectState = "copied"
	ObjectSkipped ObjectState = "skipped_existing"
	ObjectFailed  ObjectState = "failed"
	ObjectUnknown ObjectState = "unknown"
)

type Counts struct {
	TotalObjects           int64 `json:"totalObjects"`
	CopiedObjects          int64 `json:"copiedObjects"`
	SkippedExistingObjects int64 `json:"skippedExistingObjects"`
	ActiveObjects          int64 `json:"activeObjects"`
	FailedObjects          int64 `json:"failedObjects"`
	PendingObjects         int64 `json:"pendingObjects"`
	UnknownObjects         int64 `json:"unknownObjects"`
	CompletedObjects       int64 `json:"completedObjects"`
	ErrorEvents            int64 `json:"errorEvents"`
}

func (c *Counts) Add(other Counts) {
	c.TotalObjects += other.TotalObjects
	c.CopiedObjects += other.CopiedObjects
	c.SkippedExistingObjects += other.SkippedExistingObjects
	c.ActiveObjects += other.ActiveObjects
	c.FailedObjects += other.FailedObjects
	c.PendingObjects += other.PendingObjects
	c.UnknownObjects += other.UnknownObjects
	c.CompletedObjects += other.CompletedObjects
	c.ErrorEvents += other.ErrorEvents
}

type Transfer struct {
	Name  string  `json:"name"`
	Bytes int64   `json:"bytes"`
	Size  int64   `json:"size"`
	Speed float64 `json:"speed"`
}

type Progress struct {
	AttemptID           string     `json:"attemptId"`
	SampledAt           time.Time  `json:"sampledAt"`
	TransferredBytes    int64      `json:"transferredBytes"`
	SpeedBytesPerSecond float64    `json:"speedBytesPerSecond"`
	TotalBytes          *int64     `json:"totalBytes,omitempty"`
	ETA                 *float64   `json:"eta,omitempty"`
	Checking            []string   `json:"checking,omitempty"`
	Transferring        []Transfer `json:"transferring,omitempty"`
}

type Checkpoint struct {
	Revision       int64            `json:"revision"`
	ManifestSHA256 string           `json:"manifestSha256"`
	AttemptID      string           `json:"attemptId"`
	States         []ObjectState    `json:"statesByLine"`
	Errors         map[int]string   `json:"errorsByLine,omitempty"`
	Cursors        map[string]int64 `json:"logOffsets"`
	Counts         Counts           `json:"counts"`
	Progress       Progress         `json:"progress"`
	IntegrityError string           `json:"integrityError,omitempty"`
	LastError      string           `json:"lastError,omitempty"`
}

type ObjectView struct {
	Line  int         `json:"line"`
	Key   string      `json:"key"`
	State ObjectState `json:"state"`
	Error string      `json:"error,omitempty"`
}

type Connection struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}
