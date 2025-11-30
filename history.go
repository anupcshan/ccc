package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-json-experiment/json"
)

// HistoryDir returns the XDG-compliant path for history storage.
// Uses $XDG_DATA_HOME/ccc/history/ or ~/.local/share/ccc/history/
func HistoryDir() (string, error) {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		dataHome = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(dataHome, "ccc", "history"), nil
}

// HistoryFilename generates a filename for a given date.
// Format: YYYY-MM-DD-<start_epoch>-<end_epoch>.jsonl
// The range is [start, end) where end is the start of the next day.
func HistoryFilename(t time.Time) string {
	// Normalize to start of day in local time
	y, m, d := t.Date()
	startOfDay := time.Date(y, m, d, 0, 0, 0, 0, t.Location())
	endOfDay := startOfDay.AddDate(0, 0, 1)

	return startOfDay.Format("2006-01-02") + "-" +
		strconv.FormatInt(startOfDay.Unix(), 10) + "-" +
		strconv.FormatInt(endOfDay.Unix(), 10) + ".jsonl"
}

// ParseHistoryFilename extracts the time range from a history filename.
// Returns start and end Unix timestamps, or error if parsing fails.
func ParseHistoryFilename(name string) (start, end int64, err error) {
	// Strip directory and extension
	base := filepath.Base(name)
	base = strings.TrimSuffix(base, ".jsonl")

	// Format: YYYY-MM-DD-<start>-<end>
	// Split by "-" gives: [YYYY, MM, DD, start, end]
	parts := strings.Split(base, "-")
	if len(parts) < 5 {
		return 0, 0, os.ErrInvalid
	}

	start, err = strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	end, err = strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return start, end, nil
}

// FileOverlapsRange checks if a history file's time range overlaps with the query range.
// This is an O(1) check using only the filename - no file I/O needed.
func FileOverlapsRange(filename string, queryStart, queryEnd int64) bool {
	fileStart, fileEnd, err := ParseHistoryFilename(filename)
	if err != nil {
		return false
	}
	// Ranges overlap if: fileStart < queryEnd AND fileEnd > queryStart
	return fileStart < queryEnd && fileEnd > queryStart
}

// FilterFilesForRange returns only the files whose time ranges overlap with [queryStart, queryEnd).
func FilterFilesForRange(files []string, queryStart, queryEnd int64) []string {
	var result []string
	for _, f := range files {
		if FileOverlapsRange(f, queryStart, queryEnd) {
			result = append(result, f)
		}
	}
	return result
}

// ListHistoryFiles returns all history JSONL files in the history directory.
func ListHistoryFiles() ([]string, error) {
	dir, err := HistoryDir()
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No history yet
		}
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files, nil
}

// LoadUUIDs loads all UUIDs from a history file.
// Returns a set (map[string]bool) of UUIDs for deduplication.
func LoadUUIDs(file string) (map[string]bool, error) {
	f, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]bool), nil
		}
		return nil, err
	}
	defer f.Close()

	ids := make(map[string]bool)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse just enough to get uuid
		var entry struct {
			UUID string `json:"uuid"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			// Skip corrupted lines
			continue
		}
		if entry.UUID != "" {
			ids[entry.UUID] = true
		}
	}

	return ids, scanner.Err()
}

// AppendRawLines appends raw JSON lines to a history file with fsync.
// Creates the file and parent directories if they don't exist.
func AppendRawLines(file string, lines [][]byte) error {
	if len(lines) == 0 {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, line := range lines {
		if _, err := f.Write(line); err != nil {
			return err
		}
		if _, err := f.Write([]byte("\n")); err != nil {
			return err
		}
	}

	return f.Sync()
}

// HistoryFileForTimestamp returns the full path to the history file for a given timestamp.
func HistoryFileForTimestamp(t time.Time) (string, error) {
	dir, err := HistoryDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, HistoryFilename(t)), nil
}
