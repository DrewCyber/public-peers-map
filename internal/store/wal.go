package store

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"

	"ymonitor/internal/collector"
)

// WAL is a one-file, append-only write-ahead log of ingest batches. A batch
// is appended before the first send attempt and the file is cleared once
// everything has been accepted. Because the worker dedups events (unique
// index) and upserts are idempotent, replaying the whole file after a partial
// failure is safe.
type WAL struct {
	path string
}

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &WAL{path: filepath.Join(dir, "wal.jsonl")}, nil
}

func (w *WAL) Append(batch collector.Batch) error {
	line, err := batch.MarshalJSONLine()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(line)
	return err
}

func (w *WAL) Pending() ([]collector.Batch, error) {
	f, err := os.Open(w.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []collector.Batch
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var b collector.Batch
		if err := json.Unmarshal(line, &b); err != nil {
			// A torn last line (crash mid-append) is skipped; the worker's
			// dedup makes re-sending earlier batches harmless.
			continue
		}
		out = append(out, b)
	}
	return out, sc.Err()
}

func (w *WAL) Clear() error {
	if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
