package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	defaultPebbleDiskSlowThreshold = 500 * time.Millisecond
	defaultPebbleSlowJobThreshold  = 500 * time.Millisecond

	PebbleEventsLogFileName = "pebble_events.log"
)

var _ pebble.Logger = (*slogPebbleLogger)(nil)

type slogPebbleLogger struct {
	logger *slog.Logger
	exitFn func(int)
}

func newSlogPebbleLogger(base *slog.Logger, diskID string) *slogPebbleLogger {
	if base == nil {
		base = slog.Default()
	}
	return &slogPebbleLogger{
		logger: base.With("component", "pebble", "disk_id", diskID),
		exitFn: os.Exit,
	}
}

func formatPebbleMsg(format string, args ...any) string {
	return strings.TrimRight(fmt.Sprintf(format, args...), "\r\n")
}

func (l *slogPebbleLogger) Infof(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	if !l.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	l.logger.Debug(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Warnf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Warn(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Errorf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Error(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Fatalf(format string, args ...any) {
	if l != nil && l.logger != nil {
		l.logger.Error(formatPebbleMsg(format, args...), "fatal", true)
	}
	if l != nil && l.exitFn != nil {
		l.exitFn(1)
		return
	}
	os.Exit(1)
}

type pebbleEventLogWriter struct {
	mu     sync.Mutex
	diskID string
	file   *os.File
}

func openPebbleEventLogWriter(dir, diskID string, readOnly bool) *pebbleEventLogWriter {
	if readOnly || dir == "" {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(dir, PebbleEventsLogFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil
	}
	return &pebbleEventLogWriter{
		diskID: diskID,
		file:   f,
	}
}

func (w *pebbleEventLogWriter) write(event string, fields map[string]any, nonBlocking bool) {
	if w == nil {
		return
	}
	record := make(map[string]any, len(fields)+3)
	maps.Copy(record, fields)
	record["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	record["disk_id"] = w.diskID
	record["event"] = event

	line, err := json.Marshal(record)
	if err != nil {
		return
	}
	line = append(line, '\n')

	if nonBlocking {
		if !w.mu.TryLock() {
			return
		}
		defer w.mu.Unlock()
	} else {
		w.mu.Lock()
		defer w.mu.Unlock()
	}
	if w.file != nil {
		_, _ = w.file.Write(line)
	}
}

func (w *pebbleEventLogWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	f := w.file
	w.file = nil
	w.mu.Unlock()
	if f != nil {
		return f.Close()
	}
	return nil
}

type pebbleObsCloser struct {
	fsCloser io.Closer
	eventLog *pebbleEventLogWriter
}

func (c *pebbleObsCloser) Close() error {
	if c == nil {
		return nil
	}
	var fsErr error
	if c.fsCloser != nil {
		fsErr = c.fsCloser.Close()
	}
	if c.eventLog != nil {
		c.eventLog.write("close", nil, false)
	}
	logErr := c.eventLog.Close()
	return errors.Join(fsErr, logErr)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *StoreActor) configurePebbleObservability(pebbleOpts *pebble.Options, baseLogger *slog.Logger, diskSlowThreshold time.Duration) io.Closer {
	pLogger := newSlogPebbleLogger(baseLogger, s.diskID)
	pebbleOpts.Logger = pLogger

	if diskSlowThreshold <= 0 {
		diskSlowThreshold = defaultPebbleDiskSlowThreshold
	}

	var eventLog *pebbleEventLogWriter
	if pebbleOpts.FS == nil {
		eventLog = openPebbleEventLogWriter(s.dir, s.diskID, s.readOnly)
	}
	if eventLog != nil {
		eventLog.write("open_begin", map[string]any{
			"memtable_bytes":     pebbleOpts.MemTableSize,
			"wal_bytes_per_sync": pebbleOpts.WALBytesPerSync,
			"max_open_files":     pebbleOpts.MaxOpenFiles,
		}, false)
	}

	onDiskSlow := func(info vfs.DiskSlowInfo) {

		s.metrics.incPebbleDiskSlow()
		fileBase := filepath.Base(info.Path)
		pLogger.logger.Warn("pebble disk slow",
			"op", info.OpType.String(),
			"file", fileBase,
			"write_bytes", info.WriteSize,
			"duration_ms", info.Duration.Milliseconds(),
		)
		eventLog.write("disk_slow", map[string]any{
			"op":          info.OpType.String(),
			"file":        fileBase,
			"write_bytes": info.WriteSize,
			"duration_ms": info.Duration.Milliseconds(),
		}, true)
	}

	listener := pebble.EventListener{
		WALCreated: func(info pebble.WALCreateInfo) {
			eventLog.write("wal_created", map[string]any{
				"job_id":            info.JobID,
				"file_num":          info.FileNum.String(),
				"recycled_file_num": info.RecycledFileNum.String(),
				"err":               errString(info.Err),
			}, false)
		},
		WALDeleted: func(info pebble.WALDeleteInfo) {
			eventLog.write("wal_deleted", map[string]any{
				"job_id":   info.JobID,
				"file_num": info.FileNum.String(),
				"err":      errString(info.Err),
			}, false)
		},
		TableCreated: func(info pebble.TableCreateInfo) {
			eventLog.write("table_created", map[string]any{
				"job_id":   info.JobID,
				"reason":   info.Reason,
				"file_num": info.FileNum.String(),
			}, false)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			s.pebbleStallCount.Add(1)
			s.metrics.incPebbleStallEvents()
			pLogger.logger.Warn("pebble write stall begin", "reason", info.Reason)
			eventLog.write("write_stall_begin", map[string]any{
				"reason": info.Reason,
			}, false)
		},
		WriteStallEnd: func() {
			for {
				cur := s.pebbleStallCount.Load()
				if cur <= 0 {
					break
				}
				if s.pebbleStallCount.CompareAndSwap(cur, cur-1) {
					break
				}
			}
			pLogger.logger.Warn("pebble write stall end")
			eventLog.write("write_stall_end", nil, false)
		},
		DiskSlow: onDiskSlow,
		BackgroundError: func(err error) {
			pLogger.logger.Error("pebble background error", "err", err)
			eventLog.write("background_error", map[string]any{
				"err": errString(err),
			}, false)
		},
		DataCorruption: func(info pebble.DataCorruptionInfo) {
			pLogger.logger.Error("pebble data corruption",
				"path", info.Path,
				"is_remote", info.IsRemote,
				"err", info.Details,
			)
			eventLog.write("data_corruption", map[string]any{
				"path":      info.Path,
				"is_remote": info.IsRemote,
				"err":       errString(info.Details),
			}, false)
			pLogger.Fatalf("data corruption: %s", info)
		},
		FlushBegin: func(info pebble.FlushInfo) {
			eventLog.write("flush_begin", map[string]any{
				"job_id":      info.JobID,
				"reason":      info.Reason,
				"input_bytes": info.InputBytes,
			}, false)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			var wroteBytes uint64
			for _, tbl := range info.Output {
				wroteBytes += tbl.Size
			}
			eventLog.write("flush_end", map[string]any{
				"job_id":      info.JobID,
				"reason":      info.Reason,
				"input_bytes": info.InputBytes,
				"wrote_bytes": wroteBytes,
				"duration_ms": info.TotalDuration.Milliseconds(),
				"err":         errString(info.Err),
			}, false)
			if (info.Err != nil && !errors.Is(info.Err, pebble.ErrCancelledCompaction)) || info.TotalDuration > defaultPebbleSlowJobThreshold {
				pLogger.logger.Warn("pebble slow or failed flush",
					"job_id", info.JobID,
					"reason", info.Reason,
					"input_bytes", info.InputBytes,
					"duration_ms", info.TotalDuration.Milliseconds(),
					"err", info.Err,
				)
			}
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			var readBytes uint64
			for _, lvl := range info.Input {
				for _, tbl := range lvl.Tables {
					readBytes += tbl.Size
				}
			}
			eventLog.write("compaction_begin", map[string]any{
				"job_id":     info.JobID,
				"reason":     info.Reason,
				"read_bytes": readBytes,
			}, false)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			var readBytes, wroteBytes uint64
			for _, lvl := range info.Input {
				for _, tbl := range lvl.Tables {
					readBytes += tbl.Size
				}
			}
			for _, tbl := range info.Output.Tables {
				wroteBytes += tbl.Size
			}
			eventLog.write("compaction_end", map[string]any{
				"job_id":      info.JobID,
				"reason":      info.Reason,
				"read_bytes":  readBytes,
				"wrote_bytes": wroteBytes,
				"duration_ms": info.TotalDuration.Milliseconds(),
				"err":         errString(info.Err),
			}, false)
			if (info.Err != nil && !errors.Is(info.Err, pebble.ErrCancelledCompaction)) || info.TotalDuration > defaultPebbleSlowJobThreshold {
				pLogger.logger.Warn("pebble slow or failed compaction",
					"job_id", info.JobID,
					"reason", info.Reason,
					"duration_ms", info.TotalDuration.Milliseconds(),
					"err", info.Err,
				)
			}
		},
	}

	pebbleOpts.EventListener = &listener
	s.pebbleListener = &listener
	baseFS := pebbleOpts.FS
	if baseFS == nil {
		baseFS = vfs.Default
	}
	checkedFS, fsCloser := vfs.WithDiskHealthChecks(baseFS, diskSlowThreshold, nil, onDiskSlow)
	pebbleOpts.FS = checkedFS
	return &pebbleObsCloser{
		fsCloser: fsCloser,
		eventLog: eventLog,
	}
}
