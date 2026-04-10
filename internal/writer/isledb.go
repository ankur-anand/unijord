package writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/manifest"
	"github.com/ankur-anand/unijord/internal/config"
	"github.com/ankur-anand/walfs"
)

const defaultWriterMaxImmutableMemtables = 4

// IsleAppender is the production writer backend backed by a local WAL and IsleDB.
type IsleAppender struct {
	mu sync.Mutex

	store         *blobstore.Store
	db            *isledb.DB
	writer        *isledb.Writer
	wal           *walfs.WALog
	flushInterval time.Duration
	walMaxSync    time.Duration
	msyncPerWrite bool

	nextLSN        uint64
	lastPendingLSN uint64
	hasPending     bool
	walDirty       bool

	stopCh    chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func OpenIsleAppender(ctx context.Context, cfg config.WriterConfig) (*IsleAppender, error) {
	store, err := blobstore.Open(ctx, cfg.BucketURL, cfg.StoragePrefix())
	if err != nil {
		return nil, fmt.Errorf("open blobstore: %w", err)
	}

	appender, err := openIsleAppenderWithStore(ctx, cfg, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return appender, nil
}

func openIsleAppenderWithStore(ctx context.Context, cfg config.WriterConfig, store *blobstore.Store) (*IsleAppender, error) {
	wal, err := openWAL(cfg)
	if err != nil {
		return nil, err
	}

	db, err := isledb.OpenDB(ctx, store, isledb.DBOptions{
		CommittedLSNExtractor: isledb.BigEndianUint64LSNExtractor,
	})
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("open isledb database: %w", err)
	}

	opts := isledb.DefaultWriterOptions()
	opts.MemtableSize = int64(cfg.MemtableSizeMB) * 1024 * 1024
	opts.FlushInterval = 0
	opts.OwnerID = writerOwnerID(cfg)
	opts.MaxImmutableMemtables = defaultWriterMaxImmutableMemtables

	w, err := db.OpenWriter(ctx, opts)
	if err != nil {
		_ = db.Close()
		_ = wal.Close()
		return nil, fmt.Errorf("open isledb writer: %w", err)
	}

	appender := &IsleAppender{
		store:         store,
		db:            db,
		writer:        w,
		wal:           wal,
		flushInterval: cfg.FlushInterval,
		walMaxSync:    cfg.WALMaxSync,
		msyncPerWrite: cfg.WALMSyncWrite,
		stopCh:        make(chan struct{}),
	}

	if err := appender.bootstrap(ctx); err != nil {
		_ = appender.Close()
		return nil, err
	}

	appender.startLoops()
	return appender, nil
}

func openWAL(cfg config.WriterConfig) (*walfs.WALog, error) {
	if cfg.WALDir == "" {
		return nil, errors.New("wal directory is required")
	}

	opts := []walfs.WALogOptions{
		walfs.WithMaxSegmentSize(int64(cfg.WALSegmentMB) * 1024 * 1024),
		walfs.WithBytesPerSync(cfg.WALBytesSync),
	}
	if cfg.WALMSyncWrite {
		opts = append(opts, walfs.WithMSyncEveryWrite(true))
	}

	wal, err := walfs.NewWALog(cfg.WALDir, walFileExt, opts...)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return wal, nil
}

func (a *IsleAppender) bootstrap(ctx context.Context) error {
	committedLSN, err := readCommittedLSN(ctx, a.store)
	if err != nil {
		return err
	}

	replayedLSN, err := a.replayWAL(ctx, committedLSN)
	if err != nil {
		return err
	}

	if replayedLSN > committedLSN {
		committedLSN = replayedLSN
	}
	a.nextLSN = committedLSN + 1
	return nil
}

func (a *IsleAppender) replayWAL(ctx context.Context, committedLSN uint64) (uint64, error) {
	startLSN := committedLSN + 1
	startPos, ok := a.wal.LogIndex().Get(startLSN)
	if !ok {
		if err := a.cleanupCommittedSegments(committedLSN); err != nil {
			return 0, err
		}
		return committedLSN, nil
	}

	reader, err := a.wal.NewReaderWithStart(startPos)
	if err != nil {
		return 0, fmt.Errorf("open wal replay reader: %w", err)
	}
	defer reader.Close()

	maxReplayedLSN := committedLSN
	for {
		data, _, err := reader.Next()
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, walfs.ErrNoNewData):
			if maxReplayedLSN == committedLSN {
				return committedLSN, nil
			}
			if err := a.writer.Flush(ctx); err != nil {
				return 0, fmt.Errorf("flush replayed wal records: %w", err)
			}
			if err := a.cleanupCommittedSegments(maxReplayedLSN); err != nil {
				return 0, fmt.Errorf("cleanup replayed wal segments: %w", err)
			}
			return maxReplayedLSN, nil
		case err != nil:
			return 0, fmt.Errorf("read wal replay record: %w", err)
		}

		record, err := decodeWALRecord(data)
		if err != nil {
			return 0, fmt.Errorf("decode wal replay record: %w", err)
		}
		if record.LSN <= committedLSN {
			continue
		}
		if err := a.writer.Put(encodeLSNKey(record.LSN), record.Value); err != nil {
			return 0, fmt.Errorf("replay wal record lsn=%d into isledb: %w", record.LSN, err)
		}
		if record.LSN > maxReplayedLSN {
			maxReplayedLSN = record.LSN
		}
	}
}

func readCommittedLSN(ctx context.Context, store *blobstore.Store) (uint64, error) {
	ms := manifest.NewStoreWithStorage(manifest.NewBlobStoreBackend(store))
	ms.SetCommittedLSNExtractor(isledb.BigEndianUint64LSNExtractor)

	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		if errors.Is(err, manifest.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("read current manifest: %w", err)
	}
	if current == nil || current.MaxCommittedLSN == nil {
		return 0, nil
	}
	return *current.MaxCommittedLSN, nil
}

func (a *IsleAppender) startLoops() {
	if a.flushInterval > 0 {
		a.wg.Add(1)
		go a.flushLoop()
	}
	if a.walMaxSync > 0 && !a.msyncPerWrite {
		a.wg.Add(1)
		go a.syncLoop()
	}
}

func (a *IsleAppender) Append(_ context.Context, value []byte) (Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appendLocked(value)
}

func (a *IsleAppender) appendLocked(value []byte) (Record, error) {
	record := Record{
		LSN:   a.nextLSN,
		Value: cloneBytes(value),
	}

	if _, err := a.wal.Write(encodeWALRecord(record.LSN, record.Value), record.LSN); err != nil {
		return Record{}, fmt.Errorf("write wal record: %w", err)
	}

	if err := a.writer.Put(encodeLSNKey(record.LSN), record.Value); err != nil {
		if rollbackErr := a.wal.Truncate(record.LSN - 1); rollbackErr != nil {
			return Record{}, errors.Join(err, fmt.Errorf("rollback wal record lsn=%d: %w", record.LSN, rollbackErr))
		}
		return Record{}, err
	}

	a.nextLSN++
	a.lastPendingLSN = record.LSN
	a.hasPending = true
	a.walDirty = true

	return record, nil
}

func (a *IsleAppender) syncLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.walMaxSync)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := a.syncWAL(); err != nil {
				slog.Error("writer WAL sync failed", "error", err)
			}
		case <-a.stopCh:
			return
		}
	}
}

func (a *IsleAppender) flushLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := a.flushNow(ctx)
			cancel()
			if err != nil {
				slog.Error("writer flush failed", "error", err)
			}
		case <-a.stopCh:
			return
		}
	}
}

func (a *IsleAppender) syncWAL() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.walDirty {
		return nil
	}
	if err := a.wal.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	a.walDirty = false
	return nil
}

func (a *IsleAppender) flushNow(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flushLocked(ctx)
}

func (a *IsleAppender) flushLocked(ctx context.Context) error {
	if !a.hasPending {
		return nil
	}

	if a.walDirty {
		if err := a.wal.Sync(); err != nil {
			return fmt.Errorf("sync wal before flush: %w", err)
		}
		a.walDirty = false
	}

	committedLSN := a.lastPendingLSN
	if err := a.writer.Flush(ctx); err != nil {
		return fmt.Errorf("flush isledb writer: %w", err)
	}

	a.hasPending = false

	if err := a.cleanupCommittedSegments(committedLSN); err != nil {
		return fmt.Errorf("cleanup wal segments: %w", err)
	}
	return nil
}

func (a *IsleAppender) cleanupCommittedSegments(committedLSN uint64) error {
	if committedLSN == 0 {
		return nil
	}

	current := a.wal.Current()
	currentID := walfs.SegmentID(0)
	if current != nil {
		currentID = current.ID()
	}

	segments := a.wal.Segments()
	segmentIDs := make([]walfs.SegmentID, 0, len(segments))
	for id, seg := range segments {
		if id == currentID {
			continue
		}
		first := seg.FirstLogIndex()
		count := seg.GetEntryCount()
		if first == 0 || count == 0 {
			continue
		}
		last := first + uint64(count) - 1
		if last <= committedLSN {
			segmentIDs = append(segmentIDs, id)
		}
	}

	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })
	for _, id := range segmentIDs {
		if err := segments[id].Remove(); err != nil {
			return fmt.Errorf("remove wal segment %d: %w", id, err)
		}
	}
	return nil
}

func (a *IsleAppender) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		a.wg.Wait()

		a.mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		a.closeErr = errors.Join(a.closeErr, a.flushLocked(ctx))
		cancel()
		if a.walDirty {
			if err := a.wal.Sync(); err != nil {
				a.closeErr = errors.Join(a.closeErr, fmt.Errorf("sync wal on close: %w", err))
			}
			a.walDirty = false
		}
		a.mu.Unlock()

		if a.writer != nil {
			a.closeErr = errors.Join(a.closeErr, a.writer.Close())
		}
		if a.db != nil {
			a.closeErr = errors.Join(a.closeErr, a.db.Close())
		}
		if a.wal != nil {
			a.closeErr = errors.Join(a.closeErr, a.wal.Close())
		}
		if a.store != nil {
			a.closeErr = errors.Join(a.closeErr, a.store.Close())
		}
	})

	return a.closeErr
}

func writerOwnerID(cfg config.WriterConfig) string {
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return fmt.Sprintf("%s-%d", cfg.Namespace, cfg.Partition)
}
