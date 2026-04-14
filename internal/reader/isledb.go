package reader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/unijord/internal/config"
	_ "gocloud.dev/blob/s3blob"
)

const (
	defaultConsumeLimit  uint32 = 100
	defaultTailBatchSize uint32 = 256
	manifestOpTimeout           = 5 * time.Second
)

var ErrPartitionNotFound = errors.New("partition not found")

type partitionController struct {
	partition    int32
	store        *blobstore.Store
	coord        *isledb.Coordinator
	pollInterval time.Duration

	mu            sync.RWMutex
	highWatermark uint64
	notify        chan struct{}

	stopCh    chan struct{}
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
}

type IsleBackend struct {
	partitions map[int32]*partitionController

	closeOnce sync.Once
	closeErr  error
}

func OpenIsleBackend(ctx context.Context, cfg config.ReaderConfig) (*IsleBackend, error) {
	controllers := make(map[int32]*partitionController, cfg.Partitions)
	for partition := 0; partition < cfg.Partitions; partition++ {
		controller, err := openPartitionController(ctx, cfg, int32(partition))
		if err != nil {
			_ = closePartitionControllers(controllers)
			return nil, fmt.Errorf("open partition controller %d: %w", partition, err)
		}
		controllers[int32(partition)] = controller
	}

	return &IsleBackend{
		partitions: controllers,
	}, nil
}

func openPartitionController(ctx context.Context, cfg config.ReaderConfig, partition int32) (*partitionController, error) {
	store, err := blobstore.Open(ctx, cfg.BucketURL, cfg.PartitionPrefix(int(partition)))
	if err != nil {
		return nil, fmt.Errorf("open blobstore: %w", err)
	}

	cacheDir := cfg.PartitionCacheDir(int(partition))
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("create cache dir %q: %w", cacheDir, err)
	}

	opts := isledb.DefaultCoordinatorOptions()
	opts.ReaderOptions.CacheDir = cacheDir
	opts.ReaderOptions.AllowUnverifiedRangeRead = true
	opts.ReaderOptions.ValidateSSTChecksum = false
	opts.ReaderOptions.BlockCacheSize = cfg.EffectiveBlockCacheSizeBytes()
	opts.ReaderOptions.RangeReadMinSSTSize = cfg.EffectiveRangeReadMinSSTSizeBytes()

	coord, err := isledb.OpenCoordinator(ctx, store, opts)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open coordinator: %w", err)
	}

	initialView := coord.Current()
	if initialView == nil {
		_ = coord.Close()
		_ = store.Close()
		return nil, errors.New("current view not available")
	}
	defer initialView.Close()

	controller := &partitionController{
		partition:     partition,
		store:         store,
		coord:         coord,
		pollInterval:  cfg.EffectiveManifestPollInterval(),
		highWatermark: viewHighWatermark(initialView),
		notify:        make(chan struct{}),
		stopCh:        make(chan struct{}),
	}
	controller.wg.Add(1)
	go controller.refreshLoop()

	return controller, nil
}

func (b *IsleBackend) ConsumePartition(ctx context.Context, partition int32, startAfterLSN uint64, limit uint32) (ConsumeResult, error) {
	controller, err := b.partition(partition)
	if err != nil {
		return ConsumeResult{}, err
	}
	return controller.consume(ctx, startAfterLSN, limit)
}

func (b *IsleBackend) TailPartition(ctx context.Context, partition int32, startAfterLSN uint64, fromNow bool, handler func(Event) error) error {
	controller, err := b.partition(partition)
	if err != nil {
		return err
	}

	lastSeen := startAfterLSN
	if fromNow {
		view := controller.coord.Current()
		if view == nil {
			return context.Canceled
		}
		lastSeen = viewHighWatermark(view)
		_ = view.Close()
	}
	for {
		result, err := controller.consume(ctx, lastSeen, defaultTailBatchSize)
		if err != nil {
			return fmt.Errorf("consume partition %d tail batch: %w", partition, err)
		}

		for _, event := range result.Events {
			if err := handler(event); err != nil {
				return err
			}
			lastSeen = event.LSN
		}

		if lastSeen < result.HighWatermarkLSN {
			continue
		}

		if err := controller.waitForAdvance(ctx, result.HighWatermarkLSN); err != nil {
			return err
		}
	}
}

func (b *IsleBackend) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = closePartitionControllers(b.partitions)
	})
	return b.closeErr
}

func (b *IsleBackend) partition(partition int32) (*partitionController, error) {
	controller, ok := b.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrPartitionNotFound, partition)
	}
	return controller, nil
}

func (c *partitionController) consume(ctx context.Context, startAfterLSN uint64, limit uint32) (ConsumeResult, error) {
	if limit == 0 {
		limit = defaultConsumeLimit
	}

	view := c.coord.Current()
	if view == nil {
		return ConsumeResult{}, context.Canceled
	}
	defer view.Close()

	highWatermark := viewHighWatermark(view)
	result := ConsumeResult{
		NextStartAfterLSN: startAfterLSN,
		HighWatermarkLSN:  highWatermark,
	}
	if highWatermark == 0 || startAfterLSN >= highWatermark {
		return result, nil
	}

	catchUpOpts := isledb.CatchUpOptions{
		MaxKey: encodeLSNKey(highWatermark),
		Limit:  int(limit),
	}
	if startAfterLSN > 0 {
		catchUpOpts.StartAfterKey = encodeLSNKey(startAfterLSN)
	}

	_, err := view.CatchUp(ctx, catchUpOpts, func(kv isledb.KV) error {
		lsn, err := decodeLSNKey(kv.Key)
		if err != nil {
			return fmt.Errorf("decode partition %d lsn key: %w", c.partition, err)
		}

		result.Events = append(result.Events, Event{
			Partition: c.partition,
			LSN:       lsn,
			Value:     cloneBytes(kv.Value),
		})
		result.NextStartAfterLSN = lsn
		return nil
	})
	if err != nil {
		return ConsumeResult{}, fmt.Errorf("catch up partition %d: %w", c.partition, err)
	}

	return result, nil
}

func (c *partitionController) waitForAdvance(ctx context.Context, afterLSN uint64) error {
	for {
		highWatermark, notify := c.snapshot()
		if highWatermark > afterLSN {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		case <-c.stopCh:
			return context.Canceled
		}
	}
}

func (c *partitionController) refreshLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.pollCommittedState(); err != nil {
				continue
			}
		case <-c.stopCh:
			return
		}
	}
}

func (c *partitionController) pollCommittedState() error {
	ctx, cancel := context.WithTimeout(context.Background(), manifestOpTimeout)
	view, _, err := c.coord.Refresh(ctx)
	cancel()
	if err != nil {
		return err
	}
	if view == nil {
		return context.Canceled
	}
	defer view.Close()

	c.advanceHighWatermark(viewHighWatermark(view))
	return nil
}

func (c *partitionController) advanceHighWatermark(lsn uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if lsn <= c.highWatermark {
		return
	}

	c.highWatermark = lsn
	oldNotify := c.notify
	c.notify = make(chan struct{})
	close(oldNotify)
}

func (c *partitionController) snapshot() (uint64, <-chan struct{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.highWatermark, c.notify
}

func (c *partitionController) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopCh)
		c.wg.Wait()

		var firstErr error
		if err := c.coord.Close(); err != nil {
			firstErr = fmt.Errorf("close coordinator for partition %d: %w", c.partition, err)
		}
		if err := c.store.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close blobstore for partition %d: %w", c.partition, err)
		}

		c.closeErr = firstErr
	})
	return c.closeErr
}

func closePartitionControllers(controllers map[int32]*partitionController) error {
	var firstErr error
	for partition, controller := range controllers {
		if controller == nil {
			continue
		}
		if err := controller.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close partition controller %d: %w", partition, err)
		}
	}
	return firstErr
}

func viewHighWatermark(view isledb.View) uint64 {
	if view == nil {
		return 0
	}
	highWatermark, found := view.MaxCommittedLSN()
	if !found {
		return 0
	}
	return highWatermark
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func encodeLSNKey(lsn uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, lsn)
	return buf
}

func decodeLSNKey(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("lsn key must be 8 bytes, got %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}
