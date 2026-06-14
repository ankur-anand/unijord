package partitionlog

import (
	"time"

	plreader "github.com/ankur-anand/unijord/partitionlog/reader"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

// MetricName identifies one observed partitionlog event.
type MetricName string

const (
	MetricWriterAppend          MetricName = "writer.append"
	MetricWriterCut             MetricName = "writer.cut"
	MetricWriterFlush           MetricName = "writer.flush"
	MetricWriterClose           MetricName = "writer.close"
	MetricWriterAbort           MetricName = "writer.abort"
	MetricWriterSegmentFinalize MetricName = "writer.segment_finalize"
	MetricWriterSegmentPublish  MetricName = "writer.segment_publish"

	MetricReaderHead           MetricName = "reader.head"
	MetricReaderRead           MetricName = "reader.read"
	MetricReaderFetch          MetricName = "reader.fetch"
	MetricReaderTimestampRead  MetricName = "reader.timestamp_read"
	MetricReaderTailNext       MetricName = "reader.tail_next"
	MetricReaderCatalogRefresh MetricName = "reader.catalog_refresh"
	MetricReaderSegmentRead    MetricName = "reader.segment_read"
)

// Metric is a generic event emitted by public reader/writer operations and by
// background segment finalize/publish work.
type Metric struct {
	Name      MetricName
	Partition uint32

	LSN      uint64
	StartLSN uint64
	NextLSN  uint64
	Limit    int

	Records int
	Bytes   uint64

	SegmentURI   string
	SegmentCount uint64

	InflightSegments int
	InflightBytes    uint64

	Duration time.Duration
	Err      error
}

// Metrics receives partitionlog observations. Implementations must be safe for
// concurrent use because writer background workers and reader refresh loops may
// emit metrics concurrently with foreground calls.
type Metrics interface {
	Observe(Metric)
}

type writerMetricsAdapter struct {
	metrics Metrics
}

func (m writerMetricsAdapter) Observe(event plwriter.MetricEvent) {
	if m.metrics == nil {
		return
	}
	m.metrics.Observe(Metric{
		Name:       MetricName(event.Name),
		Partition:  event.Partition,
		StartLSN:   event.StartLSN,
		NextLSN:    event.NextLSN,
		Records:    event.Records,
		Bytes:      event.Bytes,
		SegmentURI: event.SegmentURI,
		Duration:   event.Duration,
		Err:        event.Err,
	})
}

type readerMetricsAdapter struct {
	metrics Metrics
}

func (m readerMetricsAdapter) Observe(event plreader.MetricEvent) {
	if m.metrics == nil {
		return
	}
	m.metrics.Observe(Metric{
		Name:       MetricName(event.Name),
		Partition:  event.Partition,
		StartLSN:   event.StartLSN,
		NextLSN:    event.NextLSN,
		Limit:      event.Limit,
		Records:    event.Records,
		SegmentURI: event.SegmentURI,
		Duration:   event.Duration,
		Err:        event.Err,
	})
}
