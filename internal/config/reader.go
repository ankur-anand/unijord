package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	EnvReaderBucketURL     = "EVENTLAKE_BUCKET_URL"
	EnvReaderNamespace     = "EVENTLAKE_NAMESPACE"
	EnvReaderPartitions    = "EVENTLAKE_PARTITIONS"
	EnvReaderHTTPListen    = "EVENTLAKE_HTTP_LISTEN"
	EnvReaderGRPCListen    = "EVENTLAKE_GRPC_LISTEN"
	EnvReaderMetricsListen = "EVENTLAKE_METRICS_LISTEN"
	EnvReaderCacheDir      = "EVENTLAKE_CACHE_DIR"
	EnvReaderManifestPoll  = "EVENTLAKE_MANIFEST_POLL_INTERVAL_MS"
	EnvReaderBlockCacheMB  = "EVENTLAKE_BLOCK_CACHE_SIZE_MB"
	EnvReaderRangeReadKB   = "EVENTLAKE_RANGE_READ_MIN_SST_SIZE_KB"
	EnvReaderLogLevel      = "EVENTLAKE_LOG_LEVEL"

	defaultReaderHTTPListen    = ":8090"
	defaultReaderGRPCListen    = ":9090"
	defaultReaderMetricsListen = ":9100"
	defaultReaderManifestPoll  = 100 * time.Millisecond
	defaultReaderBlockCacheMB  = 16
	defaultReaderRangeReadKB   = 32
	defaultReaderLogLevel      = "info"
)

// ReaderConfig is the fully resolved runtime configuration for one namespace-bound reader.
type ReaderConfig struct {
	BucketURL            string
	Namespace            string
	Partitions           int
	HTTPListen           string
	GRPCListen           string
	MetricsListen        string
	CacheDir             string
	ManifestPollInterval time.Duration
	BlockCacheSizeMB     int
	RangeReadMinSSTKB    int
	LogLevel             string
}

// PartitionPrefix returns the object-store prefix for one namespace partition.
func (c ReaderConfig) PartitionPrefix(partition int) string {
	return fmt.Sprintf("%s/p%03d/", c.Namespace, partition)
}

// PartitionCacheDir returns the cache directory for one partition tailer.
func (c ReaderConfig) PartitionCacheDir(partition int) string {
	return fmt.Sprintf("%s/p%03d", strings.TrimRight(c.CacheDir, "/"), partition)
}

// LoadReaderConfigFromEnv resolves ReaderConfig from the current process environment.
func LoadReaderConfigFromEnv() (ReaderConfig, error) {
	return LoadReaderConfig(os.Getenv)
}

// LoadReaderConfig resolves ReaderConfig using the provided environment lookup.
func LoadReaderConfig(getenv func(string) string) (ReaderConfig, error) {
	cfg := ReaderConfig{
		HTTPListen:           defaultReaderHTTPListen,
		GRPCListen:           defaultReaderGRPCListen,
		MetricsListen:        defaultReaderMetricsListen,
		ManifestPollInterval: defaultReaderManifestPoll,
		BlockCacheSizeMB:     defaultReaderBlockCacheMB,
		RangeReadMinSSTKB:    defaultReaderRangeReadKB,
		LogLevel:             defaultReaderLogLevel,
	}

	var errs []string

	cfg.BucketURL = strings.TrimSpace(getenv(EnvReaderBucketURL))
	if cfg.BucketURL == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvReaderBucketURL))
	}

	cfg.Namespace = strings.TrimSpace(getenv(EnvReaderNamespace))
	if cfg.Namespace == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvReaderNamespace))
	}

	cfg.CacheDir = strings.TrimSpace(getenv(EnvReaderCacheDir))
	if cfg.CacheDir == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvReaderCacheDir))
	}

	rawPartitions := strings.TrimSpace(getenv(EnvReaderPartitions))
	if rawPartitions == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvReaderPartitions))
	} else {
		partitions, err := parsePositiveInt(rawPartitions)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvReaderPartitions, err))
		} else {
			cfg.Partitions = partitions
		}
	}

	if raw := strings.TrimSpace(getenv(EnvReaderHTTPListen)); raw != "" {
		cfg.HTTPListen = raw
	}
	if raw := strings.TrimSpace(getenv(EnvReaderGRPCListen)); raw != "" {
		cfg.GRPCListen = raw
	}
	if raw := strings.TrimSpace(getenv(EnvReaderMetricsListen)); raw != "" {
		cfg.MetricsListen = raw
	}
	if raw := strings.TrimSpace(getenv(EnvReaderLogLevel)); raw != "" {
		cfg.LogLevel = raw
	}
	if raw := strings.TrimSpace(getenv(EnvReaderManifestPoll)); raw != "" {
		millis, err := strconv.Atoi(raw)
		if err != nil || millis <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: got %q", EnvReaderManifestPoll, raw))
		} else {
			cfg.ManifestPollInterval = time.Duration(millis) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(getenv(EnvReaderBlockCacheMB)); raw != "" {
		value, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvReaderBlockCacheMB, err))
		} else {
			cfg.BlockCacheSizeMB = value
		}
	}
	if raw := strings.TrimSpace(getenv(EnvReaderRangeReadKB)); raw != "" {
		value, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvReaderRangeReadKB, err))
		} else {
			cfg.RangeReadMinSSTKB = value
		}
	}

	if len(errs) > 0 {
		return ReaderConfig{}, errors.New(strings.Join(errs, "; "))
	}

	return cfg, nil
}

func (c ReaderConfig) EffectiveManifestPollInterval() time.Duration {
	if c.ManifestPollInterval > 0 {
		return c.ManifestPollInterval
	}
	return defaultReaderManifestPoll
}

func (c ReaderConfig) EffectiveBlockCacheSizeBytes() int64 {
	sizeMB := c.BlockCacheSizeMB
	if sizeMB <= 0 {
		sizeMB = defaultReaderBlockCacheMB
	}
	return int64(sizeMB) << 20
}

func (c ReaderConfig) EffectiveRangeReadMinSSTSizeBytes() int64 {
	sizeKB := c.RangeReadMinSSTKB
	if sizeKB <= 0 {
		sizeKB = defaultReaderRangeReadKB
	}
	return int64(sizeKB) << 10
}
