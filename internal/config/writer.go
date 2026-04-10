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
	EnvWriterBucketURL      = "EVENTLAKE_BUCKET_URL"
	EnvWriterNamespace      = "EVENTLAKE_NAMESPACE"
	EnvWriterPartition      = "EVENTLAKE_PARTITION"
	EnvWriterGRPCListen     = "EVENTLAKE_GRPC_LISTEN"
	EnvWriterMetricsListen  = "EVENTLAKE_METRICS_LISTEN"
	EnvWriterMemtableSizeMB = "EVENTLAKE_MEMTABLE_SIZE_MB"
	EnvWriterFlushInterval  = "EVENTLAKE_FLUSH_INTERVAL_MS"
	EnvWriterLogLevel       = "EVENTLAKE_LOG_LEVEL"
	EnvWriterWALDir         = "EVENTLAKE_WAL_DIR"
	EnvWriterWALSegmentMB   = "EVENTLAKE_WAL_SEGMENT_SIZE_MB"
	EnvWriterWALBytesSync   = "EVENTLAKE_WAL_BYTES_PER_SYNC"
	EnvWriterWALMaxSyncMS   = "EVENTLAKE_WAL_MAX_SYNC_INTERVAL_MS"
	EnvWriterWALMSyncWrite  = "EVENTLAKE_WAL_MSYNC_EVERY_WRITE"

	envHostname = "HOSTNAME"

	defaultWriterGRPCListen     = ":9090"
	defaultWriterMetricsListen  = ":9100"
	defaultWriterMemtableSizeMB = 4
	defaultWriterFlushInterval  = time.Second
	defaultWriterLogLevel       = "info"
	defaultWriterWALSegmentMB   = 16
	defaultWriterWALBytesSync   = 1 << 20
	defaultWriterWALMaxSync     = 50 * time.Millisecond
)

// WriterConfig is the fully resolved runtime configuration for one writer pod.
type WriterConfig struct {
	BucketURL      string
	Namespace      string
	Partition      int
	GRPCListen     string
	MetricsListen  string
	MemtableSizeMB int
	FlushInterval  time.Duration
	LogLevel       string
	WALDir         string
	WALSegmentMB   int
	WALBytesSync   int64
	WALMaxSync     time.Duration
	WALMSyncWrite  bool
}

// StoragePrefix returns the object-store prefix owned by this writer.
func (c WriterConfig) StoragePrefix() string {
	return fmt.Sprintf("%s/p%03d/", c.Namespace, c.Partition)
}

// LoadWriterConfigFromEnv resolves WriterConfig from the current process
// environment.
func LoadWriterConfigFromEnv() (WriterConfig, error) {
	return LoadWriterConfig(os.Getenv)
}

// LoadWriterConfig resolves WriterConfig using the provided environment lookup.
func LoadWriterConfig(getenv func(string) string) (WriterConfig, error) {
	cfg := WriterConfig{
		GRPCListen:     defaultWriterGRPCListen,
		MetricsListen:  defaultWriterMetricsListen,
		MemtableSizeMB: defaultWriterMemtableSizeMB,
		FlushInterval:  defaultWriterFlushInterval,
		LogLevel:       defaultWriterLogLevel,
		WALSegmentMB:   defaultWriterWALSegmentMB,
		WALBytesSync:   defaultWriterWALBytesSync,
		WALMaxSync:     defaultWriterWALMaxSync,
	}

	var errs []string

	cfg.BucketURL = strings.TrimSpace(getenv(EnvWriterBucketURL))
	if cfg.BucketURL == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvWriterBucketURL))
	}

	cfg.Namespace = strings.TrimSpace(getenv(EnvWriterNamespace))
	if cfg.Namespace == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvWriterNamespace))
	}

	cfg.WALDir = strings.TrimSpace(getenv(EnvWriterWALDir))
	if cfg.WALDir == "" {
		errs = append(errs, fmt.Sprintf("%s is required", EnvWriterWALDir))
	}

	if raw := strings.TrimSpace(getenv(EnvWriterPartition)); raw != "" {
		partition, err := parseNonNegativeInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a non-negative integer: %v", EnvWriterPartition, err))
		} else {
			cfg.Partition = partition
		}
	} else {
		partition, err := partitionFromHostname(strings.TrimSpace(getenv(envHostname)))
		if err != nil {
			errs = append(errs, err.Error())
		} else {
			cfg.Partition = partition
		}
	}

	if raw := strings.TrimSpace(getenv(EnvWriterGRPCListen)); raw != "" {
		cfg.GRPCListen = raw
	}
	if raw := strings.TrimSpace(getenv(EnvWriterMetricsListen)); raw != "" {
		cfg.MetricsListen = raw
	}
	if raw := strings.TrimSpace(getenv(EnvWriterLogLevel)); raw != "" {
		cfg.LogLevel = raw
	}
	if raw := strings.TrimSpace(getenv(EnvWriterMemtableSizeMB)); raw != "" {
		size, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvWriterMemtableSizeMB, err))
		} else {
			cfg.MemtableSizeMB = size
		}
	}
	if raw := strings.TrimSpace(getenv(EnvWriterFlushInterval)); raw != "" {
		millis, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvWriterFlushInterval, err))
		} else {
			cfg.FlushInterval = time.Duration(millis) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(getenv(EnvWriterWALSegmentMB)); raw != "" {
		size, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvWriterWALSegmentMB, err))
		} else {
			cfg.WALSegmentMB = size
		}
	}
	if raw := strings.TrimSpace(getenv(EnvWriterWALBytesSync)); raw != "" {
		bytes, err := parsePositiveInt64(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvWriterWALBytesSync, err))
		} else {
			cfg.WALBytesSync = bytes
		}
	}
	if raw := strings.TrimSpace(getenv(EnvWriterWALMaxSyncMS)); raw != "" {
		millis, err := parsePositiveInt(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer: %v", EnvWriterWALMaxSyncMS, err))
		} else {
			cfg.WALMaxSync = time.Duration(millis) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(getenv(EnvWriterWALMSyncWrite)); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s must be a boolean: %v", EnvWriterWALMSyncWrite, err))
		} else {
			cfg.WALMSyncWrite = enabled
		}
	}

	if len(errs) > 0 {
		return WriterConfig{}, errors.New(strings.Join(errs, "; "))
	}
	return cfg, nil
}

func partitionFromHostname(hostname string) (int, error) {
	if hostname == "" {
		return 0, fmt.Errorf("%s is required when %s is not set", envHostname, EnvWriterPartition)
	}
	idx := strings.LastIndexByte(hostname, '-')
	if idx < 0 || idx == len(hostname)-1 {
		return 0, fmt.Errorf("%s=%q does not end with a StatefulSet ordinal", envHostname, hostname)
	}
	partition, err := parseNonNegativeInt(hostname[idx+1:])
	if err != nil {
		return 0, fmt.Errorf("%s=%q does not end with a valid StatefulSet ordinal: %v", envHostname, hostname, err)
	}
	return partition, nil
}

func parseNonNegativeInt(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if value < 0 {
		return 0, fmt.Errorf("got %d", value)
	}
	return value, nil
}

func parsePositiveInt(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("got %d", value)
	}
	return value, nil
}

func parsePositiveInt64(raw string) (int64, error) {
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("got %d", value)
	}
	return value, nil
}
