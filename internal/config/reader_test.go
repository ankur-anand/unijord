package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadReaderConfigDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := LoadReaderConfig(func(key string) string {
		switch key {
		case EnvReaderBucketURL:
			return "file:///tmp/eventlake"
		case EnvReaderNamespace:
			return "order-events"
		case EnvReaderPartitions:
			return "4"
		case EnvReaderCacheDir:
			return "/var/lib/eventlake/cache"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("LoadReaderConfig() error = %v", err)
	}

	if cfg.BucketURL != "file:///tmp/eventlake" {
		t.Fatalf("BucketURL = %q, want %q", cfg.BucketURL, "file:///tmp/eventlake")
	}
	if cfg.Namespace != "order-events" {
		t.Fatalf("Namespace = %q, want %q", cfg.Namespace, "order-events")
	}
	if cfg.Partitions != 4 {
		t.Fatalf("Partitions = %d, want %d", cfg.Partitions, 4)
	}
	if cfg.HTTPListen != defaultReaderHTTPListen {
		t.Fatalf("HTTPListen = %q, want %q", cfg.HTTPListen, defaultReaderHTTPListen)
	}
	if cfg.GRPCListen != defaultReaderGRPCListen {
		t.Fatalf("GRPCListen = %q, want %q", cfg.GRPCListen, defaultReaderGRPCListen)
	}
	if cfg.MetricsListen != defaultReaderMetricsListen {
		t.Fatalf("MetricsListen = %q, want %q", cfg.MetricsListen, defaultReaderMetricsListen)
	}
	if cfg.ManifestPollInterval != defaultReaderManifestPoll {
		t.Fatalf("ManifestPollInterval = %s, want %s", cfg.ManifestPollInterval, defaultReaderManifestPoll)
	}
	if cfg.BlockCacheSizeMB != defaultReaderBlockCacheMB {
		t.Fatalf("BlockCacheSizeMB = %d, want %d", cfg.BlockCacheSizeMB, defaultReaderBlockCacheMB)
	}
	if cfg.RangeReadMinSSTKB != defaultReaderRangeReadKB {
		t.Fatalf("RangeReadMinSSTKB = %d, want %d", cfg.RangeReadMinSSTKB, defaultReaderRangeReadKB)
	}
	if cfg.LogLevel != defaultReaderLogLevel {
		t.Fatalf("LogLevel = %q, want %q", cfg.LogLevel, defaultReaderLogLevel)
	}
	if cfg.PartitionPrefix(3) != "order-events/p003/" {
		t.Fatalf("PartitionPrefix(3) = %q, want %q", cfg.PartitionPrefix(3), "order-events/p003/")
	}
	if cfg.PartitionCacheDir(2) != "/var/lib/eventlake/cache/p002" {
		t.Fatalf("PartitionCacheDir(2) = %q, want %q", cfg.PartitionCacheDir(2), "/var/lib/eventlake/cache/p002")
	}
}

func TestLoadReaderConfigOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := LoadReaderConfig(func(key string) string {
		switch key {
		case EnvReaderBucketURL:
			return "s3://eventlake"
		case EnvReaderNamespace:
			return "payments"
		case EnvReaderPartitions:
			return "8"
		case EnvReaderCacheDir:
			return "/tmp/reader"
		case EnvReaderHTTPListen:
			return ":18090"
		case EnvReaderGRPCListen:
			return ":19090"
		case EnvReaderMetricsListen:
			return ":19100"
		case EnvReaderManifestPoll:
			return "250"
		case EnvReaderBlockCacheMB:
			return "32"
		case EnvReaderRangeReadKB:
			return "64"
		case EnvReaderLogLevel:
			return "debug"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("LoadReaderConfig() error = %v", err)
	}

	if cfg.HTTPListen != ":18090" {
		t.Fatalf("HTTPListen = %q, want %q", cfg.HTTPListen, ":18090")
	}
	if cfg.GRPCListen != ":19090" {
		t.Fatalf("GRPCListen = %q, want %q", cfg.GRPCListen, ":19090")
	}
	if cfg.MetricsListen != ":19100" {
		t.Fatalf("MetricsListen = %q, want %q", cfg.MetricsListen, ":19100")
	}
	if cfg.ManifestPollInterval != 250*time.Millisecond {
		t.Fatalf("ManifestPollInterval = %s, want %s", cfg.ManifestPollInterval, 250*time.Millisecond)
	}
	if cfg.BlockCacheSizeMB != 32 {
		t.Fatalf("BlockCacheSizeMB = %d, want %d", cfg.BlockCacheSizeMB, 32)
	}
	if cfg.RangeReadMinSSTKB != 64 {
		t.Fatalf("RangeReadMinSSTKB = %d, want %d", cfg.RangeReadMinSSTKB, 64)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
}

func TestLoadReaderConfigValidationErrors(t *testing.T) {
	t.Parallel()

	_, err := LoadReaderConfig(func(key string) string {
		switch key {
		case EnvReaderPartitions:
			return "0"
		case EnvReaderManifestPoll:
			return "abc"
		case EnvReaderBlockCacheMB:
			return "-1"
		case EnvReaderRangeReadKB:
			return "0"
		default:
			return ""
		}
	})
	if err == nil {
		t.Fatal("LoadReaderConfig() error = nil, want validation error")
	}

	errText := err.Error()
	wantSubstrings := []string{
		EnvReaderBucketURL + " is required",
		EnvReaderNamespace + " is required",
		EnvReaderCacheDir + " is required",
		EnvReaderPartitions + " must be a positive integer",
		EnvReaderManifestPoll + " must be a positive integer",
		EnvReaderBlockCacheMB + " must be a positive integer",
		EnvReaderRangeReadKB + " must be a positive integer",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(errText, want) {
			t.Fatalf("validation error %q does not contain %q", errText, want)
		}
	}
}
