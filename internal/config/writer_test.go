package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadWriterConfigDefaultsWithExplicitPartition(t *testing.T) {
	t.Parallel()

	cfg, err := LoadWriterConfig(func(key string) string {
		switch key {
		case EnvWriterBucketURL:
			return "s3://eventlake"
		case EnvWriterNamespace:
			return "order-events"
		case EnvWriterPartition:
			return "3"
		case EnvWriterWALDir:
			return "/var/lib/eventlake/wal"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("LoadWriterConfig() error = %v", err)
	}

	if cfg.BucketURL != "s3://eventlake" {
		t.Fatalf("BucketURL = %q, want %q", cfg.BucketURL, "s3://eventlake")
	}
	if cfg.Namespace != "order-events" {
		t.Fatalf("Namespace = %q, want %q", cfg.Namespace, "order-events")
	}
	if cfg.Partition != 3 {
		t.Fatalf("Partition = %d, want %d", cfg.Partition, 3)
	}
	if cfg.GRPCListen != defaultWriterGRPCListen {
		t.Fatalf("GRPCListen = %q, want %q", cfg.GRPCListen, defaultWriterGRPCListen)
	}
	if cfg.MetricsListen != defaultWriterMetricsListen {
		t.Fatalf("MetricsListen = %q, want %q", cfg.MetricsListen, defaultWriterMetricsListen)
	}
	if cfg.MemtableSizeMB != defaultWriterMemtableSizeMB {
		t.Fatalf("MemtableSizeMB = %d, want %d", cfg.MemtableSizeMB, defaultWriterMemtableSizeMB)
	}
	if cfg.FlushInterval != defaultWriterFlushInterval {
		t.Fatalf("FlushInterval = %s, want %s", cfg.FlushInterval, defaultWriterFlushInterval)
	}
	if cfg.LogLevel != defaultWriterLogLevel {
		t.Fatalf("LogLevel = %q, want %q", cfg.LogLevel, defaultWriterLogLevel)
	}
	if cfg.WALDir != "/var/lib/eventlake/wal" {
		t.Fatalf("WALDir = %q, want %q", cfg.WALDir, "/var/lib/eventlake/wal")
	}
	if cfg.WALSegmentMB != defaultWriterWALSegmentMB {
		t.Fatalf("WALSegmentMB = %d, want %d", cfg.WALSegmentMB, defaultWriterWALSegmentMB)
	}
	if cfg.WALBytesSync != defaultWriterWALBytesSync {
		t.Fatalf("WALBytesSync = %d, want %d", cfg.WALBytesSync, defaultWriterWALBytesSync)
	}
	if cfg.WALMaxSync != defaultWriterWALMaxSync {
		t.Fatalf("WALMaxSync = %s, want %s", cfg.WALMaxSync, defaultWriterWALMaxSync)
	}
	if cfg.WALMSyncWrite {
		t.Fatal("WALMSyncWrite = true, want false")
	}
	if cfg.StoragePrefix() != "order-events/p003/" {
		t.Fatalf("StoragePrefix() = %q, want %q", cfg.StoragePrefix(), "order-events/p003/")
	}
}

func TestLoadWriterConfigDerivesPartitionFromHostname(t *testing.T) {
	t.Parallel()

	cfg, err := LoadWriterConfig(func(key string) string {
		switch key {
		case EnvWriterBucketURL:
			return "s3://eventlake"
		case EnvWriterNamespace:
			return "order-events"
		case EnvWriterWALDir:
			return "/var/lib/eventlake/wal"
		case envHostname:
			return "el-order-events-4"
		case EnvWriterFlushInterval:
			return "250"
		case EnvWriterMemtableSizeMB:
			return "16"
		case EnvWriterWALSegmentMB:
			return "32"
		case EnvWriterWALBytesSync:
			return "2097152"
		case EnvWriterWALMaxSyncMS:
			return "75"
		case EnvWriterWALMSyncWrite:
			return "true"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("LoadWriterConfig() error = %v", err)
	}

	if cfg.Partition != 4 {
		t.Fatalf("Partition = %d, want %d", cfg.Partition, 4)
	}
	if cfg.Namespace != "order-events" {
		t.Fatalf("Namespace = %q, want %q", cfg.Namespace, "order-events")
	}
	if cfg.MemtableSizeMB != 16 {
		t.Fatalf("MemtableSizeMB = %d, want %d", cfg.MemtableSizeMB, 16)
	}
	if cfg.FlushInterval != 250*time.Millisecond {
		t.Fatalf("FlushInterval = %s, want %s", cfg.FlushInterval, 250*time.Millisecond)
	}
	if cfg.WALSegmentMB != 32 {
		t.Fatalf("WALSegmentMB = %d, want %d", cfg.WALSegmentMB, 32)
	}
	if cfg.WALBytesSync != 2*1024*1024 {
		t.Fatalf("WALBytesSync = %d, want %d", cfg.WALBytesSync, 2*1024*1024)
	}
	if cfg.WALMaxSync != 75*time.Millisecond {
		t.Fatalf("WALMaxSync = %s, want %s", cfg.WALMaxSync, 75*time.Millisecond)
	}
	if !cfg.WALMSyncWrite {
		t.Fatal("WALMSyncWrite = false, want true")
	}
}

func TestLoadWriterConfigValidationErrors(t *testing.T) {
	t.Parallel()

	_, err := LoadWriterConfig(func(key string) string {
		switch key {
		case EnvWriterPartition:
			return "-1"
		case EnvWriterMemtableSizeMB:
			return "0"
		case EnvWriterFlushInterval:
			return "abc"
		case EnvWriterWALSegmentMB:
			return "0"
		case EnvWriterWALBytesSync:
			return "-1"
		case EnvWriterWALMaxSyncMS:
			return "abc"
		case EnvWriterWALMSyncWrite:
			return "not-bool"
		case envHostname:
			return "writer"
		default:
			return ""
		}
	})
	if err == nil {
		t.Fatal("LoadWriterConfig() error = nil, want validation error")
	}

	errText := err.Error()
	wantSubstrings := []string{
		EnvWriterBucketURL + " is required",
		EnvWriterNamespace + " is required",
		EnvWriterWALDir + " is required",
		EnvWriterPartition + " must be a non-negative integer",
		EnvWriterMemtableSizeMB + " must be a positive integer",
		EnvWriterFlushInterval + " must be a positive integer",
		EnvWriterWALSegmentMB + " must be a positive integer",
		EnvWriterWALBytesSync + " must be a positive integer",
		EnvWriterWALMaxSyncMS + " must be a positive integer",
		EnvWriterWALMSyncWrite + " must be a boolean",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(errText, want) {
			t.Fatalf("validation error %q does not contain %q", errText, want)
		}
	}
}

func TestPartitionFromHostname(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostname string
		want     int
		wantErr  string
	}{
		{name: "valid", hostname: "writer-7", want: 7},
		{name: "missing ordinal", hostname: "writer", wantErr: "does not end with a StatefulSet ordinal"},
		{name: "non numeric", hostname: "writer-x", wantErr: "does not end with a valid StatefulSet ordinal"},
		{name: "empty", hostname: "", wantErr: envHostname + " is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := partitionFromHostname(tt.hostname)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("partitionFromHostname(%q) error = %v, want substring %q", tt.hostname, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("partitionFromHostname(%q) error = %v", tt.hostname, err)
			}
			if got != tt.want {
				t.Fatalf("partitionFromHostname(%q) = %d, want %d", tt.hostname, got, tt.want)
			}
		})
	}
}
