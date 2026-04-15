package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	readerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/reader/v1"
	"github.com/ankur-anand/unijord/internal/config"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcHealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

var gatewayMarshaler = &runtime.JSONPb{
	MarshalOptions: protojson.MarshalOptions{
		EmitUnpopulated: true,
	},
	UnmarshalOptions: protojson.UnmarshalOptions{
		DiscardUnknown: true,
	},
}

// Run starts the reader process with the provided config and blocks until the
// context is canceled or a serving error occurs.
func Run(ctx context.Context, logger *slog.Logger, cfg config.ReaderConfig, backend Backend) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if backend == nil {
		return errors.New("reader backend is required")
	}
	defer func() {
		_ = backend.Close()
	}()

	service, err := NewService(backend)
	if err != nil {
		return err
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		return fmt.Errorf("listen on gRPC address %q: %w", cfg.GRPCListen, err)
	}
	defer func() {
		_ = grpcListener.Close()
	}()

	grpcServer := grpc.NewServer()
	readerv1.RegisterReaderServiceServer(grpcServer, service)

	healthServer := grpcHealth.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(readerv1.ReaderService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	go func() {
		logger.Info("reader gRPC server listening",
			"addr", cfg.GRPCListen,
			"namespace", cfg.Namespace,
			"partitions", cfg.Partitions,
		)
	}()

	errCh := make(chan error, 3)
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("serve gRPC: %w", err)
		}
	}()

	gatewayMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, gatewayMarshaler),
	)
	if err := readerv1.RegisterReaderServiceHandlerFromEndpoint(ctx, gatewayMux, grpcDialTarget(cfg.GRPCListen), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}); err != nil {
		grpcServer.Stop()
		return fmt.Errorf("register reader gateway: %w", err)
	}

	httpServer := &http.Server{
		Addr:              cfg.HTTPListen,
		Handler:           gatewayMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := &http.Server{
		Addr:              cfg.MetricsListen,
		Handler:           metricsMux(cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("reader HTTP server listening", "addr", cfg.HTTPListen)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve HTTP: %w", err)
		}
	}()

	go func() {
		logger.Info("reader metrics server listening", "addr", cfg.MetricsListen)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve metrics: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("reader shutting down", "reason", ctx.Err())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(readerv1.ReaderService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown metrics server: %w", err)
		}
		return nil
	case err := <-errCh:
		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(readerv1.ReaderService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_NOT_SERVING)
		grpcServer.Stop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		_ = metricsServer.Shutdown(shutdownCtx)
		return err
	}
}

func grpcDialTarget(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return listenAddr
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func metricsMux(cfg config.ReaderConfig) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w,
			"# reader config\neventlake_reader_info{namespace=%q,partitions=%q,manifest_poll_interval_ms=%q,block_cache_size_mb=%q,range_read_min_sst_size_kb=%q} 1\n",
			cfg.Namespace,
			fmt.Sprintf("%d", cfg.Partitions),
			fmt.Sprintf("%d", cfg.EffectiveManifestPollInterval()/time.Millisecond),
			fmt.Sprintf("%d", cfg.EffectiveBlockCacheSizeBytes()>>20),
			fmt.Sprintf("%d", cfg.EffectiveRangeReadMinSSTSizeBytes()>>10),
		)
	})
	return mux
}

func ParseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
