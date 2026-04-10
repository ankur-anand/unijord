package writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	writerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/writer/v1"
	"github.com/ankur-anand/unijord/internal/config"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Run starts the writer process with the provided config and blocks until the
// context is canceled or a serving error occurs.
func Run(ctx context.Context, logger *slog.Logger, cfg config.WriterConfig, appender Appender) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if appender == nil {
		return errors.New("writer appender is required")
	}
	defer func() {
		_ = appender.Close()
	}()

	service, err := NewService(appender)
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
	writerv1.RegisterWriterServiceServer(grpcServer, service)

	healthServer := grpcHealth.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(writerv1.WriterService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	metricsServer := &http.Server{
		Addr:              cfg.MetricsListen,
		Handler:           metricsMux(cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)

	go func() {
		logger.Info("writer gRPC server listening",
			"addr", cfg.GRPCListen,
			"namespace", cfg.Namespace,
			"partition", cfg.Partition,
			"prefix", cfg.StoragePrefix(),
		)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("serve gRPC: %w", err)
		}
	}()

	go func() {
		logger.Info("writer HTTP server listening", "addr", cfg.MetricsListen)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve HTTP: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("writer shutting down", "reason", ctx.Err())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(writerv1.WriterService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		return nil
	case err := <-errCh:
		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus(writerv1.WriterService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_NOT_SERVING)
		grpcServer.Stop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
		return err
	}
}

func metricsMux(cfg config.WriterConfig) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w,
			"# writer config\neventlake_writer_info{namespace=%q,partition=%q,prefix=%q} 1\n",
			cfg.Namespace,
			fmt.Sprintf("%d", cfg.Partition),
			cfg.StoragePrefix(),
		)
	})
	return mux
}
