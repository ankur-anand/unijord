package reader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	readerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/reader/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

func TestGatewayMuxGetPartitionHead(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{
		headResult: PartitionHeadResult{
			Partition:        0,
			HighWatermarkLSN: 15,
		},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	mux := newGatewayTestMux(t, service)

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/0/head", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body := resp.Body.String()
	if !strings.Contains(body, `"partition":0`) {
		t.Fatalf("body = %q, want partition field", body)
	}
	if !strings.Contains(body, `"highWatermarkLsn":"15"`) {
		t.Fatalf("body = %q, want highWatermarkLsn field", body)
	}
}

func TestGatewayMuxListPartitionHeads(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{
		listHeadsResult: []PartitionHeadResult{
			{Partition: 0, HighWatermarkLSN: 12},
			{Partition: 1, HighWatermarkLSN: 15},
		},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	mux := newGatewayTestMux(t, service)

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/heads", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body := resp.Body.String()
	if !strings.Contains(body, `"heads":[`) {
		t.Fatalf("body = %q, want heads array", body)
	}
	if !strings.Contains(body, `"partition":0`) || !strings.Contains(body, `"highWatermarkLsn":"12"`) {
		t.Fatalf("body = %q, want first head", body)
	}
	if !strings.Contains(body, `"partition":1`) || !strings.Contains(body, `"highWatermarkLsn":"15"`) {
		t.Fatalf("body = %q, want second head", body)
	}
}

func TestGatewayMuxGetPartitionHeadNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{headErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	mux := newGatewayTestMux(t, service)

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/99/head", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNotFound)
	}
	if !strings.Contains(resp.Body.String(), `"code":5`) {
		t.Fatalf("body = %q, want grpc not found code", resp.Body.String())
	}
}

func TestGatewayMuxListPartitionHeadsNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{listHeadsErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	mux := newGatewayTestMux(t, service)

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/heads", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNotFound)
	}
	if !strings.Contains(resp.Body.String(), `"code":5`) {
		t.Fatalf("body = %q, want grpc not found code", resp.Body.String())
	}
}

func newGatewayTestMux(t *testing.T, service *Service) http.Handler {
	t.Helper()

	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, gatewayMarshaler),
	)
	if err := readerv1.RegisterReaderServiceHandlerServer(context.Background(), mux, service); err != nil {
		t.Fatalf("RegisterReaderServiceHandlerServer() error = %v", err)
	}
	return mux
}
