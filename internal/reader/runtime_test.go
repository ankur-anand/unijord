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
			Partition:          0,
			HeadLSN:            15,
			OldestAvailableLSN: 4,
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
	if !strings.Contains(body, `"headLsn":"15"`) {
		t.Fatalf("body = %q, want headLsn field", body)
	}
	if !strings.Contains(body, `"oldestAvailableLsn":"4"`) {
		t.Fatalf("body = %q, want oldestAvailableLsn field", body)
	}
}

func TestGatewayMuxListPartitionHeads(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{
		listHeadsResult: []PartitionHeadResult{
			{Partition: 0, HeadLSN: 12, OldestAvailableLSN: 3},
			{Partition: 1, HeadLSN: 15, OldestAvailableLSN: 8},
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
	if !strings.Contains(body, `"partition":0`) || !strings.Contains(body, `"headLsn":"12"`) || !strings.Contains(body, `"oldestAvailableLsn":"3"`) {
		t.Fatalf("body = %q, want first head", body)
	}
	if !strings.Contains(body, `"partition":1`) || !strings.Contains(body, `"headLsn":"15"`) || !strings.Contains(body, `"oldestAvailableLsn":"8"`) {
		t.Fatalf("body = %q, want second head", body)
	}
}

func TestGatewayMuxConsumePartition(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{
		consumeResult: ConsumeResult{
			Events: []Event{
				{Partition: 0, LSN: 11, Value: []byte("hello")},
			},
			NextStartAfterLSN:  11,
			HeadLSN:            15,
			OldestAvailableLSN: 4,
		},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	mux := newGatewayTestMux(t, service)

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/0/consume?start_after_lsn=10&limit=1", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body := resp.Body.String()
	if !strings.Contains(body, `"nextStartAfterLsn":"11"`) {
		t.Fatalf("body = %q, want nextStartAfterLsn field", body)
	}
	if !strings.Contains(body, `"headLsn":"15"`) {
		t.Fatalf("body = %q, want headLsn field", body)
	}
	if !strings.Contains(body, `"oldestAvailableLsn":"4"`) {
		t.Fatalf("body = %q, want oldestAvailableLsn field", body)
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
