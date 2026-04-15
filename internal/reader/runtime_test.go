package reader

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPHandlerGetPartitionHead(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{
		headResult: PartitionHeadResult{
			Partition:        2,
			HighWatermarkLSN: 15,
		},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	handler := newHTTPHandler(service, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("gateway handler should not be called for /head")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/2/head", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body := resp.Body.String()
	if !strings.Contains(body, `"partition":2`) {
		t.Fatalf("body = %q, want partition field", body)
	}
	if !strings.Contains(body, `"highWatermarkLsn":"15"`) {
		t.Fatalf("body = %q, want highWatermarkLsn field", body)
	}
}

func TestHTTPHandlerGetPartitionHeadNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{headErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	handler := newHTTPHandler(service, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("gateway handler should not be called for /head")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/99/head", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNotFound)
	}
	if !strings.Contains(resp.Body.String(), `"message":"get partition head failed:`) {
		t.Fatalf("body = %q, want error message", resp.Body.String())
	}
}

func TestHTTPHandlerFallsBackToGateway(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	called := false
	handler := newHTTPHandler(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/partitions/2/consume", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if !called {
		t.Fatal("gateway handler was not called")
	}
	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusNoContent)
	}
}
