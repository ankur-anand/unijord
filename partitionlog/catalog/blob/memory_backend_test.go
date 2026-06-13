package blob_test

import (
	"context"
	"testing"

	blobcatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/backendtest"
)

func TestMemoryBackendConformance(t *testing.T) {
	t.Parallel()

	backendtest.Run(t, backendtest.Config{
		NewBackend: func(t testing.TB) blobcatalog.Backend {
			t.Helper()
			return blobcatalog.NewMemoryBackend()
		},
	})
}

func TestMemoryBackendPutGetAndDefensiveCopy(t *testing.T) {
	t.Parallel()

	backend := blobcatalog.NewMemoryBackend()
	body := []byte("one")
	created, err := backend.Put(context.Background(), "pages/a.json", body)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if created.Key != "pages/a.json" || string(created.Body) != "one" || created.Token == "" {
		t.Fatalf("Put() object = %+v", created)
	}

	body[0] = 'x'
	got, err := backend.Get(context.Background(), "pages/a.json")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(got.Body) != "one" {
		t.Fatalf("stored body after input mutation = %q, want one", got.Body)
	}

	got.Body[0] = 'y'
	again, err := backend.Get(context.Background(), "pages/a.json")
	if err != nil {
		t.Fatalf("Get(again) error = %v", err)
	}
	if string(again.Body) != "one" {
		t.Fatalf("stored body after returned mutation = %q, want one", again.Body)
	}
}
