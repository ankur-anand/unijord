package azure

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

const azuriteConnectionEnv = "ISLEDB_AZURITE_CONNECTION_STRING"

// liveHarness runs against Azurite (or any account named by the connection
// string) in a uniquely named container that is deleted afterwards. There is
// no fault injection: those subtests report as skipped.
func liveHarness(t *testing.T) storetest.Harness {
	connection := os.Getenv(azuriteConnectionEnv)
	if connection == "" {
		t.Skipf("%s is not set", azuriteConnectionEnv)
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	name := "isledb-conformance-" + hex.EncodeToString(suffix[:])
	open := func() *container.Client {
		client, err := container.NewClientFromConnectionString(connection, name, nil)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	client := open()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := client.Create(ctx, nil); err != nil {
		t.Fatalf("create container %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := client.Delete(ctx, nil); err != nil {
			t.Errorf("delete container %s: %v", name, err)
		}
	})
	store, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: "conformance/",
		Reopen: func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened, err := New(open())
			if err != nil {
				t.Fatal(err)
			}
			return reopened, reopened
		},
		Replace: func(t *testing.T, key string, body []byte) {
			// An unconditional Put Blob, as a foreign writer would issue.
			_, err := client.NewBlockBlobClient(key).Upload(context.Background(), streaming.NopCloser(bytes.NewReader(body)), nil)
			if err != nil {
				t.Fatal(err)
			}
		},
	}
}

func TestLiveMetadataConformance(t *testing.T) { storetest.MetadataSuite(t, liveHarness(t)) }
func TestLiveRunConformance(t *testing.T)      { storetest.RunSuite(t, liveHarness(t)) }
