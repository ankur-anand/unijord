package azure

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blob/sink/internal/sinktest"
)

const defaultAzuriteConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

var runIntegration = flag.Bool("integration", false, "run live integration tests against local object stores")

func TestLiveAzuriteStoreAndSegmentWriter(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local Azurite")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	containerName := getenv("BLOB_SINK_AZURITE_CONTAINER", "blob-sink-it")
	connString := getenv("BLOB_SINK_AZURITE_CONNECTION_STRING", defaultAzuriteConnectionString)
	client, err := container.NewClientFromConnectionString(connString, containerName, nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error = %v", err)
	}
	if _, err := client.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Fatalf("Create(container=%q) error = %v", containerName, err)
	}
	store, err := NewStore(client)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	prefix := livePrefix(t, "azurite")
	read := func(ctx context.Context, key string) (sinktest.Object, error) {
		blob := client.NewBlobClient(key)
		download, err := blob.DownloadStream(ctx, nil)
		if err != nil {
			return sinktest.Object{}, err
		}
		body, err := sinktest.ReadAll(download.Body)
		if err != nil {
			return sinktest.Object{}, err
		}
		props, err := blob.GetProperties(ctx, nil)
		if err != nil {
			return sinktest.Object{}, err
		}
		size := uint64(len(body))
		if props.ContentLength != nil && *props.ContentLength >= 0 {
			size = uint64(*props.ContentLength)
		}
		contentType := ""
		if props.ContentType != nil {
			contentType = *props.ContentType
		}
		return sinktest.Object{
			Body:        body,
			ContentType: contentType,
			SizeBytes:   size,
		}, nil
	}

	sinktest.RunMultipartStore(t, store, prefix, read)
	sinktest.RunSegmentWriter(t, store, prefix, read)
}

func livePrefix(t *testing.T, provider string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("integration/%s/%d/%s", provider, time.Now().UnixNano(), name)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
