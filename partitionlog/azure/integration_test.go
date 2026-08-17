package azure_test

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	plazure "github.com/ankur-anand/unijord/partitionlog/azure"
	"github.com/ankur-anand/unijord/partitionlog/internal/lifecycletest"
)

const defaultAzuriteConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

var runIntegration = flag.Bool("integration", false, "run integration tests against local object stores")

func TestAzuriteLifecycleConformance(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local Azurite")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	lifecycletest.Run(t, ctx, newAzuriteStore(t, ctx), lifecycletest.Config{Partition: 703})
}

func TestAzuriteLifecycleSoak(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against local Azurite")
	}
	if os.Getenv(lifecycletest.SoakEnvironment) == "" {
		t.Skipf("set %s to run the lifecycle soak", lifecycletest.SoakEnvironment)
	}
	lifecycletest.RunSoak(t, context.Background(), newAzuriteStore(t, context.Background()), 30_000)
}

func newAzuriteStore(t testing.TB, ctx context.Context) *plazure.Store {
	t.Helper()
	containerName := getenv("PARTITIONLOG_AZURITE_CONTAINER", "partitionlog-lifecycle-it")
	connectionString := getenv("PARTITIONLOG_AZURITE_CONNECTION_STRING", getenv("CATALOG_BLOB_AZURITE_CONNECTION_STRING", defaultAzuriteConnectionString))
	client, err := container.NewClientFromConnectionString(connectionString, containerName, &container.ClientOptions{
		ClientOptions: policy.ClientOptions{
			PerCallPolicies: []policy.Policy{azuriteVersionPolicy{}},
		},
	})
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error = %v", err)
	}
	if _, err := client.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Fatalf("Create(container=%q) error = %v", containerName, err)
	}
	store, err := plazure.New(plazure.Options{
		Container: client,
		Prefix:    integrationPrefix(t, "azurite"), StreamID: "integration/lifecycle",
	})
	if err != nil {
		t.Fatalf("azure.New() error = %v", err)
	}
	return store
}

type azuriteVersionPolicy struct{}

func (azuriteVersionPolicy) Do(request *policy.Request) (*http.Response, error) {
	delete(request.Raw().Header, "X-Ms-Version")
	request.Raw().Header["x-ms-version"] = []string{"2023-11-03"}
	return request.Next()
}

func integrationPrefix(t testing.TB, provider string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("integration/lifecycle/%s/%d/%s", provider, time.Now().UnixNano(), name)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
