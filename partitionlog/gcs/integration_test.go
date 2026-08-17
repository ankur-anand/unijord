package gcs_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/gcs"
	"github.com/ankur-anand/unijord/partitionlog/internal/lifecycletest"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

var runIntegration = flag.Bool("integration", false, "run integration tests against local object stores")

func TestFakeGCSLifecycleConformance(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against fake GCS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	lifecycletest.Run(t, ctx, newFakeGCSStore(t), lifecycletest.Config{Partition: 702})
}

func TestFakeGCSLifecycleSoak(t *testing.T) {
	if !*runIntegration {
		t.Skip("set -integration to run against fake GCS")
	}
	if os.Getenv(lifecycletest.SoakEnvironment) == "" {
		t.Skipf("set %s to run the lifecycle soak", lifecycletest.SoakEnvironment)
	}
	lifecycletest.RunSoak(t, context.Background(), newFakeGCSStore(t), 20_000)
}

func newFakeGCSStore(t testing.TB) *gcs.Store {
	t.Helper()
	const bucket = "partitionlog-lifecycle-it"
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		t.Fatalf("NewServerWithOptions() error = %v", err)
	}
	t.Cleanup(server.Stop)
	server.CreateBucket(bucket)
	client := server.Client()
	t.Cleanup(func() { _ = client.Close() })
	store, err := gcs.New(gcs.Options{
		Client: client, Bucket: bucket,
		Prefix: integrationPrefix(t, "fake-gcs"), StreamID: "integration/lifecycle",
	})
	if err != nil {
		t.Fatalf("gcs.New() error = %v", err)
	}
	return store
}

func integrationPrefix(t testing.TB, provider string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return fmt.Sprintf("integration/lifecycle/%s/%d/%s", provider, time.Now().UnixNano(), name)
}
