package bench

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/ankur-anand/unijord/internal/blobstore"
	blobazure "github.com/ankur-anand/unijord/internal/blobstore/azure"
	blobgcs "github.com/ankur-anand/unijord/internal/blobstore/gcs"
	blobs3 "github.com/ankur-anand/unijord/internal/blobstore/s3"
	"github.com/ankur-anand/unijord/partitionlog"
	plazure "github.com/ankur-anand/unijord/partitionlog/azure"
	"github.com/ankur-anand/unijord/partitionlog/blob/lifecycle"
	plgcs "github.com/ankur-anand/unijord/partitionlog/gcs"
	pls3 "github.com/ankur-anand/unijord/partitionlog/s3"
)

// StoreHandle is a complete provider store plus its lifecycle reclaimer
// constructor, which every provider package exposes with the same shape.
type StoreHandle interface {
	partitionlog.Store
	NewReclaimer(opts lifecycle.Options) (*lifecycle.Reclaimer, error)
}

// Provider builds partitionlog stores and raw backends for one object store.
// Scenarios never touch an SDK directly; everything they need comes from here.
type Provider interface {
	// Name is the baseline key: minio, s3, azurite, azure, fake-gcs, gcs.
	Name() string
	// Emulator reports whether this is a local emulator rather than a real
	// cloud endpoint. Results from the two are never compared to each other.
	Emulator() bool
	// Store wires a complete partitionlog store under prefix for one stream.
	// leafLimit and indexLimit set the catalog page fan-out; zero keeps the
	// catalog defaults.
	Store(ctx context.Context, prefix, streamID string, leafLimit, indexLimit int) (StoreHandle, error)
	// Backend is the raw conditional-object protocol, used for inventory
	// listings and the catalog-only scenario.
	Backend() blobstore.Store
	// Cleanup deletes every object under prefix and returns the count.
	Cleanup(ctx context.Context, prefix string) (int, error)
	Close() error
}

// ProviderNames lists every provider OpenProvider accepts.
var ProviderNames = []string{"minio", "s3", "azurite", "azure", "fake-gcs", "gcs"}

// OpenProvider resolves a provider from its name and the environment. The
// environment variables are the ones the repository's integration tests use,
// so any machine that can run `make integration` can run the load suite.
//
//	minio     PARTITIONLOG_MINIO_ENDPOINT (http://127.0.0.1:9000), PARTITIONLOG_MINIO_BUCKET (plbench),
//	          PARTITIONLOG_MINIO_ACCESS_KEY / MINIO_ROOT_USER, PARTITIONLOG_MINIO_SECRET_KEY / MINIO_ROOT_PASSWORD
//	s3        default AWS credential chain; PLBENCH_S3_BUCKET (required), AWS_REGION
//	azurite   PARTITIONLOG_AZURITE_CONNECTION_STRING (local default), PARTITIONLOG_AZURITE_CONTAINER (plbench)
//	azure     PLBENCH_AZURE_CONNECTION_STRING (required), PLBENCH_AZURE_CONTAINER (plbench)
//	fake-gcs  in-process fake-gcs-server; nothing to configure
//	gcs       Application Default Credentials; PLBENCH_GCS_BUCKET (required)
func OpenProvider(ctx context.Context, name string) (Provider, error) {
	switch strings.ToLower(name) {
	case "minio":
		return openS3(ctx, "minio", true,
			Getenv("PARTITIONLOG_MINIO_ENDPOINT", "http://127.0.0.1:9000"),
			Getenv("PARTITIONLOG_MINIO_BUCKET", "plbench"),
			Getenv("PARTITIONLOG_MINIO_ACCESS_KEY", Getenv("MINIO_ROOT_USER", "minioadmin")),
			Getenv("PARTITIONLOG_MINIO_SECRET_KEY", Getenv("MINIO_ROOT_PASSWORD", "minioadmin")))
	case "s3":
		bucket := os.Getenv("PLBENCH_S3_BUCKET")
		if bucket == "" {
			return nil, errors.New("bench: PLBENCH_S3_BUCKET is required for provider s3")
		}
		return openS3(ctx, "s3", false, "", bucket, "", "")
	case "azurite":
		return openAzure(ctx, "azurite", true,
			Getenv("PARTITIONLOG_AZURITE_CONNECTION_STRING", Getenv("CATALOG_BLOB_AZURITE_CONNECTION_STRING", defaultAzuriteConnectionString)),
			Getenv("PARTITIONLOG_AZURITE_CONTAINER", "plbench"))
	case "azure":
		conn := os.Getenv("PLBENCH_AZURE_CONNECTION_STRING")
		if conn == "" {
			return nil, errors.New("bench: PLBENCH_AZURE_CONNECTION_STRING is required for provider azure")
		}
		return openAzure(ctx, "azure", false, conn, Getenv("PLBENCH_AZURE_CONTAINER", "plbench"))
	case "fake-gcs":
		return openFakeGCS()
	case "gcs":
		bucket := os.Getenv("PLBENCH_GCS_BUCKET")
		if bucket == "" {
			return nil, errors.New("bench: PLBENCH_GCS_BUCKET is required for provider gcs")
		}
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, err
		}
		return newGCSProvider("gcs", false, client, bucket, nil)
	default:
		return nil, fmt.Errorf("bench: unknown provider %q (want one of %s)", name, strings.Join(ProviderNames, ", "))
	}
}

// Getenv returns the environment value or fallback when unset or empty.
func Getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

const defaultAzuriteConnectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

// ---- S3 / MinIO --------------------------------------------------------------

type s3Provider struct {
	name     string
	emulator bool
	client   *awss3.Client
	bucket   string
	backend  *blobs3.Backend
}

func openS3(ctx context.Context, name string, emulator bool, endpoint, bucket, accessKey, secretKey string) (Provider, error) {
	loaders := []func(*config.LoadOptions) error{config.WithRegion(Getenv("AWS_REGION", "us-east-1"))}
	if accessKey != "" {
		loaders = append(loaders, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loaders...)
	if err != nil {
		return nil, err
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil && !s3BucketExists(err) {
		return nil, fmt.Errorf("bench: create bucket %q: %w", bucket, err)
	}
	backend, err := blobs3.New(client, bucket)
	if err != nil {
		return nil, err
	}
	return &s3Provider{name: name, emulator: emulator, client: client, bucket: bucket, backend: backend}, nil
}

func s3BucketExists(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists"
}

func (p *s3Provider) Name() string             { return p.name }
func (p *s3Provider) Emulator() bool           { return p.emulator }
func (p *s3Provider) Backend() blobstore.Store { return p.backend }
func (p *s3Provider) Close() error             { return nil }

func (p *s3Provider) Store(_ context.Context, prefix, streamID string, leafLimit, indexLimit int) (StoreHandle, error) {
	return pls3.New(pls3.Options{Client: p.client, Bucket: p.bucket, Prefix: prefix, StreamID: streamID, CatalogLeafSegmentLimit: leafLimit, CatalogIndexRefLimit: indexLimit})
}

func (p *s3Provider) Cleanup(ctx context.Context, prefix string) (int, error) {
	return deletePrefix(ctx, p.backend, prefix)
}

// ---- Azure / Azurite ---------------------------------------------------------

type azureProvider struct {
	name      string
	emulator  bool
	container *container.Client
	backend   *blobazure.Backend
}

type azuriteVersionPolicy struct{}

func (azuriteVersionPolicy) Do(request *policy.Request) (*http.Response, error) {
	delete(request.Raw().Header, "X-Ms-Version")
	request.Raw().Header["x-ms-version"] = []string{"2023-11-03"}
	return request.Next()
}

func openAzure(ctx context.Context, name string, emulator bool, connectionString, containerName string) (Provider, error) {
	var opts *container.ClientOptions
	if emulator {
		opts = &container.ClientOptions{ClientOptions: policy.ClientOptions{PerCallPolicies: []policy.Policy{azuriteVersionPolicy{}}}}
	}
	client, err := container.NewClientFromConnectionString(connectionString, containerName, opts)
	if err != nil {
		return nil, err
	}
	if _, err := client.Create(ctx, nil); err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return nil, fmt.Errorf("bench: create container %q: %w", containerName, err)
	}
	backend, err := blobazure.New(client)
	if err != nil {
		return nil, err
	}
	return &azureProvider{name: name, emulator: emulator, container: client, backend: backend}, nil
}

func (p *azureProvider) Name() string             { return p.name }
func (p *azureProvider) Emulator() bool           { return p.emulator }
func (p *azureProvider) Backend() blobstore.Store { return p.backend }
func (p *azureProvider) Close() error             { return nil }

func (p *azureProvider) Store(_ context.Context, prefix, streamID string, leafLimit, indexLimit int) (StoreHandle, error) {
	return plazure.New(plazure.Options{Container: p.container, Prefix: prefix, StreamID: streamID, CatalogLeafSegmentLimit: leafLimit, CatalogIndexRefLimit: indexLimit})
}

func (p *azureProvider) Cleanup(ctx context.Context, prefix string) (int, error) {
	return deletePrefix(ctx, p.backend, prefix)
}

// ---- GCS / fake-gcs ----------------------------------------------------------

type gcsProvider struct {
	name     string
	emulator bool
	client   *storage.Client
	bucket   string
	backend  *blobgcs.Backend
	server   *fakestorage.Server
}

func openFakeGCS() (Provider, error) {
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		return nil, err
	}
	const bucket = "plbench"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})
	return newGCSProvider("fake-gcs", true, server.Client(), bucket, server)
}

func newGCSProvider(name string, emulator bool, client *storage.Client, bucket string, server *fakestorage.Server) (Provider, error) {
	backend, err := blobgcs.New(client, bucket)
	if err != nil {
		return nil, err
	}
	return &gcsProvider{name: name, emulator: emulator, client: client, bucket: bucket, backend: backend, server: server}, nil
}

func (p *gcsProvider) Name() string             { return p.name }
func (p *gcsProvider) Emulator() bool           { return p.emulator }
func (p *gcsProvider) Backend() blobstore.Store { return p.backend }

func (p *gcsProvider) Close() error {
	err := p.client.Close()
	if p.server != nil {
		p.server.Stop()
	}
	return err
}

func (p *gcsProvider) Store(_ context.Context, prefix, streamID string, leafLimit, indexLimit int) (StoreHandle, error) {
	return plgcs.New(plgcs.Options{Client: p.client, Bucket: p.bucket, Prefix: prefix, StreamID: streamID, CatalogLeafSegmentLimit: leafLimit, CatalogIndexRefLimit: indexLimit})
}

func (p *gcsProvider) Cleanup(ctx context.Context, prefix string) (int, error) {
	return deletePrefix(ctx, p.backend, prefix)
}

// ---- shared ------------------------------------------------------------------

type batchDeleter interface {
	DeleteBatch(ctx context.Context, keys []string) []error
}

func deletePrefix(ctx context.Context, backend blobstore.Store, prefix string) (int, error) {
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	deleted := 0
	for {
		page, err := backend.List(ctx, blobstore.ListOptions{Prefix: prefix, Limit: blobstore.MaxListLimit})
		if err != nil {
			return deleted, err
		}
		if len(page.Objects) == 0 {
			return deleted, nil
		}
		keys := make([]string, 0, len(page.Objects))
		for _, o := range page.Objects {
			keys = append(keys, o.Key)
		}
		if bd, ok := backend.(batchDeleter); ok {
			for _, err := range bd.DeleteBatch(ctx, keys) {
				if err != nil && !errors.Is(err, blobstore.ErrObjectNotFound) {
					return deleted, err
				}
			}
		} else {
			for _, key := range keys {
				if err := backend.Delete(ctx, key); err != nil && !errors.Is(err, blobstore.ErrObjectNotFound) {
					return deleted, err
				}
			}
		}
		deleted += len(keys)
		if !page.HasMore && len(page.Objects) < blobstore.MaxListLimit {
			// Re-list from the start: deleting shifts the page window.
			continue
		}
	}
}
