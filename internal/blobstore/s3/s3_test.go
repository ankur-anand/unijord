package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsretry "github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awstypes "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

func TestMapErrorPreconditionFailure(t *testing.T) {
	t.Parallel()

	for _, code := range []string{"PreconditionFailed", "ConditionalRequestConflict"} {
		t.Run(code, func(t *testing.T) {
			err := mapError(&smithy.GenericAPIError{Code: code, Message: "conditional write lost"})
			if !errors.Is(err, blobstore.ErrImmutableConflict) {
				t.Fatalf("mapError() = %v, want %v", err, blobstore.ErrImmutableConflict)
			}
		})
	}
}

func TestCompareAndSwapReturnsTokenFromConditionalWrite(t *testing.T) {
	t.Parallel()

	const (
		bucket        = "metadata"
		writeETag     = `"writer-a"`
		successorETag = `"writer-b"`
	)
	var headCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			w.Header().Set("ETag", writeETag)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("ETag", successorETag)
			w.Header().Set("Last-Modified", time.Unix(1_776_263_000, 0).UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)

	backend, err := New(newClientForEndpoint(t, server.URL, nil), bucket)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	object, swapped, err := backend.CompareAndSwap(context.Background(), "head.plc", `"previous"`, []byte("writer-a"))
	if err != nil {
		t.Fatalf("CompareAndSwap() error = %v", err)
	}
	if !swapped {
		t.Fatal("CompareAndSwap() swapped = false, want true")
	}
	if object.Token != writeETag {
		t.Fatalf("CompareAndSwap() token = %q, want write response token %q", object.Token, writeETag)
	}
	if got := headCalls.Load(); got != 0 {
		t.Fatalf("HeadObject calls = %d, want 0 after successful conditional write", got)
	}
}

func TestDeleteBatchWithFakeS3(t *testing.T) {
	t.Parallel()

	const bucket = "metadata"
	client := newFakeClient(t, bucket)
	backend, err := New(client, bucket)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := client.PutObject(context.Background(), &awss3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte(key)),
		}); err != nil {
			t.Fatalf("PutObject(%q) error = %v", key, err)
		}
	}

	errs := backend.DeleteBatch(context.Background(), []string{"a", "missing", "b", "c"})
	if len(errs) != 4 {
		t.Fatalf("DeleteBatch() results = %d, want 4", len(errs))
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("DeleteBatch() result[%d] = %v", i, err)
		}
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := backend.Get(context.Background(), key); !errors.Is(err, blobstore.ErrObjectNotFound) {
			t.Fatalf("Get(%q) error = %v, want %v", key, err, blobstore.ErrObjectNotFound)
		}
	}
}

func TestApplyDeleteErrorsRejectsUnknownResponseKey(t *testing.T) {
	t.Parallel()

	errs := make([]error, 1)
	err := applyDeleteErrors(errs, map[string][]int{"expected": {0}}, []awstypes.Error{{
		Key:     aws.String("unexpected"),
		Code:    aws.String("AccessDenied"),
		Message: aws.String("denied"),
	}})
	if err == nil {
		t.Fatal("applyDeleteErrors() error = nil, want protocol error")
	}
}

func TestDeleteBatchUsesConfiguredSDKRetryForSlowDown(t *testing.T) {
	t.Parallel()

	const bucket = "metadata"
	storage := s3mem.New()
	if err := storage.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	fakeHandler := gofakes3.New(storage).Server()
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") && attempts.Add(1) <= 2 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, `<Error><Code>SlowDown</Code><Message>reduce request rate</Message></Error>`)
			return
		}
		fakeHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	client := newClientForEndpoint(t, server.URL, awsretry.NewStandard(func(o *awsretry.StandardOptions) {
		o.MaxAttempts = 3
		o.Backoff = awsretry.BackoffDelayerFunc(func(int, error) (time.Duration, error) {
			return 0, nil
		})
	}))
	backend, err := New(client, bucket)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := client.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("a"), Body: bytes.NewReader([]byte("a")),
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	errs := backend.DeleteBatch(context.Background(), []string{"a"})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("DeleteBatch() errors = %v", errs)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("DeleteObjects attempts = %d, want 3", got)
	}
}

func newFakeClient(t testing.TB, bucket string) *awss3.Client {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(server.Close)
	return newClientForEndpoint(t, server.URL, nil)
}

func newClientForEndpoint(t testing.TB, endpoint string, retryer aws.Retryer) *awss3.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access-key", "secret-key", "")),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		if retryer != nil {
			o.Retryer = retryer
		}
	})
}
