package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ankur-anand/isledb/blobstore"
	s3store "github.com/ankur-anand/isledb/blobstore/s3"
	"github.com/ankur-anand/isledb/blobstore/storetest"
)

// liveHarness runs against a real S3-compatible endpoint (path-style,
// us-east-1) when all four E09_S3_* variables are set. There is no transport
// fault injection against a live endpoint beyond what faultTransport can do on
// the client side, which is the same injector the in-process fake uses.
func liveHarness(t *testing.T) storetest.Harness {
	t.Helper()
	endpoint, bucket := os.Getenv("E09_S3_ENDPOINT"), os.Getenv("E09_S3_BUCKET")
	accessKey, secretKey := os.Getenv("E09_S3_ACCESS_KEY"), os.Getenv("E09_S3_SECRET_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set E09_S3_ENDPOINT, E09_S3_BUCKET, E09_S3_ACCESS_KEY and E09_S3_SECRET_KEY to run against a live endpoint")
	}
	ctx := context.Background()
	base := &http.Transport{MaxIdleConnsPerHost: 8}
	t.Cleanup(base.CloseIdleConnections)
	faults := &faultTransport{next: base}
	raw := newClient(endpoint, accessKey, secretKey, base)

	if _, err := raw.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		_, createErr := raw.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
		var owned *types.BucketAlreadyOwnedByYou
		if createErr != nil && !errors.As(createErr, &owned) {
			t.Fatalf("bucket %q unavailable: head: %v; create: %v", bucket, err, createErr)
		}
	}

	var nonce [6]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("e09-blobstore-s3/%d-%s/", time.Now().UnixNano(), hex.EncodeToString(nonce[:]))
	t.Cleanup(func() { // best effort
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		var token *string
		for {
			page, err := raw.ListObjectsV2(cleanup, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token})
			if err != nil {
				t.Logf("cleanup list: %v", err)
				return
			}
			for _, object := range page.Contents {
				if _, err := raw.DeleteObject(cleanup, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key}); err != nil {
					t.Logf("cleanup delete %s: %v", aws.ToString(object.Key), err)
				}
			}
			if !aws.ToBool(page.IsTruncated) {
				return
			}
			token = page.NextContinuationToken
		}
	})

	open := func(t *testing.T) *s3store.Store {
		t.Helper()
		store, err := s3store.New(newClient(endpoint, accessKey, secretKey, faults), bucket)
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open(t)
	return storetest.Harness{
		Metadata: store, Runs: store, Prefix: prefix,
		Reopen: func(t *testing.T) (blobstore.MetadataStore, blobstore.RunStore) {
			reopened := open(t)
			return reopened, reopened
		},
		Replace: func(t *testing.T, key string, body []byte) {
			t.Helper()
			if _, err := raw.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
		},
		Inject: func(t *testing.T, op storetest.Op, kind storetest.FaultKind) func() int {
			t.Cleanup(faults.disarm)
			return faults.arm(op, kind)
		},
	}
}

func TestLiveMetadataConformance(t *testing.T) { storetest.MetadataSuite(t, liveHarness(t)) }
func TestLiveRunConformance(t *testing.T)      { storetest.RunSuite(t, liveHarness(t)) }
