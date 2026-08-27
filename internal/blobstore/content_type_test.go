package blobstore

import "testing"

func TestContentTypeForKey(t *testing.T) {
	t.Parallel()

	if got := ContentTypeForKey("catalog/head.plc"); got != BinaryContentType {
		t.Fatalf("binary catalog content type = %q", got)
	}
	if got := ContentTypeForKey("catalog/maintenance/retention.json"); got != JSONContentType {
		t.Fatalf("JSON maintenance content type = %q", got)
	}
}
