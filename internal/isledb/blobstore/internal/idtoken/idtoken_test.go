package idtoken

import (
	"encoding/base64"
	"errors"
	"testing"
)

type fields struct {
	ETag      string `json:"etag"`
	VersionID string `json:"version_id,omitempty"`
}

func TestRoundTripAndDeterminism(t *testing.T) {
	in := fields{ETag: `"abc"`, VersionID: "v.1/2"}
	first, err := Encode("s3", in)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := Encode("s3", in)
	if first != second || first != `s3.v1.`+base64.RawURLEncoding.EncodeToString([]byte(`{"etag":"\"abc\"","version_id":"v.1/2"}`)) {
		t.Fatalf("not deterministic or not pinned: %s", first)
	}
	var out fields
	if err := Decode("s3", first, &out); err != nil || out != in {
		t.Fatalf("%+v %v", out, err)
	}
}

func TestDecodeRejections(t *testing.T) {
	good, _ := Encode("s3", fields{ETag: "e"})
	enc := func(s string) string { return "s3.v1." + base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for name, token := range map[string]string{
		"empty": "", "garbage": "garbage", "other provider": "gcs" + good[2:], "other version": "s3.v2" + good[5:],
		"no payload": "s3.v1.", "bad base64": "s3.v1.!!!", "padded": good + "=", "extra part": good + ".x",
		"unknown field": enc(`{"etag":"e","x":1}`), "trailing data": enc(`{"etag":"e"}{}`),
		"non-canonical spacing": enc(`{ "etag": "e" }`), "non-canonical order": enc(`{"version_id":"v","etag":"e"}`),
		"not an object": enc(`"e"`),
	} {
		var out fields
		if err := Decode("s3", token, &out); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
	for _, provider := range []string{"", "a.b", "a b"} {
		if _, err := Encode(provider, fields{}); !errors.Is(err, ErrMalformed) {
			t.Fatalf("provider %q accepted", provider)
		}
	}
}
