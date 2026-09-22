package azure

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const fakeContainerURL = "http://fake.blob.invalid/ctr"

// fakeService is a scripted policy.Transporter implementing just enough of the
// Blob REST API for both conformance suites: Put Blob, Put Block, Put Block
// List, Get Blob, Get Blob Properties, Delete Blob and List Blobs, with
// If-Match / If-None-Match enforcement. Uncommitted blocks live only in
// staged and are invisible to every read.
type fakeService struct {
	mu         sync.Mutex
	blobs      map[string]*fakeBlob
	versions   map[string]*fakeBlob // name + "\x00" + version
	staged     map[string]map[string][]byte
	sequence   uint64
	versioning bool
	// putIfMatchMissing is the status for a conditional write whose If-Match
	// names a blob that does not exist.
	putIfMatchMissing int
	// missingIs412 mimics Azurite: If-Match reads and deletes of a missing
	// blob answer 412 ConditionNotMet instead of 404 BlobNotFound.
	missingIs412 bool
	records      []*fakeRequest
	faults       []*fakeFault
	bodies       []*trackedBody
}

type fakeBlob struct {
	data    []byte
	etag    string
	version string
}

type fakeRequest struct {
	raw       *http.Request
	method    string
	blob      string // empty for container requests
	comp      string
	blockID   string
	versionID string
	header    http.Header
	body      []byte // retained for every request except Put Block
	bodyLen   int
}

func (r *fakeRequest) isRange() bool {
	return r.method == http.MethodGet && r.blob != "" && r.header.Get("x-ms-range") != ""
}

// fakeFault intercepts the first matching request after arming. seen counts
// every matching request since arming, so a hidden retry is observable.
type fakeFault struct {
	match func(*fakeRequest) bool
	act   func(f *fakeService, r *fakeRequest, apply func() *http.Response) (*http.Response, error)
	armed bool
	seen  int
}

type trackedBody struct {
	reader   *bytes.Reader
	request  *fakeRequest
	status   int
	closes   atomic.Int32
	read     atomic.Int64
	closeErr error
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read.Add(int64(n))
	return n, err
}

func (b *trackedBody) Close() error {
	b.closes.Add(1)
	return b.closeErr
}

func newFakeService() *fakeService {
	return &fakeService{blobs: map[string]*fakeBlob{}, versions: map[string]*fakeBlob{},
		staged: map[string]map[string][]byte{}, putIfMatchMissing: http.StatusPreconditionFailed}
}

func (f *fakeService) store(t testing.TB) *Store {
	t.Helper()
	// Default azcore retry options (three retries) stay in force on purpose:
	// only the per-call override in the leaf can keep request counts at one.
	client, err := container.NewClientWithNoCredential(fakeContainerURL,
		&container.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: f}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func (f *fakeService) arm(match func(*fakeRequest) bool,
	act func(f *fakeService, r *fakeRequest, apply func() *http.Response) (*http.Response, error)) *fakeFault {
	fault := &fakeFault{match: match, act: act, armed: true}
	f.mu.Lock()
	f.faults = append(f.faults, fault)
	f.mu.Unlock()
	return fault
}

func (f *fakeService) disarm(fault *fakeFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, candidate := range f.faults {
		if candidate == fault {
			f.faults = append(f.faults[:i], f.faults[i+1:]...)
			return
		}
	}
}

func (f *fakeService) seen(fault *fakeFault) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fault.seen
}

// requests returns the recorded requests from index start that satisfy match.
func (f *fakeService) requests(start int, match func(*fakeRequest) bool) []*fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*fakeRequest
	for _, record := range f.records[start:] {
		if match == nil || match(record) {
			out = append(out, record)
		}
	}
	return out
}

func (f *fakeService) mark() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

// replace is an unconditional out-of-band Put Blob.
func (f *fakeService) replace(name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitLocked(name, bytes.Clone(data))
}

func (f *fakeService) commitLocked(name string, data []byte) *fakeBlob {
	f.sequence++
	committed := &fakeBlob{data: data, etag: fmt.Sprintf("\"0x8DFAKE%010X\"", f.sequence)}
	if f.versioning {
		committed.version = fmt.Sprintf("2026-01-01T00:00:00.%07dZ", f.sequence)
		f.versions[name+"\x00"+committed.version] = committed
	}
	f.blobs[name] = committed
	// Any commit on the blob discards its uncommitted blocks.
	delete(f.staged, name)
	return committed
}

func (f *fakeService) Do(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err = errors.Join(err, req.Body.Close()); err != nil {
			return nil, err
		}
	}
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	query := req.URL.Query()
	record := &fakeRequest{raw: req, method: req.Method, comp: query.Get("comp"), blockID: query.Get("blockid"),
		versionID: query.Get("versionid"), header: http.Header{}, bodyLen: len(body)}
	// The SDK writes x-ms-* keys in lower case, bypassing canonicalization.
	for name, values := range req.Header {
		for _, value := range values {
			record.header.Add(name, value)
		}
	}
	if name, ok := strings.CutPrefix(req.URL.Path, "/ctr/"); ok {
		record.blob = name
	}
	if record.comp != "block" {
		record.body = body
	}
	f.mu.Lock()
	f.records = append(f.records, record)
	var hit *fakeFault
	for _, fault := range f.faults {
		if !fault.match(record) {
			continue
		}
		fault.seen++
		if fault.armed && hit == nil {
			fault.armed, hit = false, fault
		}
	}
	f.mu.Unlock()
	apply := func() *http.Response { return f.handle(req, record, body) }
	if hit != nil {
		return hit.act(f, record, apply)
	}
	return apply(), nil
}

func (f *fakeService) respond(record *fakeRequest, status int, header http.Header, body []byte) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	if record.method == http.MethodHead {
		body = nil
	} else {
		header.Set("Content-Length", strconv.Itoa(len(body)))
	}
	tracked := &trackedBody{reader: bytes.NewReader(body), request: record, status: status}
	f.mu.Lock()
	f.bodies = append(f.bodies, tracked)
	f.mu.Unlock()
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header,
		Request: record.raw, Body: tracked, ContentLength: int64(len(body)), Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1}
}

func (f *fakeService) fail(record *fakeRequest, status int, code string) *http.Response {
	header := http.Header{"X-Ms-Error-Code": {code}, "Content-Type": {"application/xml"}}
	body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>fake %s</Message></Error>`, code, code)
	return f.respond(record, status, header, []byte(body))
}

// conditions enforces If-Match / If-None-Match against the current blob.
func (f *fakeService) conditions(record *fakeRequest, current *fakeBlob, write bool) (int, string) {
	ifMatch, ifNoneMatch := record.header.Get("If-Match"), record.header.Get("If-None-Match")
	if current == nil {
		switch {
		case ifMatch != "" && write:
			if f.putIfMatchMissing == http.StatusNotFound {
				return http.StatusNotFound, "BlobNotFound"
			}
			return f.putIfMatchMissing, "ConditionNotMet"
		case !write && ifMatch != "" && f.missingIs412:
			return http.StatusPreconditionFailed, "ConditionNotMet"
		case !write:
			return http.StatusNotFound, "BlobNotFound"
		}
		return 0, ""
	}
	if ifNoneMatch == "*" && write {
		return http.StatusConflict, "BlobAlreadyExists"
	}
	if (ifMatch != "" && ifMatch != "*" && ifMatch != current.etag) || (ifNoneMatch != "" && ifNoneMatch == current.etag) {
		return http.StatusPreconditionFailed, "ConditionNotMet"
	}
	return 0, ""
}

func blobHeaders(current *fakeBlob) http.Header {
	header := http.Header{"Etag": {current.etag}, "X-Ms-Blob-Type": {"BlockBlob"}}
	if current.version != "" {
		header.Set("x-ms-version-id", current.version)
	}
	return header
}

func (f *fakeService) handle(req *http.Request, record *fakeRequest, body []byte) *http.Response {
	f.mu.Lock()
	status, code, header, payload := f.decideLocked(req, record, body)
	f.mu.Unlock()
	if code != "" {
		return f.fail(record, status, code)
	}
	return f.respond(record, status, header, payload)
}

func (f *fakeService) decideLocked(req *http.Request, record *fakeRequest, body []byte) (status int, code string, header http.Header, payload []byte) {
	query := req.URL.Query()
	if record.blob == "" {
		if record.method == http.MethodGet && query.Get("restype") == "container" && record.comp == "list" {
			return http.StatusOK, "", http.Header{"Content-Type": {"application/xml"}},
				f.listLocked(query.Get("prefix"), query.Get("marker"), query.Get("maxresults"))
		}
		return http.StatusBadRequest, "UnsupportedFakeOperation", nil, nil
	}
	current := f.blobs[record.blob]
	if record.versionID != "" {
		current = f.versions[record.blob+"\x00"+record.versionID]
	}
	switch {
	case record.method == http.MethodPut && record.comp == "block":
		if f.staged[record.blob] == nil {
			f.staged[record.blob] = map[string][]byte{}
		}
		f.staged[record.blob][record.blockID] = body
		return http.StatusCreated, "", nil, nil
	case record.method == http.MethodPut && (record.comp == "" || record.comp == "blocklist"):
		if status, code = f.conditions(record, current, true); status != 0 {
			return status, code, nil, nil
		}
		data := bytes.Clone(body)
		if record.comp == "blocklist" {
			var list struct {
				Latest []string `xml:"Latest"`
			}
			if err := xml.Unmarshal(body, &list); err != nil {
				return http.StatusBadRequest, "InvalidXmlDocument", nil, nil
			}
			data = nil
			for _, id := range list.Latest {
				block, ok := f.staged[record.blob][id]
				if !ok {
					return http.StatusBadRequest, "InvalidBlockList", nil, nil
				}
				data = append(data, block...)
			}
		}
		return http.StatusCreated, "", blobHeaders(f.commitLocked(record.blob, data)), nil
	case (record.method == http.MethodGet || record.method == http.MethodHead) && record.comp == "":
		if status, code = f.conditions(record, current, false); status != 0 {
			return status, code, nil, nil
		}
		header, payload, status = blobHeaders(current), current.data, http.StatusOK
		if value := record.header.Get("x-ms-range"); value != "" {
			var first, last int64
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &first, &last); err != nil || first < 0 || last < first {
				return http.StatusBadRequest, "InvalidHeaderValue", nil, nil
			}
			if first >= int64(len(current.data)) {
				return http.StatusRequestedRangeNotSatisfiable, "InvalidRange", nil, nil
			}
			last = min(last, int64(len(current.data))-1)
			header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(current.data)))
			payload, status = current.data[first:last+1], http.StatusPartialContent
		}
		if record.method == http.MethodHead {
			header.Set("Content-Length", strconv.Itoa(len(payload)))
		}
		return status, "", header, payload
	case record.method == http.MethodDelete && record.comp == "":
		if status, code = f.conditions(record, current, false); status != 0 {
			return status, code, nil, nil
		}
		delete(f.blobs, record.blob)
		return http.StatusAccepted, "", nil, nil
	}
	return http.StatusBadRequest, "UnsupportedFakeOperation", nil, nil
}

func (f *fakeService) listLocked(prefix, marker, maxResults string) []byte {
	limit, err := strconv.Atoi(maxResults)
	if err != nil || limit < 1 || limit > 5000 {
		limit = 5000
	}
	names := make([]string, 0, len(f.blobs))
	for name := range f.blobs {
		if strings.HasPrefix(name, prefix) && name >= marker {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	next := ""
	if len(names) > limit {
		next, names = names[limit], names[:limit]
	}
	var out bytes.Buffer
	out.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://fake.blob.invalid/" ContainerName="ctr"><Blobs>`)
	for _, name := range names {
		out.WriteString("<Blob><Name>")
		_ = xml.EscapeText(&out, []byte(name))
		fmt.Fprintf(&out, "</Name><Properties><Content-Length>%d</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>", len(f.blobs[name].data))
	}
	out.WriteString("</Blobs><NextMarker>")
	_ = xml.EscapeText(&out, []byte(next))
	out.WriteString("</NextMarker></EnumerationResults>")
	return out.Bytes()
}

// assertBodiesClosed proves every response body handed to the SDK or the leaf
// was closed exactly once.
func (f *fakeService) assertBodiesClosed(t testing.TB) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, body := range f.bodies {
		if n := body.closes.Load(); n != 1 {
			t.Errorf("response body of %s %q comp=%q status=%d closed %d times",
				body.request.method, body.request.blob, body.request.comp, body.status, n)
		}
	}
}
