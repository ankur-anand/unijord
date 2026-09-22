// Package memory is the deterministic in-process backend used by tests. It is
// the only local backend: IsleDB tests model durable storage with it, and a
// process restart is modelled by Reopen, which returns a fresh handle over the
// same objects. It implements both blobstore contracts and exposes explicit
// fault hooks and request counters, both of which are per handle.
package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/blobstore/internal/idtoken"
)

const (
	provider = "memory"
	// MaxCreateBytes bounds a test run object held in process memory.
	MaxCreateBytes = int64(1 << 30)
	copyChunk      = 64 << 10
)

// Op names one contract operation for fault injection and request counting.
type Op string

const (
	OpBoundedGet       Op = "BoundedGet"
	OpPut              Op = "Put"
	OpCompareAndSwap   Op = "CompareAndSwap"
	OpList             Op = "List"
	OpDelete           Op = "Delete"
	OpCreate           Op = "Create"
	OpStat             Op = "Stat"
	OpOpenRange        Op = "OpenRange"
	OpDeleteIfIdentity Op = "DeleteIfIdentity"
)

// Fault describes one injected provider failure. Before fails the request
// without applying it. After applies the request and then loses the response.
// CloseErr makes the OpenRange body fail on Close.
type Fault struct {
	Before   error
	After    error
	CloseErr error
}

type entry struct {
	body       []byte
	generation uint64
}

// state is the "durable" object set. It outlives any one Store handle.
type state struct {
	mu         sync.Mutex
	objects    map[string]entry
	generation uint64
}

type Store struct {
	*state
	hooks struct {
		sync.Mutex
		fault    func(Op, string) Fault
		requests map[Op]uint64
	}
}

var (
	_ blobstore.MetadataStore = (*Store)(nil)
	_ blobstore.RunStore      = (*Store)(nil)
)

func New() *Store {
	return newHandle(&state{objects: make(map[string]entry)})
}

func newHandle(st *state) *Store {
	s := &Store{state: st}
	s.hooks.requests = make(map[Op]uint64)
	return s
}

// Reopen returns a new handle over the same objects, as a restarted process
// would see them. Fault hooks and request counters start empty; the objects,
// their tokens and identities are exactly those the previous handle stored.
func (s *Store) Reopen() *Store { return newHandle(s.state) }

// SetFault installs a hook consulted once per operation. nil removes it.
func (s *Store) SetFault(hook func(op Op, key string) Fault) {
	s.hooks.Lock()
	s.hooks.fault = hook
	s.hooks.Unlock()
}

// Requests returns a copy of this handle's per-operation request counters.
func (s *Store) Requests() map[Op]uint64 {
	s.hooks.Lock()
	defer s.hooks.Unlock()
	out := make(map[Op]uint64, len(s.hooks.requests))
	for op, n := range s.hooks.requests {
		out[op] = n
	}
	return out
}

// Replace unconditionally overwrites a key, as an out-of-band writer would.
// It exists so tests can prove replacement detection.
func (s *Store) Replace(key string, body []byte) {
	s.mu.Lock()
	s.generation++
	s.objects[key] = entry{body: bytes.Clone(body), generation: s.generation}
	s.mu.Unlock()
}

// Remove unconditionally deletes a key out of band.
func (s *Store) Remove(key string) {
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
}

func (s *Store) begin(ctx context.Context, op Op, key string) (Fault, error) {
	if ctx == nil || (op != OpList && !blobstore.ValidKey(key)) {
		return Fault{}, blobstore.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Fault{}, err
	}
	s.hooks.Lock()
	hook := s.hooks.fault
	s.hooks.requests[op]++
	s.hooks.Unlock()
	if hook == nil {
		return Fault{}, nil
	}
	return hook(op, key), nil
}

func metadataToken(generation uint64) string { return "m" + strconv.FormatUint(generation, 10) }

type runToken struct {
	Generation uint64 `json:"generation"`
}

func identity(key string, e entry) (blobstore.RunIdentity, error) {
	token, err := idtoken.Encode(provider, runToken{Generation: e.generation})
	if err != nil {
		return blobstore.RunIdentity{}, err
	}
	return blobstore.RunIdentity{Key: key, Size: int64(len(e.body)), Token: token}, nil
}

func (s *Store) BoundedGet(ctx context.Context, key string, maxBytes int64) (blobstore.Object, error) {
	if maxBytes < 1 || maxBytes > blobstore.MaxMetadataBytes {
		return blobstore.Object{}, blobstore.ErrInvalidRequest
	}
	fault, err := s.begin(ctx, OpBoundedGet, key)
	if err != nil {
		return blobstore.Object{}, err
	}
	if fault.Before != nil {
		return blobstore.Object{}, fault.Before
	}
	s.mu.Lock()
	e, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return blobstore.Object{}, blobstore.ErrNotFound
	}
	if int64(len(e.body)) > maxBytes {
		return blobstore.Object{}, blobstore.ErrTooLarge
	}
	if fault.After != nil {
		return blobstore.Object{}, fault.After
	}
	return blobstore.Object{Key: key, Body: bytes.Clone(e.body), Token: metadataToken(e.generation)}, nil
}

func (s *Store) Put(ctx context.Context, key string, body []byte) (blobstore.Object, error) {
	fault, err := s.begin(ctx, OpPut, key)
	if err != nil {
		return blobstore.Object{}, err
	}
	if fault.Before != nil {
		return blobstore.Object{}, fault.Before
	}
	s.mu.Lock()
	e, exists := s.objects[key]
	if !exists {
		s.generation++
		e = entry{body: bytes.Clone(body), generation: s.generation}
		s.objects[key] = e
	}
	s.mu.Unlock()
	if exists && !bytes.Equal(e.body, body) {
		return blobstore.Object{}, blobstore.ErrImmutableConflict
	}
	if fault.After != nil {
		return blobstore.Object{}, errors.Join(blobstore.ErrIndeterminate, fault.After)
	}
	return blobstore.Object{Key: key, Body: bytes.Clone(e.body), Token: metadataToken(e.generation)}, nil
}

func (s *Store) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (blobstore.CASResult, error) {
	fault, err := s.begin(ctx, OpCompareAndSwap, key)
	if err != nil {
		return blobstore.CASResult{}, err
	}
	if fault.Before != nil {
		return blobstore.CASResult{}, fault.Before
	}
	s.mu.Lock()
	current, exists := s.objects[key]
	matched := (expectedToken == "" && !exists) || (exists && expectedToken == metadataToken(current.generation))
	var next entry
	if matched {
		s.generation++
		next = entry{body: bytes.Clone(body), generation: s.generation}
		s.objects[key] = next
	}
	s.mu.Unlock()
	if fault.After != nil {
		return blobstore.CASResult{}, fault.After
	}
	if !matched {
		result := blobstore.CASResult{Outcome: blobstore.CASConflict}
		if exists {
			result.Current = blobstore.ObjectInfo{Key: key, Size: int64(len(current.body))}
			result.CurrentToken, result.CurrentKnown = metadataToken(current.generation), true
		}
		return result, nil
	}
	return blobstore.CASResult{Outcome: blobstore.CASApplied,
		Object: blobstore.Object{Key: key, Token: metadataToken(next.generation)}}, nil
}

func (s *Store) List(ctx context.Context, opts blobstore.ListOptions) (blobstore.ObjectPage, error) {
	fault, err := s.begin(ctx, OpList, opts.Prefix)
	if err != nil {
		return blobstore.ObjectPage{}, err
	}
	if fault.Before != nil {
		return blobstore.ObjectPage{}, fault.Before
	}
	limit := opts.NormalizedLimit()
	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, opts.Prefix) && key > opts.AfterKey {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var page blobstore.ObjectPage
	if len(keys) > limit {
		keys, page.HasMore = keys[:limit], true
	}
	page.Objects = make([]blobstore.ObjectInfo, len(keys))
	for i, key := range keys {
		page.Objects[i] = blobstore.ObjectInfo{Key: key, Size: int64(len(s.objects[key].body))}
	}
	s.mu.Unlock()
	if page.HasMore {
		page.NextAfterKey = keys[len(keys)-1]
	}
	if fault.After != nil {
		return blobstore.ObjectPage{}, fault.After
	}
	return page, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	fault, err := s.begin(ctx, OpDelete, key)
	if err != nil {
		return err
	}
	if fault.Before != nil {
		return fault.Before
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return fault.After
}

func (s *Store) Create(ctx context.Context, key string, body io.Reader, exactSize int64) (blobstore.CreateResult, error) {
	absent := blobstore.CreateResult{Outcome: blobstore.DefinitelyAbsent}
	if body == nil || exactSize < 1 || exactSize > MaxCreateBytes {
		return absent, blobstore.ErrInvalidRequest
	}
	fault, err := s.begin(ctx, OpCreate, key)
	if err != nil {
		return absent, err
	}
	if fault.Before != nil {
		return absent, fault.Before
	}
	counted := blobstore.NewCountingBody(body, exactSize)
	var data bytes.Buffer
	data.Grow(int(min(exactSize, 1<<20)))
	chunk := make([]byte, copyChunk)
	for {
		if err := ctx.Err(); err != nil {
			return absent, errors.Join(err, counted.Err())
		}
		n, readErr := counted.Read(chunk)
		data.Write(chunk[:n])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return absent, readErr
		}
	}
	if !counted.Complete() {
		return absent, errors.Join(io.ErrUnexpectedEOF, counted.Err())
	}
	s.mu.Lock()
	_, exists := s.objects[key]
	var e entry
	if !exists {
		s.generation++
		e = entry{body: data.Bytes(), generation: s.generation}
		s.objects[key] = e
	}
	s.mu.Unlock()
	if exists {
		return blobstore.CreateResult{Outcome: blobstore.AlreadyExists}, blobstore.ErrAlreadyExists
	}
	if fault.After != nil {
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, fault.After)
	}
	id, err := identity(key, e)
	if err != nil {
		return blobstore.CreateResult{}, errors.Join(blobstore.ErrIndeterminate, err)
	}
	return blobstore.CreateResult{Outcome: blobstore.Created, Identity: id}, nil
}

func (s *Store) Stat(ctx context.Context, key string) (blobstore.RunIdentity, error) {
	fault, err := s.begin(ctx, OpStat, key)
	if err != nil {
		return blobstore.RunIdentity{}, err
	}
	if fault.Before != nil {
		return blobstore.RunIdentity{}, fault.Before
	}
	s.mu.Lock()
	e, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return blobstore.RunIdentity{}, blobstore.ErrNotFound
	}
	if fault.After != nil {
		return blobstore.RunIdentity{}, fault.After
	}
	return identity(key, e)
}

func decode(id blobstore.RunIdentity) (uint64, error) {
	var token runToken
	if err := idtoken.Decode(provider, id.Token, &token); err != nil || token.Generation == 0 {
		return 0, errors.Join(blobstore.ErrInvalidIdentity, err)
	}
	return token.Generation, nil
}

func (s *Store) OpenRange(ctx context.Context, key string, id blobstore.RunIdentity, offset, length int64) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, blobstore.ErrInvalidRequest
	}
	if err := blobstore.CheckRange(id, key, offset, length); err != nil {
		return nil, err
	}
	generation, err := decode(id)
	if err != nil {
		return nil, err
	}
	fault, err := s.begin(ctx, OpOpenRange, key)
	if err != nil {
		return nil, err
	}
	if fault.Before != nil {
		return nil, fault.Before
	}
	s.mu.Lock()
	e, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		return nil, errors.Join(blobstore.ErrRunChanged, blobstore.ErrNotFound)
	}
	if e.generation != generation || int64(len(e.body)) != id.Size {
		return nil, blobstore.ErrRunChanged
	}
	if fault.After != nil {
		return nil, fault.After
	}
	// Entry bodies are never mutated in place, so this view stays pinned to
	// the identity even if the key is replaced while it is being read.
	return &rangeBody{ctx: ctx, r: bytes.NewReader(e.body[offset : offset+length]), closeErr: fault.CloseErr}, nil
}

type rangeBody struct {
	ctx      context.Context
	r        *bytes.Reader
	closeErr error
}

func (b *rangeBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.r.Read(p)
}

func (b *rangeBody) Close() error {
	if b.closeErr != nil {
		return errors.Join(blobstore.ErrCleanup, b.closeErr)
	}
	return nil
}

func (s *Store) DeleteIfIdentity(ctx context.Context, key string, id blobstore.RunIdentity) error {
	if ctx == nil || !blobstore.ValidKey(key) {
		return blobstore.ErrInvalidRequest
	}
	if !id.Valid() || id.Key != key {
		return blobstore.ErrInvalidIdentity
	}
	generation, err := decode(id)
	if err != nil {
		return err
	}
	fault, err := s.begin(ctx, OpDeleteIfIdentity, key)
	if err != nil {
		return err
	}
	if fault.Before != nil {
		return fault.Before
	}
	s.mu.Lock()
	e, ok := s.objects[key]
	matched := ok && e.generation == generation && int64(len(e.body)) == id.Size
	if matched {
		delete(s.objects, key)
	}
	s.mu.Unlock()
	if !ok {
		return blobstore.ErrNotFound
	}
	if !matched {
		return blobstore.ErrRunChanged
	}
	if fault.After != nil {
		return errors.Join(blobstore.ErrIndeterminate, fault.After)
	}
	return nil
}
