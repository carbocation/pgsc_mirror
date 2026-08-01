package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/catalog"
	"github.com/carbocation/pgsc_mirror/internal/config"
	"github.com/carbocation/pgsc_mirror/internal/store"
	"github.com/carbocation/pgsc_mirror/internal/transfer"
)

func probeMD5(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}

func newProbeTestApp(server *httptest.Server, attempts int) *App {
	httpClient := transfer.NewHTTPClient(5*time.Second, transfer.Policy{
		Attempts: attempts, InitialBackoff: 0, MaxBackoff: 0,
	}, "pgsc-probe-test").WithClient(server.Client())
	return &App{
		Config:  config.Config{GenomeBuild: "GRCh38"},
		Catalog: catalog.New([]string{server.URL}, "score-list", "metadata-must-not-be-fetched", httpClient),
		HTTP:    httpClient,
		now:     time.Now,
	}
}

func TestProbeSuccessRetriesAndUsesUnknownSize(t *testing.T) {
	bodies := map[string][]byte{
		"PGS000001": []byte("first compressed fixture"),
		"PGS000002": []byte("second compressed fixture"),
	}
	var listCalls atomic.Int32
	var retryCalls atomic.Int32
	var metadataCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/score-list":
			listCalls.Add(1)
			_, _ = io.WriteString(w, "PGS000001\nPGS000002\n")
		case r.URL.Path == "/metadata-must-not-be-fetched":
			metadataCalls.Add(1)
			http.Error(w, "not allowed", http.StatusInternalServerError)
		default:
			id := "PGS000001"
			if strings.Contains(r.URL.Path, "PGS000002") {
				id = "PGS000002"
			}
			body := bodies[id]
			if strings.HasSuffix(r.URL.Path, ".md5") {
				if id == "PGS000002" && retryCalls.Add(1) == 1 {
					http.Error(w, "temporary", http.StatusServiceUnavailable)
					return
				}
				_, _ = fmt.Fprintf(w, "%s  %s.txt.gz\n", probeMD5(body), id)
				return
			}
			if r.Method == http.MethodHead {
				if id == "PGS000002" {
					http.Error(w, "HEAD unsupported", http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(body)))
				w.Header().Set("ETag", `"fixture-v1"`)
				return
			}
			if id == "PGS000002" {
				w.WriteHeader(http.StatusOK)
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			_, _ = w.Write(body)
		}
	}))
	defer srv.Close()

	fake := newMemoryStore()
	a := newProbeTestApp(srv, 2)
	a.targets = []target{{kind: "gcs", Store: fake}}
	report, err := a.Probe(context.Background(), ProbeOptions{
		PGSIDs: []string{"PGS000002", "PGS000001"}, MaxBytes: 1 << 20, GCSSmoke: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "passed" || len(report.Scores) != 2 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Scores[0].PGSID != "PGS000002" || report.Scores[1].PGSID != "PGS000001" {
		t.Fatalf("requested order was not preserved: %+v", report.Scores)
	}
	if report.Scores[0].SizeKnown || report.Scores[0].HeadSupported {
		t.Fatalf("unsupported HEAD was not reported as unknown: %+v", report.Scores[0])
	}
	if report.Scores[1].ETag != `"fixture-v1"` || !report.Scores[1].SizeKnown {
		t.Fatalf("known-size validators missing: %+v", report.Scores[1])
	}
	if retryCalls.Load() != 2 || listCalls.Load() != 1 || metadataCalls.Load() != 0 {
		t.Fatalf("calls: retry=%d list=%d metadata=%d", retryCalls.Load(), listCalls.Load(), metadataCalls.Load())
	}
	if report.GCS == nil || report.GCS.Status != "passed" || !report.GCS.StaleGenerationRejected || !report.GCS.AbsenceConfirmed {
		t.Fatalf("GCS smoke report incomplete: %+v", report.GCS)
	}
	if fake.puts != 3 || fake.opens != 1 || fake.deletes != 1 || fake.stats != 1 {
		t.Fatalf("unexpected fake-store calls: %+v", fake)
	}
}

func TestProbeContinuesAfterMissingIDAndGatesGCS(t *testing.T) {
	body := []byte("valid fixture")
	var scoreGets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/score-list":
			_, _ = io.WriteString(w, "PGS000002\n")
		case strings.HasSuffix(r.URL.Path, ".md5"):
			_, _ = fmt.Fprintln(w, probeMD5(body))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		case strings.Contains(r.URL.Path, "PGS000002"):
			scoreGets.Add(1)
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	fake := newMemoryStore()
	a := newProbeTestApp(srv, 2)
	a.targets = []target{{kind: "gcs", Store: fake}}
	report, err := a.Probe(context.Background(), ProbeOptions{
		PGSIDs: []string{"PGS000001", "PGS000002"}, MaxBytes: 1 << 20, GCSSmoke: true,
	})
	if !errors.Is(err, ErrProbeFailed) {
		t.Fatalf("got %v, want probe failure", err)
	}
	if len(report.Scores) != 2 || report.Scores[0].Status != "missing" || report.Scores[1].Status != "verified" || scoreGets.Load() != 1 {
		t.Fatalf("probe did not continue: %+v", report.Scores)
	}
	if report.GCS == nil || report.GCS.Status != "skipped" || fake.puts != 0 {
		t.Fatalf("GCS was not gated: report=%+v puts=%d", report.GCS, fake.puts)
	}
}

func TestProbeFailureModes(t *testing.T) {
	tests := []struct {
		name       string
		headStatus int
		headSize   int
		body       []byte
		sidecar    string
		max        int64
		wantStatus string
		wantGets   int32
	}{
		{name: "HEAD rejection", headStatus: http.StatusNotFound, body: []byte("ok"), max: 20, wantStatus: "failed", wantGets: 0},
		{name: "known oversized", headStatus: http.StatusOK, headSize: 21, body: []byte("ok"), max: 20, wantStatus: "oversized", wantGets: 0},
		{name: "unknown streaming oversized", headStatus: http.StatusMethodNotAllowed, body: []byte("this body exceeds ten bytes"), max: 10, wantStatus: "oversized", wantGets: 1},
		{name: "checksum mismatch", headStatus: http.StatusOK, body: []byte("wrong body"), sidecar: probeMD5([]byte("right body")), max: 20, wantStatus: "checksum_mismatch", wantGets: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gets atomic.Int32
			if tt.sidecar == "" {
				tt.sidecar = probeMD5(tt.body)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/score-list":
					_, _ = io.WriteString(w, "PGS000001\n")
				case strings.HasSuffix(r.URL.Path, ".md5"):
					_, _ = fmt.Fprintln(w, tt.sidecar)
				case r.Method == http.MethodHead:
					if tt.headStatus != http.StatusOK {
						http.Error(w, "head response", tt.headStatus)
						return
					}
					size := tt.headSize
					if size == 0 {
						size = len(tt.body)
					}
					w.Header().Set("Content-Length", fmt.Sprint(size))
				default:
					gets.Add(1)
					if tt.headStatus == http.StatusMethodNotAllowed {
						w.WriteHeader(http.StatusOK)
						if f, ok := w.(http.Flusher); ok {
							f.Flush()
						}
					}
					_, _ = w.Write(tt.body)
				}
			}))
			defer srv.Close()
			a := newProbeTestApp(srv, 2)
			report, err := a.Probe(context.Background(), ProbeOptions{PGSIDs: []string{"PGS000001"}, MaxBytes: tt.max})
			if !errors.Is(err, ErrProbeFailed) {
				t.Fatalf("got %v, want probe failure", err)
			}
			if report.Scores[0].Status != tt.wantStatus || gets.Load() != tt.wantGets {
				t.Fatalf("score=%+v GETs=%d", report.Scores[0], gets.Load())
			}
		})
	}
}

func TestProbePermanentSidecar404IsNotRetried(t *testing.T) {
	var sidecarCalls atomic.Int32
	var headCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/score-list":
			_, _ = io.WriteString(w, "PGS000001\n")
		case strings.HasSuffix(r.URL.Path, ".md5"):
			sidecarCalls.Add(1)
			http.NotFound(w, r)
		case r.Method == http.MethodHead:
			headCalls.Add(1)
			w.Header().Set("Content-Length", "4")
		default:
			t.Fatal("scoring file was downloaded without a checksum")
		}
	}))
	defer srv.Close()
	a := newProbeTestApp(srv, 2)
	report, err := a.Probe(context.Background(), ProbeOptions{PGSIDs: []string{"PGS000001"}, MaxBytes: 20})
	if !errors.Is(err, ErrProbeFailed) || sidecarCalls.Load() != 1 || headCalls.Load() != 1 {
		t.Fatalf("report=%+v err=%v sidecars=%d heads=%d", report, err, sidecarCalls.Load(), headCalls.Load())
	}
}

func TestProbeValidatesExplicitIDsBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()
	a := newProbeTestApp(srv, 1)
	for _, ids := range [][]string{nil, {"bad"}, {"PGS000001", "PGS000001"}} {
		if _, err := a.Probe(context.Background(), ProbeOptions{PGSIDs: ids, MaxBytes: 10}); err == nil {
			t.Fatalf("invalid IDs %v were accepted", ids)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("validation made %d network request(s)", calls.Load())
	}
}

func TestStoreSmokeTestUsesConditionalLifecycle(t *testing.T) {
	fake := newMemoryStore()
	report, err := runStoreSmokeTest(context.Background(), fake, "run-one", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if report.CreateGeneration == 0 || report.ReplacementGeneration <= report.CreateGeneration || !report.ReadChecksumVerified || !report.StaleGenerationRejected || !report.AbsenceConfirmed {
		t.Fatalf("incomplete report: %+v", report)
	}
	if len(fake.objects) != 0 || fake.puts != 3 || fake.opens != 1 || fake.deletes != 1 || fake.stats != 1 {
		t.Fatalf("unexpected fake store state: %+v", fake)
	}
}

type memoryObject struct {
	body       []byte
	generation int64
}

type memoryStore struct {
	mu        sync.Mutex
	objects   map[string]memoryObject
	next      int64
	puts      int
	opens     int
	deletes   int
	stats     int
	stalePuts int
}

func newMemoryStore() *memoryStore  { return &memoryStore{objects: make(map[string]memoryObject)} }
func (s *memoryStore) Name() string { return "gcs://fake/pgsc_mirror" }
func (s *memoryStore) Close() error { return nil }

func (s *memoryStore) Open(_ context.Context, key string) (io.ReadCloser, store.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens++
	o, ok := s.objects[key]
	if !ok {
		return nil, store.ObjectInfo{}, store.ErrNotFound
	}
	sum := md5.Sum(o.body)
	return io.NopCloser(bytes.NewReader(o.body)), store.ObjectInfo{Key: key, Size: int64(len(o.body)), MD5: sum[:], Generation: o.generation}, nil
}

func (s *memoryStore) Stat(_ context.Context, key string) (store.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats++
	o, ok := s.objects[key]
	if !ok {
		return store.ObjectInfo{}, store.ErrNotFound
	}
	return store.ObjectInfo{Key: key, Size: int64(len(o.body)), Generation: o.generation}, nil
}

func (s *memoryStore) Put(_ context.Context, key string, r io.Reader, opts store.PutOptions) (store.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	current, exists := s.objects[key]
	if opts.DoesNotExist && exists {
		return store.ObjectInfo{}, store.ErrPrecondition
	}
	if opts.GenerationMatch != nil && (!exists || current.generation != *opts.GenerationMatch) {
		s.stalePuts++
		return store.ObjectInfo{}, store.ErrPrecondition
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	s.next++
	s.objects[key] = memoryObject{body: append([]byte(nil), body...), generation: s.next}
	sum := md5.Sum(body)
	return store.ObjectInfo{Key: key, Size: int64(len(body)), MD5: sum[:], Generation: s.next}, nil
}

func (s *memoryStore) Delete(_ context.Context, key string, opts store.DeleteOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	current, exists := s.objects[key]
	if !exists {
		return store.ErrNotFound
	}
	if opts.GenerationMatch != nil && current.generation != *opts.GenerationMatch {
		return store.ErrPrecondition
	}
	delete(s.objects, key)
	return nil
}

func (s *memoryStore) List(context.Context, string) ([]store.ObjectInfo, error) { return nil, nil }
