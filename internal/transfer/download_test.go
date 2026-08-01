package transfer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func checksum(b []byte) string { s := md5.Sum(b); return hex.EncodeToString(s[:]) }

func testClient() *HTTPClient {
	c := NewHTTPClient(5*time.Second, Policy{Attempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, "test")
	c.sleep = func(context.Context, time.Duration) error { return nil }
	return c
}

func TestDownloadResumes(t *testing.T) {
	body := []byte("abcdefghijklmnopqrstuvwxyz")
	var sawRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"v1\"")
		if r.Header.Get("Range") == "bytes=10-" {
			sawRange.Store(true)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 10-%d/%d", len(body)-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(body[10:])
			return
		}
		w.Write(body)
	}))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "x.part")
	if err := os.WriteFile(part, body[:10], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePartialMeta(part+".json", partialMeta{SourceURL: srv.URL, ETag: "\"v1\"", TotalSize: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	result, err := testClient().Download(context.Background(), []string{srv.URL}, part, checksum(body))
	if err != nil {
		t.Fatal(err)
	}
	if !sawRange.Load() || result.Size != int64(len(body)) {
		t.Fatalf("resume not used: range=%v result=%+v", sawRange.Load(), result)
	}
}

func TestDownloadResumesAfterInterruptedResponse(t *testing.T) {
	body := []byte("a response long enough to interrupt and resume")
	var calls atomic.Int32
	var sawRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"stable\"")
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body[:12])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
		if r.Header.Get("Range") == "bytes=12-" {
			sawRange.Store(true)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 12-%d/%d", len(body)-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[12:])
			return
		}
		http.Error(w, "unexpected range", http.StatusBadRequest)
	}))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "interrupted.part")
	result, err := testClient().Download(context.Background(), []string{srv.URL}, part, checksum(body))
	if err != nil {
		t.Fatal(err)
	}
	if !sawRange.Load() || result.Size != int64(len(body)) {
		t.Fatalf("did not resume: %+v range=%v", result, sawRange.Load())
	}
}

func TestDownloadRestartsWhenETagChanges(t *testing.T) {
	body := []byte("new complete representation")
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", "\"new\"")
		w.Write(body)
	}))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "x.part")
	os.WriteFile(part, []byte("old"), 0o644)
	writePartialMeta(part+".json", partialMeta{SourceURL: srv.URL, ETag: "\"old\"", TotalSize: 20})
	result, err := testClient().Download(context.Background(), []string{srv.URL}, part, checksum(body))
	if err != nil {
		t.Fatal(err)
	}
	if result.ETag != "\"new\"" || requests.Load() != 1 {
		t.Fatalf("unexpected result=%+v requests=%d", result, requests.Load())
	}
}

func TestDownloadChecksumMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("bad")) }))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "x.part")
	_, err := testClient().Download(context.Background(), []string{srv.URL}, part, checksum([]byte("good")))
	if err == nil {
		t.Fatal("checksum mismatch was accepted")
	}
	if _, statErr := os.Stat(part); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("bad partial was retained: %v", statErr)
	}
}

func TestDownloadBoundedRejectsAdvertisedSize(t *testing.T) {
	body := []byte("too large")
	var bodyWrites atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		bodyWrites.Add(1)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "bounded.part")
	_, err := testClient().DownloadBounded(context.Background(), []string{srv.URL}, part, checksum(body), int64(len(body)-1))
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("got %v, want size-limit error", err)
	}
	if _, statErr := os.Stat(part); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized partial was retained: %v", statErr)
	}
	if bodyWrites.Load() != 1 {
		t.Fatalf("unexpected request count %d", bodyWrites.Load())
	}
}

func TestDownloadBoundedEnforcesLimitWithoutContentLength(t *testing.T) {
	body := []byte("a chunked response beyond the limit")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	part := filepath.Join(t.TempDir(), "bounded.part")
	_, err := testClient().DownloadBounded(context.Background(), []string{srv.URL}, part, checksum(body), 8)
	if !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("got %v, want size-limit error", err)
	}
	if _, statErr := os.Stat(part); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized partial was retained: %v", statErr)
	}
}

func TestRetryAfterAndPermanent404(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			http.Error(w, "busy", 429)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := testClient()
	var waited time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { waited = d; return nil }
	resp, err := c.Do(context.Background(), []string{srv.URL}, make(http.Header))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if waited != 2*time.Second {
		t.Fatalf("waited %s", waited)
	}
	var notFoundCalls atomic.Int32
	nf := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { notFoundCalls.Add(1); http.NotFound(w, r) }))
	defer nf.Close()
	resp, err = c.Do(context.Background(), []string{nf.URL}, make(http.Header))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if notFoundCalls.Load() != 1 {
		t.Fatalf("404 retried %d times", notFoundCalls.Load())
	}
}
