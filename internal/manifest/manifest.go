// Package manifest produces deterministic compressed release manifests.
package manifest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/pgsc-mirror/pgsc-mirror/internal/model"
)

func sorted(entries []model.Entry) []model.Entry {
	out := append([]model.Entry(nil), entries...)
	sort.Slice(out, func(i, j int) bool { return out[i].PGSID < out[j].PGSID })
	return out
}

// ReleaseID combines a UTC timestamp with a digest of the desired logical state.
func ReleaseID(now time.Time, entries []model.Entry, snapshots ...[]byte) (string, error) {
	type seed struct {
		PGSID, GenomeBuild, SourceURL, SourceMD5, BlobKey, Status, License string
		SizeBytes                                                          int64
	}
	ss := make([]seed, 0, len(entries))
	for _, e := range sorted(entries) {
		ss = append(ss, seed{e.PGSID, e.GenomeBuild, e.SourceURL, e.SourceMD5, e.BlobKey, e.Status, e.License, e.SizeBytes})
	}
	b, err := json.Marshal(ss)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write(b)
	for _, snapshot := range snapshots {
		sum := sha256.Sum256(snapshot)
		_, _ = h.Write(sum[:])
	}
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(h.Sum(nil)[:6]), nil
}

func Encode(entries []model.Entry) ([]byte, string, error) {
	var out bytes.Buffer
	gz, err := gzip.NewWriterLevel(&out, gzip.BestCompression)
	if err != nil {
		return nil, "", err
	}
	gz.Header.ModTime = time.Unix(0, 0).UTC()
	gz.Header.OS = 255
	enc := json.NewEncoder(gz)
	enc.SetEscapeHTML(false)
	for _, e := range sorted(entries) {
		if err := enc.Encode(e); err != nil {
			return nil, "", err
		}
	}
	if err := gz.Close(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(out.Bytes())
	return out.Bytes(), hex.EncodeToString(sum[:]), nil
}

func Decode(r io.Reader) ([]model.Entry, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open manifest gzip: %w", err)
	}
	defer gz.Close()
	s := bufio.NewScanner(gz)
	s.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var entries []model.Entry
	line := 0
	for s.Scan() {
		line++
		var e model.Entry
		if err := json.Unmarshal(s.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("manifest line %d: %w", line, err)
		}
		entries = append(entries, e)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func PointerJSON(p model.Pointer) ([]byte, error) {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
