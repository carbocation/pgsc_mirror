// Package catalog parses and fetches PGS Catalog inventory data.
package catalog

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/carbocation/pgsc_mirror/internal/transfer"
)

var (
	idPattern  = regexp.MustCompile(`^PGS[0-9]{6}$`)
	md5Pattern = regexp.MustCompile(`(?i)^[0-9a-f]{32}$`)
)

type Client struct {
	baseURLs    []string
	scoreList   string
	metadataCSV string
	http        *transfer.HTTPClient
}

type Document struct {
	Body         []byte
	URL          string
	ETag         string
	LastModified string
	NotModified  bool
}

type ScoreObjectInfo struct {
	URL           string
	Size          int64
	SizeKnown     bool
	ETag          string
	LastModified  string
	HeadSupported bool
	StatusCode    int
}

// ScoreMetadata is the per-score descriptive subset of the catalog metadata
// that belongs directly in release manifests. The complete upstream CSV is
// still preserved alongside every release.
type ScoreMetadata struct {
	PGSName       string
	TraitReported string
	TraitMapped   string
	TraitEFO      string
	License       string
}

func New(baseURLs []string, scoreList, metadataCSV string, httpClient *transfer.HTTPClient) *Client {
	return &Client{baseURLs: baseURLs, scoreList: scoreList, metadataCSV: metadataCSV, http: httpClient}
}

func ParseScoreList(r io.Reader) ([]string, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n")
	seen := make(map[string]struct{})
	for n, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !idPattern.MatchString(line) {
			return nil, fmt.Errorf("invalid score ID on line %d: %q", n+1, line)
		}
		seen[line] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func ParseMD5(r io.Reader) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, 64<<10))
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 || !md5Pattern.MatchString(fields[0]) {
		return "", errors.New("invalid MD5 sidecar")
	}
	return strings.ToLower(fields[0]), nil
}

func ParseMetadata(r io.Reader) (map[string]ScoreMetadata, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	columns := map[string]int{}
	for i, h := range header {
		normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")))
		switch normalized {
		case "pgs_id", "pgs id", "polygenic score (pgs) id":
			columns["pgs_id"] = i
		case "pgs_name", "pgs name":
			columns["pgs_name"] = i
		case "trait_reported", "reported trait":
			columns["trait_reported"] = i
		case "trait_mapped", "mapped trait(s) (efo label)":
			columns["trait_mapped"] = i
		case "trait_efo", "mapped trait(s) (efo id)":
			columns["trait_efo"] = i
		case "license", "license_name", "license/terms of use":
			columns["license"] = i
		}
	}
	idCol, ok := columns["pgs_id"]
	if !ok {
		return nil, errors.New("metadata CSV has no pgs_id column")
	}
	out := make(map[string]ScoreMetadata)
	for {
		record, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if idCol >= len(record) {
			continue
		}
		id := strings.TrimSpace(record[idCol])
		if !idPattern.MatchString(id) {
			continue
		}
		value := func(name string) string {
			column, ok := columns[name]
			if !ok || column >= len(record) {
				return ""
			}
			return strings.TrimSpace(record[column])
		}
		out[id] = ScoreMetadata{
			PGSName:       value("pgs_name"),
			TraitReported: value("trait_reported"),
			TraitMapped:   value("trait_mapped"),
			TraitEFO:      value("trait_efo"),
			License:       value("license"),
		}
	}
	return out, nil
}

func (c *Client) URLs(rel string) []string {
	out := make([]string, 0, len(c.baseURLs))
	for _, base := range c.baseURLs {
		out = append(out, strings.TrimRight(base, "/")+"/"+strings.TrimLeft(rel, "/"))
	}
	return out
}

func ScorePath(id, genomeBuild string) string {
	name := id + "_hmPOS_" + genomeBuild + ".txt.gz"
	return path.Join("scores", id, "ScoringFiles", "Harmonized", name)
}

func ValidScoreID(id string) bool { return idPattern.MatchString(id) }

func (c *Client) fetch(ctx context.Context, rel, etag, modified string, max int64) (Document, error) {
	headers := make(http.Header)
	if etag != "" {
		headers.Set("If-None-Match", etag)
	}
	if modified != "" {
		headers.Set("If-Modified-Since", modified)
	}
	resp, err := c.http.Do(ctx, c.URLs(rel), headers)
	if err != nil {
		return Document{}, err
	}
	defer resp.Body.Close()
	d := Document{URL: resp.Request.URL.String(), ETag: resp.Header.Get("ETag"), LastModified: resp.Header.Get("Last-Modified")}
	if resp.StatusCode == http.StatusNotModified {
		d.NotModified = true
		return d, nil
	}
	if resp.StatusCode != http.StatusOK {
		return d, fmt.Errorf("GET %s: %s", d.URL, resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return d, err
	}
	if int64(len(b)) > max {
		return d, fmt.Errorf("GET %s exceeded %d-byte limit", d.URL, max)
	}
	d.Body = b
	return d, nil
}

func (c *Client) ScoreList(ctx context.Context, etag, modified string) (Document, []string, error) {
	d, err := c.fetch(ctx, c.scoreList, etag, modified, 32<<20)
	if err != nil || d.NotModified {
		return d, nil, err
	}
	ids, err := ParseScoreList(bytes.NewReader(d.Body))
	return d, ids, err
}

func (c *Client) Metadata(ctx context.Context, etag, modified string) (Document, map[string]ScoreMetadata, error) {
	d, err := c.fetch(ctx, c.metadataCSV, etag, modified, 512<<20)
	if err != nil || d.NotModified {
		return d, nil, err
	}
	metadata, err := ParseMetadata(bytes.NewReader(d.Body))
	return d, metadata, err
}

func (c *Client) Checksum(ctx context.Context, id, genomeBuild string) (string, string, error) {
	if !idPattern.MatchString(id) {
		return "", "", fmt.Errorf("invalid PGS ID %q", id)
	}
	rel := ScorePath(id, genomeBuild) + ".md5"
	d, err := c.fetch(ctx, rel, "", "", 64<<10)
	if err != nil {
		return "", strings.TrimSuffix(d.URL, ".md5"), err
	}
	sum, err := ParseMD5(bytes.NewReader(d.Body))
	if err != nil {
		return "", d.URL, err
	}
	return sum, strings.TrimSuffix(d.URL, ".md5"), nil
}

// InspectScore performs a HEAD request without reading the score body. Servers
// that do not implement HEAD are reported with an unknown size so callers can
// still rely on their streaming byte limit.
func (c *Client) InspectScore(ctx context.Context, id, genomeBuild string) (ScoreObjectInfo, error) {
	if !idPattern.MatchString(id) {
		return ScoreObjectInfo{}, fmt.Errorf("invalid PGS ID %q", id)
	}
	resp, err := c.http.DoMethod(ctx, http.MethodHead, c.URLs(ScorePath(id, genomeBuild)), make(http.Header))
	if err != nil {
		return ScoreObjectInfo{}, err
	}
	defer resp.Body.Close()
	info := ScoreObjectInfo{
		URL:           resp.Request.URL.String(),
		ETag:          resp.Header.Get("ETag"),
		LastModified:  resp.Header.Get("Last-Modified"),
		HeadSupported: true,
		StatusCode:    resp.StatusCode,
	}
	switch resp.StatusCode {
	case http.StatusOK:
		if resp.ContentLength >= 0 {
			info.Size = resp.ContentLength
			info.SizeKnown = true
		}
		return info, nil
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		info.HeadSupported = false
		return info, nil
	default:
		return info, fmt.Errorf("HEAD %s: %s", info.URL, resp.Status)
	}
}

func ValidateURL(raw string) error { _, err := url.ParseRequestURI(raw); return err }
