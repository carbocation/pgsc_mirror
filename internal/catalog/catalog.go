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

	"github.com/pgsc-mirror/pgsc-mirror/internal/transfer"
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

func ParseLicenses(r io.Reader) (map[string]string, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	idCol, licenseCol := -1, -1
	for i, h := range header {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "pgs_id", "pgs id":
			idCol = i
		case "license", "license_name":
			licenseCol = i
		}
	}
	if idCol < 0 {
		return nil, errors.New("metadata CSV has no pgs_id column")
	}
	out := make(map[string]string)
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
		license := ""
		if licenseCol >= 0 && licenseCol < len(record) {
			license = strings.TrimSpace(record[licenseCol])
		}
		out[id] = license
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

func (c *Client) Metadata(ctx context.Context, etag, modified string) (Document, map[string]string, error) {
	d, err := c.fetch(ctx, c.metadataCSV, etag, modified, 512<<20)
	if err != nil || d.NotModified {
		return d, nil, err
	}
	licenses, err := ParseLicenses(bytes.NewReader(d.Body))
	return d, licenses, err
}

func (c *Client) Checksum(ctx context.Context, id, genomeBuild string) (string, string, error) {
	if !idPattern.MatchString(id) {
		return "", "", fmt.Errorf("invalid PGS ID %q", id)
	}
	rel := ScorePath(id, genomeBuild) + ".md5"
	d, err := c.fetch(ctx, rel, "", "", 64<<10)
	if err != nil {
		return "", "", err
	}
	sum, err := ParseMD5(bytes.NewReader(d.Body))
	if err != nil {
		return "", d.URL, err
	}
	return sum, strings.TrimSuffix(d.URL, ".md5"), nil
}

func ValidateURL(raw string) error { _, err := url.ParseRequestURI(raw); return err }
