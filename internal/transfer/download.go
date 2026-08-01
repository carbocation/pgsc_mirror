package transfer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	ErrChecksumMismatch = errors.New("download checksum mismatch")
	ErrSizeLimit        = errors.New("download exceeds size limit")
)

type DownloadResult struct {
	Path         string
	SourceURL    string
	MD5          string
	Size         int64
	ETag         string
	LastModified string
	Attempts     int
}

type partialMeta struct {
	SourceURL    string `json:"source_url"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	TotalSize    int64  `json:"total_size,omitempty"`
}

func (c *HTTPClient) Download(ctx context.Context, urls []string, partPath, expectedMD5 string) (DownloadResult, error) {
	return c.DownloadBounded(ctx, urls, partPath, expectedMD5, 0)
}

// DownloadBounded downloads and verifies an object while enforcing a maximum
// total byte count. A non-positive maxBytes disables the limit.
func (c *HTTPClient) DownloadBounded(ctx context.Context, urls []string, partPath, expectedMD5 string, maxBytes int64) (DownloadResult, error) {
	if len(urls) == 0 {
		return DownloadResult{}, errors.New("no download URLs")
	}
	if len(expectedMD5) != 32 {
		return DownloadResult{}, fmt.Errorf("invalid expected MD5 %q", expectedMD5)
	}
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		return DownloadResult{}, err
	}
	metaPath := partPath + ".json"
	meta, _ := readPartialMeta(metaPath)
	if _, err := os.Stat(partPath); errors.Is(err, os.ErrNotExist) {
		meta = partialMeta{}
	}
	var last error
	for attempt := 0; attempt < c.policy.Attempts; attempt++ {
		size := fileSize(partPath)
		if maxBytes > 0 && size > maxBytes {
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			return DownloadResult{}, fmt.Errorf("%w: partial file is %d bytes, limit is %d", ErrSizeLimit, size, maxBytes)
		}
		if size > 0 && meta.SourceURL == "" {
			_ = os.Truncate(partPath, 0)
			size = 0
		}
		u := urls[attempt%len(urls)]
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return DownloadResult{}, err
		}
		req.Header.Set("User-Agent", c.userAgent)
		if size > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", size))
			if meta.ETag != "" {
				req.Header.Set("If-Range", meta.ETag)
			} else if meta.LastModified != "" {
				req.Header.Set("If-Range", meta.LastModified)
			}
		}
		resp, err := c.client.Do(req)
		if err != nil {
			last = err
			if err := c.waitRetry(ctx, attempt, 0); err != nil {
				return DownloadResult{}, err
			}
			continue
		}
		if retryable(resp.StatusCode) {
			last = fmt.Errorf("GET %s: %s", u, resp.Status)
			wait := retryAfter(resp.Header.Get("Retry-After"), time.Now())
			io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
			if err := c.waitRetry(ctx, attempt, wait); err != nil {
				return DownloadResult{}, err
			}
			continue
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			resp.Body.Close()
			_ = os.Truncate(partPath, 0)
			_ = os.Remove(metaPath)
			meta = partialMeta{}
			last = errors.New("upstream rejected resume range")
			if err := c.waitRetry(ctx, attempt, 0); err != nil {
				return DownloadResult{}, err
			}
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return DownloadResult{}, fmt.Errorf("GET %s: %s", resp.Request.URL, resp.Status)
		}
		appendMode := size > 0 && resp.StatusCode == http.StatusPartialContent
		if appendMode {
			if !validContentRange(resp.Header.Get("Content-Range"), size) || changedValidator(meta, resp) {
				resp.Body.Close()
				_ = os.Truncate(partPath, 0)
				_ = os.Remove(metaPath)
				meta = partialMeta{}
				last = errors.New("upstream validator changed while resuming")
				if err := c.waitRetry(ctx, attempt, 0); err != nil {
					return DownloadResult{}, err
				}
				continue
			}
		} else {
			size = 0
			if err := os.Truncate(partPath, 0); err != nil && !errors.Is(err, os.ErrNotExist) {
				resp.Body.Close()
				return DownloadResult{}, err
			}
		}
		if maxBytes > 0 && resp.ContentLength >= 0 && size+resp.ContentLength > maxBytes {
			resp.Body.Close()
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			return DownloadResult{}, fmt.Errorf("%w: advertised total is %d bytes, limit is %d", ErrSizeLimit, size+resp.ContentLength, maxBytes)
		}
		flags := os.O_CREATE | os.O_WRONLY
		if appendMode {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
		}
		f, err := os.OpenFile(partPath, flags, 0o644)
		if err != nil {
			resp.Body.Close()
			return DownloadResult{}, err
		}
		meta = partialMeta{SourceURL: resp.Request.URL.String(), ETag: resp.Header.Get("ETag"), LastModified: resp.Header.Get("Last-Modified"), TotalSize: responseTotal(resp, size)}
		if err := writePartialMeta(metaPath, meta); err != nil {
			f.Close()
			resp.Body.Close()
			return DownloadResult{}, err
		}
		var copied int64
		var copyErr error
		if maxBytes > 0 {
			remaining := maxBytes - size
			copied, copyErr = io.Copy(f, io.LimitReader(resp.Body, remaining+1))
			if copyErr == nil && copied > remaining {
				copyErr = fmt.Errorf("%w: response exceeded %d bytes", ErrSizeLimit, maxBytes)
			}
		} else {
			copied, copyErr = io.Copy(f, resp.Body)
		}
		closeErr := f.Close()
		resp.Body.Close()
		if errors.Is(copyErr, ErrSizeLimit) {
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			return DownloadResult{}, copyErr
		}
		if copyErr != nil || closeErr != nil {
			last = errors.Join(copyErr, closeErr)
			if err := c.waitRetry(ctx, attempt, 0); err != nil {
				return DownloadResult{}, err
			}
			continue
		}
		gotSize := fileSize(partPath)
		if meta.TotalSize > 0 && gotSize != meta.TotalSize {
			last = fmt.Errorf("short download: got %d bytes, want %d", gotSize, meta.TotalSize)
			if gotSize > meta.TotalSize {
				_ = os.Truncate(partPath, 0)
				_ = os.Remove(metaPath)
				meta = partialMeta{}
			}
			if err := c.waitRetry(ctx, attempt, 0); err != nil {
				return DownloadResult{}, err
			}
			continue
		}
		gotMD5, err := fileMD5(partPath)
		if err != nil {
			return DownloadResult{}, err
		}
		if !strings.EqualFold(gotMD5, expectedMD5) {
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			return DownloadResult{}, fmt.Errorf("%w: got %s, want %s", ErrChecksumMismatch, gotMD5, expectedMD5)
		}
		_ = os.Remove(metaPath)
		return DownloadResult{Path: partPath, SourceURL: meta.SourceURL, MD5: gotMD5, Size: gotSize, ETag: meta.ETag, LastModified: meta.LastModified, Attempts: attempt + 1}, nil
	}
	return DownloadResult{}, fmt.Errorf("download failed after %d attempts: %w", c.policy.Attempts, last)
}

func (c *HTTPClient) waitRetry(ctx context.Context, attempt int, explicit time.Duration) error {
	if attempt+1 >= c.policy.Attempts {
		return nil
	}
	if explicit <= 0 {
		explicit = c.backoff(attempt)
	}
	return c.sleep(ctx, explicit)
}

func fileSize(name string) int64 {
	fi, err := os.Stat(name)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func fileMD5(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func readPartialMeta(name string) (partialMeta, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return partialMeta{}, err
	}
	var m partialMeta
	err = json.Unmarshal(b, &m)
	return m, err
}

func writePartialMeta(name string, m partialMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := name + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}

func changedValidator(m partialMeta, resp *http.Response) bool {
	if m.ETag != "" && resp.Header.Get("ETag") != "" {
		return m.ETag != resp.Header.Get("ETag")
	}
	if m.LastModified != "" && resp.Header.Get("Last-Modified") != "" {
		return m.LastModified != resp.Header.Get("Last-Modified")
	}
	return false
}

func validContentRange(raw string, start int64) bool {
	return strings.HasPrefix(raw, "bytes "+strconv.FormatInt(start, 10)+"-")
}

func responseTotal(resp *http.Response, already int64) int64 {
	if raw := resp.Header.Get("Content-Range"); raw != "" {
		if slash := strings.LastIndexByte(raw, '/'); slash >= 0 {
			if n, err := strconv.ParseInt(raw[slash+1:], 10, 64); err == nil {
				return n
			}
		}
	}
	if resp.ContentLength >= 0 {
		return already + resp.ContentLength
	}
	return 0
}
