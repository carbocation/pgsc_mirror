// Package transfer provides bounded, retrying, resumable HTTP transfers.
package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Policy struct {
	Attempts       int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type HTTPClient struct {
	client    *http.Client
	policy    Policy
	userAgent string
	rng       *rand.Rand
	mu        sync.Mutex
	sleep     func(context.Context, time.Duration) error
}

func NewHTTPClient(timeout time.Duration, policy Policy, userAgent string) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{Timeout: timeout}, policy: policy, userAgent: userAgent,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())), sleep: sleepContext,
	}
}

func (c *HTTPClient) WithClient(client *http.Client) *HTTPClient { c.client = client; return c }

func (c *HTTPClient) Do(ctx context.Context, urls []string, headers http.Header) (*http.Response, error) {
	return c.DoMethod(ctx, http.MethodGet, urls, headers)
}

// DoMethod performs a retrying GET or HEAD request against ordered fallback URLs.
func (c *HTTPClient) DoMethod(ctx context.Context, method string, urls []string, headers http.Header) (*http.Response, error) {
	if len(urls) == 0 {
		return nil, errors.New("no upstream URLs")
	}
	if method != http.MethodGet && method != http.MethodHead {
		return nil, fmt.Errorf("unsupported HTTP method %q", method)
	}
	var last error
	for attempt := 0; attempt < c.policy.Attempts; attempt++ {
		u := urls[attempt%len(urls)]
		req, err := http.NewRequestWithContext(ctx, method, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header = headers.Clone()
		req.Header.Set("User-Agent", c.userAgent)
		resp, err := c.client.Do(req)
		if err == nil && !retryable(resp.StatusCode) {
			return resp, nil
		}
		var wait time.Duration
		if err == nil {
			last = fmt.Errorf("%s: %s", u, resp.Status)
			wait = retryAfter(resp.Header.Get("Retry-After"), time.Now())
			io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			resp.Body.Close()
		} else {
			last = err
		}
		if attempt+1 < c.policy.Attempts {
			if wait <= 0 {
				wait = c.backoff(attempt)
			}
			if err := c.sleep(ctx, wait); err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("upstream request failed after %d attempts: %w", c.policy.Attempts, last)
}

func retryable(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooEarly || code == http.StatusTooManyRequests || code >= 500
}

func retryAfter(raw string, now time.Time) time.Duration {
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(raw); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func (c *HTTPClient) backoff(attempt int) time.Duration {
	d := c.policy.InitialBackoff
	for i := 0; i < attempt && d < c.policy.MaxBackoff/2; i++ {
		d *= 2
	}
	if d > c.policy.MaxBackoff {
		d = c.policy.MaxBackoff
	}
	if d <= 0 {
		return 0
	}
	c.mu.Lock()
	jitter := time.Duration(c.rng.Int63n(int64(d/2) + 1))
	c.mu.Unlock()
	return d/2 + jitter
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
