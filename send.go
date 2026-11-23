package lapwy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxBackoff    = 30 * time.Second
	maxRetryAfter = 5 * time.Minute
)

// sendAll sends the queue in batches until it is empty or a send fails.
func (c *Client) sendAll(ctx context.Context, background bool) error {
	select {
	case c.sending <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.sending }()

	for {
		batch := c.take()
		if len(batch) == 0 {
			return nil
		}
		keep, err := c.post(ctx, batch)
		if err == nil {
			continue
		}
		if keep {
			c.putBack(batch)
			if background {
				c.log.Warn("lapwy: send failed, will try again", "events", len(batch), "error", err)
			}
		} else {
			c.log.Error("lapwy: batch refused, events dropped", "events", len(batch), "error", err)
		}
		return err
	}
}

// post sends one batch, retrying passing failures with backoff. keep says
// whether the events should stay queued after an error.
func (c *Client) post(ctx context.Context, batch [][]byte) (keep bool, err error) {
	body := encodeBody(batch)
	for attempt := 0; ; attempt++ {
		status, retryAfter, err := c.do(ctx, body)
		switch {
		case err == nil && status >= 200 && status < 300:
			return false, nil
		case err == nil && status != http.StatusTooManyRequests && status != http.StatusRequestTimeout && status < 500:
			return false, fmt.Errorf("lapwy: %s", explain(status))
		}
		if err == nil {
			err = fmt.Errorf("lapwy: %s", explain(status))
		}
		delay := retryAfter
		if delay <= 0 {
			delay = min(c.backoff<<attempt, maxBackoff)
			delay += time.Duration(mrand.Int64N(int64(delay)/5 + 1))
		}
		if ctx.Err() != nil || attempt+1 >= maxAttempts {
			c.mu.Lock()
			c.pauseUntil = time.Now().Add(delay)
			c.mu.Unlock()
			return true, err
		}
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return true, err
		}
	}
}

// do makes one request and returns the status and the Retry-After wait.
func (c *Client) do(ctx context.Context, body []byte) (int, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/events", bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "lapwy-go/"+Version)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode, parseRetryAfter(resp.Header.Get("Retry-After")), nil
}

func encodeBody(batch [][]byte) []byte {
	var b bytes.Buffer
	b.WriteString(`{"sdk":"go-` + Version + `","events":[`)
	for i, e := range batch {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(e)
	}
	b.WriteString("]}")
	return b.Bytes()
}

func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	if s, err := strconv.Atoi(v); err == nil {
		d = time.Duration(s) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
	}
	return max(0, min(d, maxRetryAfter))
}

func explain(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "400: lapwy could not read the batch"
	case http.StatusUnauthorized:
		return "401: the server key is missing or unknown"
	case http.StatusRequestEntityTooLarge:
		return "413: the batch is too large"
	case http.StatusTooManyRequests:
		return "429: too many requests"
	case http.StatusServiceUnavailable:
		return "503: lapwy is busy"
	}
	return strconv.Itoa(status) + " " + http.StatusText(status)
}
