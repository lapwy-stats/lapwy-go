package lapwy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type gotBody struct {
	SDK    string `json:"sdk"`
	Events []struct {
		ID    string         `json:"id"`
		Time  int64          `json:"time"`
		User  string         `json:"user"`
		Name  string         `json:"name"`
		Props map[string]any `json:"props"`
	} `json:"events"`
}

type gotRequest struct {
	header http.Header
	path   string
	method string
	body   gotBody
	at     time.Time
}

// collector is a fake lapwy: it answers with the next status in answers
// (the last one repeats) and keeps every request.
type collector struct {
	t       *testing.T
	mu      sync.Mutex
	reqs    []gotRequest
	answers []int
	retry   string
	srv     *httptest.Server
}

func newCollector(t *testing.T, answers ...int) *collector {
	k := &collector{t: t, answers: answers}
	if len(k.answers) == 0 {
		k.answers = []int{http.StatusAccepted}
	}
	k.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var b gotBody
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Errorf("body is not JSON: %v: %s", err, raw)
		}
		k.mu.Lock()
		n := len(k.reqs)
		k.reqs = append(k.reqs, gotRequest{header: r.Header.Clone(), path: r.URL.Path, method: r.Method, body: b, at: time.Now()})
		status := k.answers[min(n, len(k.answers)-1)]
		retry := k.retry
		k.mu.Unlock()
		if retry != "" && status != http.StatusAccepted {
			w.Header().Set("Retry-After", retry)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(k.srv.Close)
	return k
}

func (k *collector) requests() []gotRequest {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]gotRequest(nil), k.reqs...)
}

func (k *collector) names() []string {
	var out []string
	for _, r := range k.requests() {
		for _, e := range r.body.Events {
			out = append(out, e.Name)
		}
	}
	return out
}

// logs keeps what the client logs.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logs) logger() *slog.Logger { return slog.New(slog.NewTextHandler(l, nil)) }

func client(t *testing.T, k *collector, l *logs, opts ...Option) *Client {
	if l == nil {
		l = &logs{}
	}
	all := append([]Option{WithEndpoint(k.srv.URL), WithLogger(l.logger()), WithFlushInterval(time.Hour), withBackoff(10 * time.Millisecond)}, opts...)
	c := New("lws_test", all...)
	t.Cleanup(func() { c.Close() })
	return c
}

func flush(t *testing.T, c *Client) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Flush(ctx)
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestHeadersAndBody(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil)
	before := time.Now().UnixMilli()
	c.Track("u_123", "purchase", Props{"plan": "pro", "revenue": 29, "currency": "EUR", "trial": false})
	c.Track("u_123", "api_call", nil)
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	reqs := k.requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	r := reqs[0]
	if r.method != http.MethodPost || r.path != "/v1/events" {
		t.Errorf("%s %s, want POST /v1/events", r.method, r.path)
	}
	for h, want := range map[string]string{
		"Authorization": "Bearer lws_test",
		"Content-Type":  "application/json",
		"User-Agent":    "lapwy-go/" + Version,
	} {
		if got := r.header.Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
	if r.body.SDK != "go-"+Version {
		t.Errorf("sdk = %q", r.body.SDK)
	}
	if len(r.body.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(r.body.Events))
	}
	e := r.body.Events[0]
	if !uuidV4.MatchString(e.ID) || e.ID == r.body.Events[1].ID {
		t.Errorf("ids %q %q are not distinct UUID v4", e.ID, r.body.Events[1].ID)
	}
	if e.Time < before || e.Time > time.Now().UnixMilli() {
		t.Errorf("time = %d, not now", e.Time)
	}
	if e.User != "u_123" || e.Name != "purchase" {
		t.Errorf("user, name = %q, %q", e.User, e.Name)
	}
	if e.Props["plan"] != "pro" || e.Props["revenue"] != float64(29) || e.Props["currency"] != "EUR" || e.Props["trial"] != false {
		t.Errorf("props = %v", e.Props)
	}
	if r.body.Events[1].Props != nil {
		t.Errorf("nil props sent as %v", r.body.Events[1].Props)
	}
}

func TestTrackAt(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil)
	at := time.Now().Add(-time.Hour)
	c.TrackAt("u_1", "signup", nil, at)
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	if got := k.requests()[0].body.Events[0].Time; got != at.UnixMilli() {
		t.Errorf("time = %d, want %d", got, at.UnixMilli())
	}
}

func TestBatching(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil, WithBatchSize(3))
	for i := 0; i < 7; i++ {
		c.Track("u_1", fmt.Sprint(i), nil)
	}
	// Three waiting start a send without Flush and before the hour is up.
	deadline := time.Now().Add(3 * time.Second)
	for len(k.requests()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(k.requests()); n < 2 {
		t.Fatalf("requests before Flush = %d, want 2", n)
	}
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	for _, r := range k.requests() {
		if len(r.body.Events) > 3 {
			t.Errorf("batch of %d, want at most 3", len(r.body.Events))
		}
	}
	if got := strings.Join(k.names(), ","); got != "0,1,2,3,4,5,6" {
		t.Errorf("names = %s", got)
	}
}

func TestInterval(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil, WithFlushInterval(50*time.Millisecond))
	c.Track("u_1", "a", nil)
	deadline := time.Now().Add(3 * time.Second)
	for len(k.requests()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(k.requests()) != 1 {
		t.Fatal("the interval did not send the event")
	}
}

func TestRetryAfter503(t *testing.T) {
	k := newCollector(t, http.StatusServiceUnavailable, http.StatusAccepted)
	k.retry = "1"
	c := client(t, k, nil)
	c.Track("u_1", "a", nil)
	start := time.Now()
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	reqs := k.requests()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(reqs))
	}
	if wait := reqs[1].at.Sub(reqs[0].at); wait < 900*time.Millisecond {
		t.Errorf("retried after %v, want Retry-After's 1s", wait)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("too slow")
	}
	if reqs[0].body.Events[0].ID != reqs[1].body.Events[0].ID {
		t.Error("the retry has a new event id")
	}
}

func TestRetryKeepsEvents(t *testing.T) {
	k := newCollector(t, 500, 502, 503, 500, 500, http.StatusAccepted)
	c := client(t, k, nil)
	c.Track("u_1", "a", nil)
	if err := flush(t, c); err == nil {
		t.Fatal("Flush after five 5xx answers returned no error")
	}
	if n := len(k.requests()); n != maxAttempts {
		t.Fatalf("requests = %d, want %d", n, maxAttempts)
	}
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	reqs := k.requests()
	if got := reqs[len(reqs)-1].body.Events; len(got) != 1 || got[0].ID != reqs[0].body.Events[0].ID {
		t.Errorf("the event was not kept for the next Flush: %+v", got)
	}
}

func TestNetworkErrorKeepsEvents(t *testing.T) {
	k := newCollector(t)
	l := &logs{}
	c := New("lws_test", WithEndpoint("http://127.0.0.1:1"), WithLogger(l.logger()), WithFlushInterval(time.Hour), withBackoff(time.Millisecond))
	defer c.Close()
	c.Track("u_1", "a", nil)
	if err := flush(t, c); err == nil {
		t.Fatal("no error without a server")
	}
	if c.waiting() != 1 {
		t.Fatalf("waiting = %d, want 1", c.waiting())
	}
	c.endpoint = k.srv.URL // nothing else runs: Flush holds the only send
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	if len(k.names()) != 1 {
		t.Error("the event was not sent once the server was there")
	}
}

func TestNoRetry(t *testing.T) {
	for _, status := range []int{400, 401, 413} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			k := newCollector(t, status)
			l := &logs{}
			c := client(t, k, l)
			c.Track("u_1", "a", nil)
			c.Track("u_1", "b", nil)
			if err := flush(t, c); err == nil || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Fatalf("err = %v, want one naming %d", err, status)
			}
			if err := flush(t, c); err != nil {
				t.Fatalf("second Flush: %v", err)
			}
			if n := len(k.requests()); n != 1 {
				t.Errorf("requests = %d, want 1", n)
			}
			if n := strings.Count(l.String(), "batch refused"); n != 1 {
				t.Errorf("logged %d times, want once:\n%s", n, l)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	k := newCollector(t)
	l := &logs{}
	c := client(t, k, l)
	many := Props{}
	for i := 0; i < 31; i++ {
		many[fmt.Sprint(i)] = i
	}
	c.Track("", "a", nil)
	c.Track("  ", "a", nil)
	c.Track("u_1", "", nil)
	c.Track(strings.Repeat("u", 201), "a", nil)
	c.Track("ann@example.com", "a", nil)
	c.Track("u_1", strings.Repeat("n", 101), nil)
	c.Track("u_1", "a", many)
	c.Track("u_1", "a", Props{"when": time.Now()})
	c.Track("u_1", "a", Props{"list": []string{"x"}})
	c.TrackAt("u_1", "a", nil, time.Now().Add(-8*24*time.Hour))
	c.Track(strings.Repeat("u", 200), "ok", Props{"n": 1.5, "s": "x", "b": true, "i": int64(3)})
	c.Track("team@acme", "ok", nil) // no dot after the @: not an email
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(k.names(), ","); got != "ok,ok" {
		t.Errorf("sent %q, want only the two valid events", got)
	}
	if n := strings.Count(l.String(), "event dropped"); n != 10 {
		t.Errorf("warnings = %d, want 10:\n%s", n, l)
	}
}

func TestQueueCap(t *testing.T) {
	k := newCollector(t)
	l := &logs{}
	c := client(t, k, l, withMaxQueue(5))
	for i := 0; i < 8; i++ {
		c.Track("u_1", fmt.Sprint(i), nil)
	}
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(k.names(), ","); got != "3,4,5,6,7" {
		t.Errorf("names = %s, want the newest five", got)
	}
	if n := strings.Count(l.String(), "queue full"); n != 1 {
		t.Errorf("logged %d times, want once", n)
	}
}

func TestClose(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil)
	c.Track("u_1", "a", nil)
	c.Track("u_1", "b", nil)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(k.names(), ","); got != "a,b" {
		t.Errorf("Close sent %q", got)
	}
	c.Track("u_1", "after", nil)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := flush(t, c); err != nil {
		t.Fatal(err)
	}
	if len(k.names()) != 2 {
		t.Error("Track after Close was sent")
	}
}

func TestCloseDuringRetry(t *testing.T) {
	k := newCollector(t, http.StatusServiceUnavailable)
	k.retry = "60"
	c := client(t, k, nil, WithBatchSize(1))
	c.Track("u_1", "a", nil) // the worker sends it and waits 60s to retry
	deadline := time.Now().Add(3 * time.Second)
	for len(k.requests()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	if err := c.Close(); err == nil {
		t.Error("Close against a busy server returned no error")
	}
	if d := time.Since(start); d > closeTimeout+time.Second {
		t.Errorf("Close took %v", d)
	}
}

func TestEmptyKey(t *testing.T) {
	l := &logs{}
	c := New("", WithLogger(l.logger()))
	for i := 0; i < 3; i++ {
		c.Track("u_1", "a", nil)
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(l.String(), "level=WARN"); n != 1 {
		t.Errorf("warnings = %d, want 1:\n%s", n, l)
	}
}

func TestConcurrent(t *testing.T) {
	k := newCollector(t)
	c := client(t, k, nil, WithBatchSize(50), WithFlushInterval(5*time.Millisecond))
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.Track(fmt.Sprintf("u_%d", g), "e", Props{"i": i})
				if i%25 == 0 {
					_ = flush(t, c)
				}
			}
		}()
	}
	wg.Wait()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, r := range k.requests() {
		if len(r.body.Events) > 50 {
			t.Errorf("batch of %d", len(r.body.Events))
		}
		for _, e := range r.body.Events {
			ids[e.ID] = true
		}
	}
	if len(ids) != 2000 {
		t.Errorf("distinct events = %d, want 2000", len(ids))
	}
}

func TestRetryAfterParse(t *testing.T) {
	if d := parseRetryAfter("3"); d != 3*time.Second {
		t.Errorf("3 -> %v", d)
	}
	if d := parseRetryAfter(time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)); d < 8*time.Second || d > 10*time.Second {
		t.Errorf("date -> %v", d)
	}
	if d := parseRetryAfter("soon"); d != 0 {
		t.Errorf("soon -> %v", d)
	}
}
