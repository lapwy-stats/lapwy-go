// Package lapwy sends server events to lapwy (https://lapwy.com): business
// events from your backend, such as a payment, an API call or an MCP tool used,
// about one of your signed-in users.
//
// The user id is the same id your site passes to identify() in the browser, so
// lapwy links the server events to that person's visits.
//
//	c := lapwy.New(os.Getenv("LAPWY_SERVER_KEY"))
//	defer c.Close()
//	c.Track("u_123", "purchase", lapwy.Props{"plan": "pro", "revenue": 29, "currency": "EUR"})
//
// Track never waits for the network. Events wait in memory and a background
// goroutine sends them in batches. See https://lapwy.com/docs#go.
package lapwy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Version is the version of this package. It is sent with every batch.
const Version = "0.1.0"

// DefaultEndpoint is where events go unless WithEndpoint says otherwise.
const DefaultEndpoint = "https://i.lapwy.com"

const (
	defaultFlushInterval = time.Second
	defaultBatchSize     = 100
	maxBatchSize         = 500     // events per request
	maxBodyBytes         = 1 << 20 // bytes per request
	defaultMaxQueue      = 10000
	maxProps             = 30
	maxUserLen           = 200
	maxNameLen           = 100
	maxAge               = 7 * 24 * time.Hour
	closeTimeout         = 5 * time.Second
	maxAttempts          = 5
)

// Props are an event's properties: up to 30, each a string, a number or a bool.
// Money is "revenue" (a number) with a three-letter "currency", such as "EUR".
type Props map[string]any

// Option changes a Client made by New.
type Option func(*Client)

// WithEndpoint sends events to another address than https://i.lapwy.com.
func WithEndpoint(url string) Option {
	return func(c *Client) { c.endpoint = strings.TrimRight(url, "/") }
}

// WithHTTPClient sends events with your own *http.Client, for example one with
// a proxy or a timeout. The default has a 10 second timeout.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.http = hc
		}
	}
}

// WithLogger logs dropped events and failed sends to l instead of slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		if l != nil {
			c.log = l
		}
	}
}

// WithFlushInterval sets how often waiting events are sent. The default is one second.
func WithFlushInterval(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.interval = d
		}
	}
}

// WithBatchSize sets how many waiting events start a send before the interval
// is up, and the most sent in one request. The default is 100, the most 500.
func WithBatchSize(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.batchSize = min(n, maxBatchSize)
		}
	}
}

// withBackoff and withMaxQueue are for tests.
func withBackoff(d time.Duration) Option { return func(c *Client) { c.backoff = d } }
func withMaxQueue(n int) Option          { return func(c *Client) { c.maxQueue = n } }

// Client sends events to lapwy. Make one with New, share it, and Close it when
// the program ends. It is safe for concurrent use.
type Client struct {
	key       string
	endpoint  string
	http      *http.Client
	log       *slog.Logger
	interval  time.Duration
	batchSize int
	maxQueue  int
	backoff   time.Duration

	mu         sync.Mutex
	queue      [][]byte // events, already encoded
	closed     bool
	overflowed bool      // a drop for a full queue was logged; reset when it empties
	pauseUntil time.Time // the worker waits after a send that kept failing

	sending   chan struct{} // holds one token: one send at a time
	wake      chan struct{}
	quit      chan struct{}
	done      chan struct{}
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

// New returns a Client that sends with the server key (it starts with lws_).
// With an empty key, the Client drops every event and says so once in the log,
// so a missing environment variable never stops your program.
func New(key string, opts ...Option) *Client {
	c := &Client{
		key:       strings.TrimSpace(key),
		endpoint:  DefaultEndpoint,
		http:      &http.Client{Timeout: 10 * time.Second},
		log:       slog.Default(),
		interval:  defaultFlushInterval,
		batchSize: defaultBatchSize,
		maxQueue:  defaultMaxQueue,
		backoff:   500 * time.Millisecond,
		sending:   make(chan struct{}, 1),
		wake:      make(chan struct{}, 1),
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	if c.key == "" {
		c.log.Warn("lapwy: no server key, events are not sent")
		c.closed = true
		close(c.done)
		return c
	}
	c.bgCtx, c.bgCancel = context.WithCancel(context.Background())
	go c.run()
	return c
}

// Track queues an event that happens now: name, done by the user with this id,
// with optional props. It never waits for the network. An event that lapwy
// would refuse (empty user or name, a user id that is an email address, too
// long, or bad props) is dropped with a warning in the log.
func (c *Client) Track(user, name string, props Props) {
	c.TrackAt(user, name, props, time.Now())
}

// TrackAt is Track for an event that happened at another time, up to 7 days ago.
func (c *Client) TrackAt(user, name string, props Props, at time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return
	}
	raw, problem := encode(user, name, props, at)
	if problem != "" {
		c.log.Warn("lapwy: event dropped: "+problem, "user", trim(user, 64), "name", trim(name, 64))
		return
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.queue = append(c.queue, raw)
	c.dropOverflowLocked()
	full := len(c.queue) >= c.batchSize
	c.mu.Unlock()
	if full {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

// Flush sends every waiting event now and waits until they are accepted, ctx
// ends, or a send fails. Events that could not be sent for a passing reason
// (no network, lapwy busy) stay queued for the next try.
func (c *Client) Flush(ctx context.Context) error {
	if c == nil || c.key == "" {
		return nil
	}
	return c.sendAll(ctx, false)
}

// Close sends what is waiting, waiting up to 5 seconds, and stops the
// background goroutine. Track after Close does nothing. Close returns the
// error of that last send, if any; calling it again returns the same.
func (c *Client) Close() error {
	if c == nil || c.key == "" {
		return nil
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.quit)
		c.bgCancel() // a send in its backoff gives up; its events stay queued
		<-c.done
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		c.closeErr = c.sendAll(ctx, false)
		if n := c.waiting(); n > 0 {
			c.log.Warn("lapwy: events not sent before Close", "events", n)
		}
	})
	return c.closeErr
}

func (c *Client) run() {
	defer close(c.done)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
		case <-c.wake:
		}
		c.mu.Lock()
		paused := time.Now().Before(c.pauseUntil)
		c.mu.Unlock()
		if !paused {
			_ = c.sendAll(c.bgCtx, true)
		}
	}
}

func (c *Client) waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

// dropOverflowLocked keeps the queue at maxQueue by dropping the oldest events.
func (c *Client) dropOverflowLocked() {
	over := len(c.queue) - c.maxQueue
	if over <= 0 {
		return
	}
	clear(c.queue[:over])
	c.queue = c.queue[over:]
	if !c.overflowed {
		c.overflowed = true
		c.log.Warn("lapwy: queue full, dropping the oldest events", "limit", c.maxQueue)
	}
}

// take removes the next batch from the front of the queue.
func (c *Client) take() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	size := 64 // the wrapper around the events
	n := 0
	for n < len(c.queue) && n < c.batchSize {
		if size+len(c.queue[n])+1 > maxBodyBytes {
			break
		}
		size += len(c.queue[n]) + 1
		n++
	}
	batch := c.queue[:n:n]
	c.queue = c.queue[n:]
	if len(c.queue) == 0 {
		c.queue = nil
		c.overflowed = false
	}
	return batch
}

// putBack returns a batch that could not be sent to the front of the queue.
func (c *Client) putBack(batch [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	q := make([][]byte, 0, len(batch)+len(c.queue))
	q = append(append(q, batch...), c.queue...)
	c.queue = q
	c.dropOverflowLocked()
}

func encode(user, name string, props Props, at time.Time) ([]byte, string) {
	switch {
	case strings.TrimSpace(user) == "":
		return nil, "no user id"
	case strings.TrimSpace(name) == "":
		return nil, "no event name"
	case len(user) > maxUserLen:
		return nil, "user id over 200 characters"
	case looksLikeEmail(user):
		return nil, "user id is an email address; use your own account id"
	case len(name) > maxNameLen:
		return nil, "event name over 100 characters"
	case len(props) > maxProps:
		return nil, "more than 30 props"
	}
	for k, v := range props {
		if k == "" {
			return nil, "a prop with no name"
		}
		if !allowed(v) {
			return nil, "prop " + trim(k, 40) + " is not a string, number or bool"
		}
	}
	if at.IsZero() {
		at = time.Now()
	}
	if time.Since(at) > maxAge {
		return nil, "event older than 7 days"
	}
	raw, err := json.Marshal(wireEvent{ID: newID(), Time: at.UnixMilli(), User: user, Name: name, Props: props})
	if err != nil {
		return nil, "props cannot be encoded: " + err.Error()
	}
	if len(raw) > maxBodyBytes-128 {
		return nil, "event over 1 MB"
	}
	return raw, ""
}

type wireEvent struct {
	ID    string `json:"id"`
	Time  int64  `json:"time"`
	User  string `json:"user"`
	Name  string `json:"name"`
	Props Props  `json:"props,omitempty"`
}

func allowed(v any) bool {
	switch x := v.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float32:
		return !math.IsNaN(float64(x)) && !math.IsInf(float64(x), 0)
	case float64:
		return !math.IsNaN(x) && !math.IsInf(x, 0)
	case json.Number:
		return true
	}
	return false
}

func looksLikeEmail(s string) bool {
	at := strings.IndexByte(s, '@')
	if at <= 0 || at != strings.LastIndexByte(s, '@') || strings.ContainsAny(s, " \t\r\n") {
		return false
	}
	domain := s[at+1:]
	dot := strings.LastIndexByte(domain, '.')
	return dot > 0 && dot < len(domain)-1
}

// newID returns a random UUID v4.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:])
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
