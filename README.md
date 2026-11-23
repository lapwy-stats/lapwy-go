# lapwy for Go

Server events for [lapwy](https://lapwy.com): payments, API calls, MCP tools used and other things your backend sees, about one of your signed-in users. Standard library only.

## Install

```
go get github.com/lapwy-stats/lapwy-go
```

Go 1.22 or later.

## Use

```go
import "github.com/lapwy-stats/lapwy-go"

func main() {
	c := lapwy.New(os.Getenv("LAPWY_SERVER_KEY"))
	defer c.Close()

	// Something a signed-in user did:
	c.Track("u_123", "api_call", lapwy.Props{"endpoint": "/v1/search"})

	// Money: revenue and a three-letter currency.
	c.Track("u_123", "purchase", lapwy.Props{"plan": "pro", "revenue": 29, "currency": "EUR"})
}
```

The server key (`lws_…`) is on the site's Install screen in lapwy. Keep it on the server.

## The user id

Every event needs one, and it must be the same id your site passes to `identify()` in the browser: lapwy links the two, so a payment counts for the visit and the source that brought the person. Use your own account id, never an email address; lapwy refuses those.

## Reference

| | |
|---|---|
| `lapwy.New(key, options...)` | once, at start; share the client |
| `c.Track(user, name, props)` | something that happened now |
| `c.TrackAt(user, name, props, time)` | something that happened earlier, up to 7 days ago |
| `c.Flush(ctx)` | send what waits, now, and wait; an error if it failed |
| `c.Close()` | send what waits (up to 5 s) and stop |

| Option | |
|---|---|
| `lapwy.WithEndpoint(url)` | default `https://i.lapwy.com` |
| `lapwy.WithHTTPClient(*http.Client)` | your own client, for a proxy or timeout |
| `lapwy.WithLogger(*slog.Logger)` | default `slog.Default()` |
| `lapwy.WithFlushInterval(d)` | default 1 s |
| `lapwy.WithBatchSize(n)` | default 100, at most 500 |

`Track` never waits for the network: events wait in memory (10 000 at most, then the oldest go) and are sent in batches every second, or as soon as 100 wait. A busy server or no network is tried again later, with the same event ids, so nothing is counted twice. A refused batch (a wrong key, say) is dropped and logged.

Props: up to 30, each a string, number or bool. An event lapwy would refuse is dropped with a warning in the log, never a panic: no user or name, a user id over 200 characters or shaped like an email address, a name over 100 characters.

With an empty key, `New` still returns a client: it drops everything and says so once in the log, so a missing environment variable does not stop the program.

Docs: [lapwy.com/docs#go](https://lapwy.com/docs#go).

## Licence

MIT
