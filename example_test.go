package lapwy_test

import (
	"os"
	"time"

	"github.com/lapwy-stats/lapwy-go"
)

func Example() {
	c := lapwy.New(os.Getenv("LAPWY_SERVER_KEY"))
	defer c.Close()

	// The same user id the site passes to identify() in the browser.
	c.Track("u_123", "purchase", lapwy.Props{"plan": "pro", "revenue": 29, "currency": "EUR"})
	c.TrackAt("u_123", "signup", nil, time.Now().Add(-time.Hour))
}
