package main

import (
	breaker "circuit-breaker-pattern/pkg/breaker"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	gobreaker "github.com/sony/gobreaker"
)

type Balance struct {
	AccountID string
	Amount    int64 // minor units, e.g. cents
	Currency  string
}

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http status %d", e.StatusCode)
}

func isClientError(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.StatusCode >= 400 && he.StatusCode < 500
	}
	return false
}

var cb *gobreaker.CircuitBreaker
var customCB *breaker.Breaker

func init() {
	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "payments",
		MaxRequests: 3,                // half-open probes
		Interval:    30 * time.Second, // closed-state counter reset
		Timeout:     15 * time.Second, // open duration
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 20 &&
				float64(c.TotalFailures)/float64(c.Requests) >= 0.5
		},
		IsSuccessful: func(err error) bool {
			return err == nil || isClientError(err)
		},
	})
	customCB = breaker.New(breaker.Config{
		Name:             "circuit-breaker",
		FailureRatio:     0.5,
		MinRequests:      3,
		Interval:         30 * time.Second, // closed-state counter reset
		OpenTimeout:      15 * time.Second, // open duration
		HalfOpenMaxProbe: 20,               // max concurrent probes; also successes needed to close
		IsFailure: func(err error) bool {
			return err == nil || isClientError(err)
		},
		OnStateChange: func(name string, from, to breaker.State) {
			// emit metrics/logs here
		},
	})
}

func Get(url string) ([]byte, error) {
	body, err := cb.Execute(func() (interface{}, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	})
	if err != nil {
		return nil, err
	}

	return body.([]byte), nil
}

func GetCustomCB(url string) ([]byte, error) {
	body, err := customCB.Execute(func() (interface{}, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	})
	if err != nil {
		return nil, err
	}

	return body.([]byte), nil
}
func main() {
	// GetBalance := func(ctx context.Context, id string) (Balance, error) {
	// 	var out Balance
	// 	err := paymentsCB.Do(func() error {
	// 		ctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond) // always set a timeout
	// 		defer cancel()
	// 		var err error
	// 		out, err = paymentsClient.Balance(ctx, id)
	// 		return err
	// 	})
	// 	if errors.Is(err, breaker.ErrCircuitOpen) {
	// 		return cachedBalance(id) // fallback: stale cache, default, or degraded response
	// 	}
	// 	return out, err
	// }

	body, err := Get("http://www.google.com/robots.txt")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(body))
}
