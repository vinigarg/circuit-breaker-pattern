package breaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

type CircuitBreaker interface {
	Execute[T any](b *Breaker, fn func() (T, error)) (T, error)
	State() State
}

var ErrCircuitOpen = errors.New("Circuit is in Open State.")

type Config struct {
	Name string

	FailureRatio     float64       // trip when failures/total >= this (e.g. 0.5)
	MinRequests      int           // don't evaluate until this many calls in the window
	Interval         time.Duration // closed-state stats window; 0 = never reset
	OpenTimeout      time.Duration // how long to stay open before probing
	HalfOpenMaxProbe int           // max concurrent probes; also successes needed to close
	IsFailure        func(error) bool
	OnStateChange    func(name string, from, to State)
}

type Breaker struct {
	mu  sync.Mutex
	cfg Config

	state  State
	gen    uint64    // bumps on every state change / window reset
	expiry time.Time // closed: window end; open: when to go half-open

	total, failures      int // closed-state counters
	halfInFlight, halfOK int // half-open counters
}

func New(cfg Config) *Breaker {
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(err error) bool { return err != nil }
	}
	if cfg.OnStateChange == nil {
		// emit metrics/logs here
	}
	if cfg.HalfOpenMaxProbe < 1 {
		cfg.HalfOpenMaxProbe = 1
	}
	b := &Breaker{cfg: cfg}
	b.transition(Closed, time.Now())
	return b
}

func (b *Breaker) Execute[T any](req func() (T, error)) (T, error) {
	gen, err := b.before()
	if err != nil {
		return nil, err
	}

	// A panic counts as a failure, then continues unwinding.
	defer func() {
		if r := recover(); r != nil {
			b.after(gen, false)
			panic(r)
		}
	}()

	res, err = req()
	b.after(gen, !b.cfg.IsFailure(err))
	return res, err
}

func (b *Breaker) before() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()

	// Lazy state refresh: no background goroutine needed.
	switch b.state {
	case Closed:
		if !b.expiry.IsZero() && now.After(b.expiry) {
			b.transition(Closed, now) // start a fresh stats window
		}
	case Open:
		if now.After(b.expiry) {
			b.transition(HalfOpen, now)
		}
	}

	switch b.state {
	case Open:
		return 0, ErrCircuitOpen
	case HalfOpen:
		if b.halfInFlight >= b.cfg.HalfOpenMaxProbe {
			return 0, ErrCircuitOpen // probe slots are taken
		}
		b.halfInFlight++
	}
	return b.gen, nil
}

func (b *Breaker) after(gen uint64, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if gen != b.gen {
		return // result belongs to an earlier state; ignore it
	}
	now := time.Now()

	switch b.state {
	case Closed:
		b.total++
		if !success {
			b.failures++
		}
		if b.total >= b.cfg.MinRequests &&
			float64(b.failures)/float64(b.total) >= b.cfg.FailureRatio {
			b.transition(Open, now)
		}
	case HalfOpen:
		b.halfInFlight--
		if !success {
			b.transition(Open, now)
			return
		}
		b.halfOK++
		if b.halfOK >= b.cfg.HalfOpenMaxProbe {
			b.transition(Closed, now)
		}
	}
}

func (b *Breaker) transition(to State, now time.Time) {
	b.state = to
	b.gen++
	b.total, b.failures, b.halfInFlight, b.halfOK = 0, 0, 0, 0

	switch to {
	case Closed:
		if b.cfg.Interval > 0 {
			b.expiry = now.Add(b.cfg.Interval)
		} else {
			b.expiry = time.Time{}
		}
	case Open:
		b.expiry = now.Add(b.cfg.OpenTimeout)
	}
	// Hook: emit metrics/logs on every transition here.
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
