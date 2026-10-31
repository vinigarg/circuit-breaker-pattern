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

type Breaker interface {
	Execute(fn func() (any, error)) (any, error)
	State() State
}

var ErrCircuitOpen = errors.New("Circuit is in Open State.")

type Settings struct {
	Name string

	FailureRatio     float64       // trip when failures/total >= this (e.g. 0.5)
	MinRequests      int           // don't evaluate until this many calls in the window
	Interval         time.Duration // closed-state stats window; 0 = never reset
	OpenTimeout      time.Duration // how long to stay open before probing
	HalfOpenMaxProbe int           // max concurrent probes; also successes needed to close
	IsFailure        func(error) bool
	OnStateChange    func(name string, from, to State)
}

type CircuitBreaker struct {
	mu sync.Mutex
	s  Settings

	state  State
	gen    uint64    // bumps on every state change / window reset
	expiry time.Time // closed: window end; open: when to go half-open

	total, failures      int // closed-state counters
	halfInFlight, halfOK int // half-open counters
}

func New(s Settings) *CircuitBreaker {
	if s.IsFailure == nil {
		s.IsFailure = func(err error) bool { return err != nil }
	}
	if s.OnStateChange == nil {
		// emit metrics/logs here
	}
	if s.HalfOpenMaxProbe < 1 {
		s.HalfOpenMaxProbe = 1
	}
	b := &CircuitBreaker{s: s}
	b.transition(Closed, time.Now())
	return b
}

func (b *CircuitBreaker) Execute(req func() (any, error)) (any, error) {
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

	res, err := req()
	b.after(gen, !b.s.IsFailure(err))
	return res, err
}

func (b *CircuitBreaker) before() (uint64, error) {
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
		if b.halfInFlight >= b.s.HalfOpenMaxProbe {
			return 0, ErrCircuitOpen // probe slots are taken
		}
		b.halfInFlight++
	}
	return b.gen, nil
}

func (b *CircuitBreaker) after(gen uint64, success bool) {
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
		if b.total >= b.s.MinRequests &&
			float64(b.failures)/float64(b.total) >= b.s.FailureRatio {
			b.transition(Open, now)
		}
	case HalfOpen:
		b.halfInFlight--
		if !success {
			b.transition(Open, now)
			return
		}
		b.halfOK++
		if b.halfOK >= b.s.HalfOpenMaxProbe {
			b.transition(Closed, now)
		}
	}
}

func (b *CircuitBreaker) transition(to State, now time.Time) {
	b.state = to
	b.gen++
	b.total, b.failures, b.halfInFlight, b.halfOK = 0, 0, 0, 0

	switch to {
	case Closed:
		if b.s.Interval > 0 {
			b.expiry = now.Add(b.s.Interval)
		} else {
			b.expiry = time.Time{}
		}
	case Open:
		b.expiry = now.Add(b.s.OpenTimeout)
	}
	// Hook: emit metrics/logs on every transition here.
}

func (b *CircuitBreaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
