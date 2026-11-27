package netem

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	shaperMaxQueue = 200 * time.Millisecond
	shaperBacklog  = 8192
)

type shapedPacket struct {
	data []byte
	addr net.Addr
	at   time.Time
}

type ShapedConn struct {
	net.PacketConn
	mu       sync.Mutex
	lossPct  float64
	rateKbps float64
	nextFree time.Time
	queue    chan shapedPacket
	done     chan struct{}
	once     sync.Once
	dropped  atomic.Uint64
}

func NewShapedConn(c net.PacketConn) *ShapedConn {
	s := &ShapedConn{PacketConn: c, queue: make(chan shapedPacket, shaperBacklog), done: make(chan struct{})}
	go s.run()
	return s
}

func (s *ShapedConn) Set(lossPct, rateKbps float64) {
	s.mu.Lock()
	s.lossPct, s.rateKbps = lossPct, rateKbps
	s.mu.Unlock()
}

func (s *ShapedConn) Get() (lossPct, rateKbps float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lossPct, s.rateKbps
}

func (s *ShapedConn) Dropped() uint64 { return s.dropped.Load() }

func (s *ShapedConn) admit(size int, now time.Time) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lossPct > 0 && rand.Float64()*100 < s.lossPct {
		return time.Time{}, false
	}
	dep := now
	if s.nextFree.After(dep) {
		dep = s.nextFree
	}
	if s.rateKbps > 0 {
		dep = dep.Add(time.Duration(float64(size) * 8 / (s.rateKbps * 1000) * float64(time.Second)))
		if dep.Sub(now) > shaperMaxQueue {
			return time.Time{}, false
		}
		s.nextFree = dep
	}
	return dep, true
}

func (s *ShapedConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	dep, ok := s.admit(len(p), time.Now())
	if !ok {
		s.dropped.Add(1)
		return len(p), nil
	}

	pk := shapedPacket{data: append([]byte(nil), p...), addr: addr, at: dep}
	select {
	case s.queue <- pk:
	default:
		s.dropped.Add(1)
	}
	return len(p), nil
}

func (s *ShapedConn) run() {
	for {
		select {
		case <-s.done:
			return
		case pk := <-s.queue:
			if d := time.Until(pk.at); d > 0 {
				time.Sleep(d)
			}
			s.PacketConn.WriteTo(pk.data, pk.addr)
		}
	}
}

func (s *ShapedConn) Close() error {
	s.once.Do(func() { close(s.done) })
	return s.PacketConn.Close()
}
