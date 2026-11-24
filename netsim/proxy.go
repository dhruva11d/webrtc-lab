package main

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Settings struct {
	LossPct    float64 `json:"loss_pct"`
	DelayMs    float64 `json:"delay_ms"`
	JitterMs   float64 `json:"jitter_ms"`
	RateKbps   float64 `json:"rate_kbps"`
	MaxQueueMs float64 `json:"max_queue_ms"`
}

type Counters struct {
	UpForwarded   uint64 `json:"up_forwarded"`
	UpDropped     uint64 `json:"up_dropped"`
	DownForwarded uint64 `json:"down_forwarded"`
	DownDropped   uint64 `json:"down_dropped"`
}

type shaper struct {
	mu       sync.Mutex
	nextFree time.Time
}

type session struct {
	client *net.UDPAddr
	up     *net.UDPConn // socket towards the target, one per client so the target can tell clients apart
	last   atomic.Int64
	upQ    shaper
	downQ  shaper
}

type Proxy struct {
	conn   *net.UDPConn
	target *net.UDPAddr

	setMu sync.RWMutex
	set   Settings

	sessMu   sync.Mutex
	sessions map[string]*session

	upFwd, upDrop, downFwd, downDrop atomic.Uint64
}

func NewProxy(listen, target string, s Settings) (*Proxy, error) {
	la, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return nil, err
	}
	ta, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", la)
	if err != nil {
		return nil, err
	}
	return &Proxy{conn: conn, target: ta, set: s, sessions: make(map[string]*session)}, nil
}

func (p *Proxy) Addr() *net.UDPAddr { return p.conn.LocalAddr().(*net.UDPAddr) }

func (p *Proxy) Settings() Settings {
	p.setMu.RLock()
	defer p.setMu.RUnlock()
	return p.set
}

func (p *Proxy) SetSettings(s Settings) {
	p.setMu.Lock()
	p.set = s
	p.setMu.Unlock()
}

func (p *Proxy) Counters() Counters {
	return Counters{p.upFwd.Load(), p.upDrop.Load(), p.downFwd.Load(), p.downDrop.Load()}
}

func ms(v float64) time.Duration { return time.Duration(v * float64(time.Millisecond)) }

func (p *Proxy) shape(q *shaper, size int) (delay time.Duration, drop bool) {
	s := p.Settings()

	if s.LossPct > 0 && rand.Float64()*100 < s.LossPct {
		return 0, true
	}

	d := s.DelayMs
	if s.JitterMs > 0 {
		d += (rand.Float64()*2 - 1) * s.JitterMs
	}
	if d < 0 {
		d = 0
	}
	delay = ms(d)

	if s.RateKbps > 0 {
		now := time.Now()
		tx := time.Duration(float64(size*8) / (s.RateKbps * 1000) * float64(time.Second))
		q.mu.Lock()
		start := now
		if q.nextFree.After(now) {
			start = q.nextFree
		}
		queued := start.Sub(now)
		if s.MaxQueueMs > 0 && queued > ms(s.MaxQueueMs) {
			q.mu.Unlock()
			return 0, true
		}
		q.nextFree = start.Add(tx)
		q.mu.Unlock()
		delay += queued + tx
	}
	return delay, false
}

func after(d time.Duration, f func()) {
	if d <= 0 {
		f()
		return
	}
	time.AfterFunc(d, f)
}

func (p *Proxy) getSession(client *net.UDPAddr) *session {
	key := client.String()
	p.sessMu.Lock()
	defer p.sessMu.Unlock()
	if s, ok := p.sessions[key]; ok {
		return s
	}
	up, err := net.DialUDP("udp", nil, p.target)
	if err != nil {
		return nil
	}
	s := &session{client: client, up: up}
	s.last.Store(time.Now().UnixNano())
	p.sessions[key] = s
	go p.downLoop(s)
	return s
}

func (p *Proxy) downLoop(s *session) {
	buf := make([]byte, 65535)
	for {
		n, err := s.up.Read(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		delay, drop := p.shape(&s.downQ, n)
		if drop {
			p.downDrop.Add(1)
			continue
		}
		p.downFwd.Add(1)
		data := append([]byte(nil), buf[:n]...)
		after(delay, func() { p.conn.WriteToUDP(data, s.client) })
	}
}

func (p *Proxy) Run(ctx context.Context) {
	go func() { <-ctx.Done(); p.conn.Close() }()
	go p.reaper(ctx)

	buf := make([]byte, 65535)
	for {
		n, client, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			// On Windows an earlier send to a closed port can surface here as a read error
			// (WSAECONNRESET), the same ICMP effect seen in the UDP lab. Keep going.
			continue
		}
		s := p.getSession(client)
		if s == nil {
			continue
		}
		s.last.Store(time.Now().UnixNano())

		delay, drop := p.shape(&s.upQ, n)
		if drop {
			p.upDrop.Add(1)
			continue
		}
		p.upFwd.Add(1)
		data := append([]byte(nil), buf[:n]...)
		after(delay, func() { s.up.Write(data) })
	}
}

func (p *Proxy) reaper(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.sessMu.Lock()
			for k, s := range p.sessions {
				if time.Since(time.Unix(0, s.last.Load())) > time.Minute {
					s.up.Close()
					delete(p.sessions, k)
				}
			}
			p.sessMu.Unlock()
		}
	}
}
