package rpc

import (
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	pb "github.com/jsndz/bottle/rpc/proto"
)

type Connection struct {
	ID        string
	Conn      *net.Conn
	CreatedAt time.Time
	LastUsed  time.Time
	Busy      bool
}
type ConnPool struct {
	mu      sync.Mutex
	Addr    string
	MaxConn uint8
	Conns   map[string]*Connection
	cond    sync.Cond
}

func (c *ConnPool) Get() *Connection {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		for _, conn := range c.Conns {
			if !conn.Busy {
				conn.Busy = true
				conn.LastUsed = time.Now()
				return conn
			}
		}
		if len(c.Conns) < int(c.MaxConn) {
			netConn, err := net.Dial("tcp", c.Addr)
			if err != nil {
				return nil
			}
			conn := &Connection{
				ID:        uuid.NewString(),
				Conn:      &netConn,
				CreatedAt: time.Now(),
				LastUsed:  time.Now(),
				Busy:      true,
			}
			c.Conns[conn.ID] = conn
			return conn
		}
		c.cond.Wait()
	}
}

func (c *ConnPool) Release(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if connection, ok := c.Conns[id]; ok && connection != nil {
		connection.Busy = false
		connection.LastUsed = time.Now()
		c.cond.Signal()
	}
}

func (c *ConnPool) PingTicker() {
	ticker := time.NewTicker(time.Second * 30)

	go func() {
		for range ticker.C {
			c.pingIdleConn()
		}
	}()
}

func (c *ConnPool) pingIdleConn() {
	ping := &pb.Message{
		Type: pb.FrameType_HEARTBEAT,
	}
	data, err := proto.Marshal(ping)
	if err != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, conn := range c.Conns {
		if !conn.Busy && conn.Conn != nil && *conn.Conn != nil {
			_ = writeFrame(*conn.Conn, data)
		}
	}
}

type GlobalPool struct {
	mu      sync.Mutex
	NetPool map[string]*ConnPool
}

func NewConnectionPool(port string, max uint8) *ConnPool {
	p := &ConnPool{
		Addr:    port,
		MaxConn: max,
		Conns:   make(map[string]*Connection),
		mu:      sync.Mutex{},
	}
	p.cond.L = &p.mu
	p.PingTicker()
	return p
}

func NewGlobalPool() *GlobalPool {
	return &GlobalPool{
		NetPool: make(map[string]*ConnPool),
	}
}

func (gp *GlobalPool) Get(addr string) *Connection {
	gp.mu.Lock()
	pool, ok := gp.NetPool[addr]
	if !ok {
		pool = NewConnectionPool(addr, 10)
		gp.NetPool[addr] = pool
	}
	gp.mu.Unlock()

	return pool.Get()
}

func (gp *GlobalPool) Release(addr, id string) {
	gp.mu.Lock()
	pool := gp.NetPool[addr]
	gp.mu.Unlock()

	if pool != nil {
		pool.Release(id)
	}
}
