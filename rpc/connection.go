package rpc

import (
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
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
}

func (c *ConnPool) Get() *Connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.Conns {
		if !conn.Busy {
			return conn
		}
	}
	if int(c.MaxConn) > len(c.Conns) {
		netConn, err := net.Dial("tcp", c.Addr)
		if err != nil {

		}
		conn := Connection{
			ID:        uuid.NewString(),
			Conn:      &netConn,
			CreatedAt: time.Now(),
			Busy:      true,
		}
		c.Conns[conn.ID] = &conn
		conn.Busy = true

		return &conn
	}
	return nil
}

func (c *ConnPool) Release(addr, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	connection := c.Conns[addr]

	connection.Busy = false
	connection.LastUsed = time.Now()

}

type GlobalPool struct {
	mu      sync.Mutex
	NetPool map[string]*ConnPool
}

func NewConnectionPool(port string, max uint8) *ConnPool {
	return &ConnPool{
		Addr:    port,
		MaxConn: max,
		Conns:   make(map[string]*Connection),
	}
}

func NewGlobalPool() *GlobalPool {
	return &GlobalPool{
		NetPool: make(map[string]*ConnPool),
	}
}

func (gp *GlobalPool) Get(addr string) *Connection {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	pool, ok := gp.NetPool[addr]
	if !ok {
		pool = NewConnectionPool(addr, 10)
		gp.NetPool[addr] = pool
	}
	conn := pool.Get()
	return conn
}

func (gp *GlobalPool) Release(addr, id string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()
	pool, _ := gp.NetPool[addr]
	pool.Release(addr, id)
}
