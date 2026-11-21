package controllerruntime

import (
	"context"
	"errors"
	"sync"

	"github.com/jsndz/bottle/rpc"
)

type ControllerManager interface {
	Add(c *Controller) error
	AddController(name string, resource string, reconciler Reconciler) error
	Start(ctx context.Context) error
	Stop()
}

type Manager struct {
	mu          sync.Mutex
	apiAddr     string
	pool        *rpc.GlobalPool
	client      *rpc.Client
	controllers []*Controller
	stopCh      chan struct{}
	stopOnce    sync.Once
	started     bool
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewManager(apiServerAddr string, pool *rpc.GlobalPool) *Manager {
	var client *rpc.Client
	if pool != nil && apiServerAddr != "" {
		client = rpc.NewClient(apiServerAddr, pool)
	}
	return &Manager{
		apiAddr:     apiServerAddr,
		pool:        pool,
		client:      client,
		controllers: make([]*Controller, 0),
		stopCh:      make(chan struct{}),
	}
}

func (m *Manager) Add(c *Controller) error {
	if c == nil {
		return errors.New("controller cannot be nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.client == nil && m.client != nil {
		c.client = m.client
	}
	m.controllers = append(m.controllers, c)
	if m.started {
		return c.Start(m.ctx)
	}
	return nil
}

func (m *Manager) AddController(name string, resource string, reconciler Reconciler) error {
	c := NewController(name, resource, m.apiAddr, m.pool, reconciler)
	if c.client == nil && m.client != nil {
		c.client = m.client
	}
	return m.Add(c)
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return errors.New("manager already started")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.started = true

	for _, c := range m.controllers {
		if c.client == nil && m.client != nil {
			c.client = m.client
		}
		if err := c.Start(m.ctx); err != nil {
			m.mu.Unlock()
			m.Stop()
			return err
		}
	}
	m.mu.Unlock()

	go func() {
		select {
		case <-m.ctx.Done():
			m.Stop()
		case <-m.stopCh:
		}
	}()

	return nil
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		if m.cancel != nil {
			m.cancel()
		}
		close(m.stopCh)
		for _, c := range m.controllers {
			c.Stop()
		}
		m.mu.Unlock()
	})
}

func (m *Manager) Controllers() []*Controller {
	m.mu.Lock()
	defer m.mu.Unlock()
	list := make([]*Controller, len(m.controllers))
	copy(list, m.controllers)
	return list
}

func (m *Manager) Client() *rpc.Client {
	return m.client
}

func (m *Manager) StopCh() <-chan struct{} {
	return m.stopCh
}

func (m *Manager) Wait() {
	<-m.stopCh
}
