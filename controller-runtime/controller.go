package controllerruntime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
	"github.com/jsndz/bottle/watch"
)

type Request struct {
	Namespace string
	Name      string
}

type Result struct {
	Requeue      bool
	RequeueAfter time.Duration
}

func parseKey(key string) Request {
	trimmed := strings.Trim(key, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) >= 4 && parts[0] == "registry" {
		return Request{
			Namespace: parts[2],
			Name:      parts[3],
		}
	}
	if len(parts) == 3 && parts[0] == "registry" {
		return Request{
			Namespace: "",
			Name:      parts[2],
		}
	}
	if len(parts) == 2 {
		return Request{
			Namespace: parts[0],
			Name:      parts[1],
		}
	}
	if len(parts) == 1 && parts[0] != "" {
		return Request{
			Namespace: "",
			Name:      parts[0],
		}
	}
	return Request{}
}

type Reconciler interface {
	Reconcile(ctx context.Context, req Request) (Result, error)
}

type Controller struct {
	name       string
	resource   string
	queue      *WorkQueue
	client     *rpc.Client
	workers    int
	reconciler Reconciler
	stopch     chan struct{}
	stopOnce   sync.Once
}

func NewController(name, res, addr string, pool *rpc.GlobalPool, rec Reconciler) *Controller {
	var c *rpc.Client
	if pool != nil && addr != "" {
		c = rpc.NewClient(addr, pool)
	}
	q := NewWorkQueue()
	return &Controller{
		name:       name,
		resource:   res,
		queue:      q,
		client:     c,
		workers:    3,
		reconciler: rec,
		stopch:     make(chan struct{}),
	}
}

func (c *Controller) watchResource(ctx context.Context, ch <-chan *pb.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopch:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev watch.Event
			if err := json.Unmarshal(msg.Payload, &ev); err == nil && ev.Key != "" {
				c.queue.Add(ev.Key)
				continue
			}
			var meta struct {
				Metadata struct {
					Namespace string `json:"namespace"`
					Name      string `json:"name"`
				} `json:"metadata"`
			}
			if err := json.Unmarshal(msg.Payload, &meta); err == nil && meta.Metadata.Name != "" {
				if meta.Metadata.Namespace != "" {
					c.queue.Add(meta.Metadata.Namespace + "/" + meta.Metadata.Name)
				} else {
					c.queue.Add(meta.Metadata.Name)
				}
				continue
			}
			if len(msg.Payload) > 0 {
				c.queue.Add(string(msg.Payload))
			}
		}
	}
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	req := parseKey(key)
	res, err := c.reconciler.Reconcile(ctx, req)
	if err != nil {
		c.queue.AddRateLimited(key)
		return true
	}
	if res.RequeueAfter > 0 {
		c.queue.Forget(key)
		c.queue.AddAfter(key, res.RequeueAfter)
		return true
	}
	if res.Requeue {
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) StartRecon(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *Controller) Start(ctx ...context.Context) error {
	runCtx := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		runCtx = ctx[0]
	}
	if c.client != nil && c.resource != "" {
		ch, err := c.client.StartStream(runCtx, "watch.resource", []byte(c.resource), nil)
		if err != nil {
			return err
		}
		go c.watchResource(runCtx, ch)
	}
	for i := 0; i < c.workers; i++ {
		go c.StartRecon(runCtx)
	}
	return nil
}

func (c *Controller) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopch)
		c.queue.ShutDown()
	})
}

func (c *Controller) Queue() *WorkQueue {
	return c.queue
}

func (c *Controller) SetWorkers(workers int) {
	if workers > 0 {
		c.workers = workers
	}
}
