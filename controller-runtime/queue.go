package controllerruntime

import (
	"sync"
	"time"
)

type WorkQueue struct {
	mu           sync.Mutex
	queue        []string
	dirty        map[string]bool
	processing   map[string]bool
	cond         *sync.Cond
	shuttingDown bool
	failures     map[string]int
}

func NewWorkQueue() *WorkQueue {
	w := &WorkQueue{
		queue:        make([]string, 0),
		dirty:        make(map[string]bool),
		processing:   make(map[string]bool),
		failures:     make(map[string]int),
		shuttingDown: false,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *WorkQueue) Add(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.shuttingDown {
		return
	}
	if w.dirty[key] {
		return
	}
	w.dirty[key] = true
	if w.processing[key] {
		return
	}
	w.queue = append(w.queue, key)
	w.cond.Signal()
}

func (w *WorkQueue) Get() (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.queue) == 0 && !w.shuttingDown {
		w.cond.Wait()
	}
	if len(w.queue) == 0 {
		return "", true
	}
	key := w.queue[0]
	w.queue = w.queue[1:]
	w.processing[key] = true
	delete(w.dirty, key)
	return key, false
}

func (w *WorkQueue) Done(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.processing, key)
	if w.dirty[key] {
		w.queue = append(w.queue, key)
		w.cond.Signal()
	}
}

func (w *WorkQueue) ShutDown() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.shuttingDown = true
	w.cond.Broadcast()
}

func (w *WorkQueue) ShuttingDown() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.shuttingDown
}

func (w *WorkQueue) AddAfter(key string, duration time.Duration) {
	if duration <= 0 {
		w.Add(key)
		return
	}
	time.AfterFunc(duration, func() {
		w.Add(key)
	})
}

func (w *WorkQueue) AddRateLimited(key string) {
	w.mu.Lock()
	count := w.failures[key]
	w.failures[key] = count + 1
	w.mu.Unlock()

	delay := time.Duration(1<<count) * 5 * time.Millisecond
	if delay > 1*time.Minute {
		delay = 1 * time.Minute
	}
	w.AddAfter(key, delay)
}

func (w *WorkQueue) Forget(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.failures, key)
}

func (w *WorkQueue) NumRequeues(key string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failures[key]
}

func (w *WorkQueue) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue)
}
