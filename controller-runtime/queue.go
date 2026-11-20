package controllerruntime

import "sync"

type WorkQueue struct {
	mu           sync.Mutex
	queue        []string
	dirty        map[string]bool
	processing   map[string]bool
	cond         *sync.Cond
	shuttingDown bool
}

func NewWorkQueue() *WorkQueue {
	w := &WorkQueue{
		queue:        make([]string, 0),
		dirty:        make(map[string]bool),
		processing:   make(map[string]bool),
		shuttingDown: false,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *WorkQueue) Add(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dirty[key] == true {
		return
	}
	w.dirty[key] = true
	if w.processing[key] == true {
		return
	}
	w.queue = append(w.queue, key)
	w.cond.Signal()
}

func (w *WorkQueue) Get() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var key string
	for len(w.queue) == 0 {
		w.cond.Wait()
	}
	key = w.queue[0]
	w.queue = w.queue[1:]
	w.processing[key] = true
	delete(w.dirty, key)
	return key
}

func (w *WorkQueue) Done(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.processing, key)
	if w.dirty[key] == true {
		w.queue = append(w.queue, key)
		w.cond.Signal()
	}
}
