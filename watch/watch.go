package watch

import (
	"context"
	"strings"
	"sync"
)

type WatchHub struct {
	Watchers map[int64]*Watcher
	id       int64
	mu       sync.RWMutex
}

func NewWatchHub() *WatchHub {
	return &WatchHub{
		Watchers: make(map[int64]*Watcher),
	}
}

func (w *WatchHub) remove(id int64) {
	if watcher, exists := w.Watchers[id]; exists {
		delete(w.Watchers, id)
		watcher.Close()
	}
}

func (w *WatchHub) Watch(ctx context.Context, prefix string) *Watcher {
	w.mu.Lock()
	watcher := NewWatcher(w.id, prefix)
	w.id++
	w.Watchers[watcher.ID] = watcher
	w.mu.Unlock()

	if ctx != nil {
		go func() {
			<-ctx.Done()
			w.Unwatch(watcher.ID)
		}()
	}

	return watcher
}

func (w *WatchHub) Unwatch(id int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.remove(id)
}

func (w *WatchHub) Publish(event Event) {
	w.mu.RLock()
	var lagged []int64
	for id, val := range w.Watchers {
		if strings.HasPrefix(event.Key, val.Prefix) {
			select {
			case val.Buffer <- event:
			default:
				lagged = append(lagged, id)
			}
		}
	}
	w.mu.RUnlock()

	if len(lagged) > 0 {
		w.mu.Lock()
		for _, id := range lagged {
			w.remove(id)
		}
		w.mu.Unlock()
	}
}

func (w *WatchHub) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id := range w.Watchers {
		w.remove(id)
	}
}
