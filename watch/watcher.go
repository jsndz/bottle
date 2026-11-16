package watch

import (
	"sync"
)

type Watcher struct {
	ID     int64
	Prefix string
	Buffer chan Event
	once   sync.Once
}

func NewWatcher(id int64, prefix string) *Watcher {
	return &Watcher{
		ID:     id,
		Prefix: prefix,
		Buffer: make(chan Event, 1000),
	}
}

func (w *Watcher) Close() {
	w.once.Do(func() {
		close(w.Buffer)
	})
}
