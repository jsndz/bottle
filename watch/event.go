package watch

type EventType string

const (
	PUT    EventType = "PUT"
	DELETE EventType = "DELETE"
)

type Event struct {
	Type      EventType
	Key       string
	Value     []byte
	PrevValue []byte
	Rev       int64
}

func NewEvent(t EventType, key string, val, prevVal []byte, rev int64) Event {
	return Event{
		Type:      t,
		Key:       key,
		Value:     val,
		PrevValue: prevVal,
		Rev:       rev,
	}
}
