package rpc

import pb "github.com/jsndz/bottle/rpc/proto"

type HandleFunc func(*pb.Message) *pb.Message

type StreamFunc func(*pb.Message, *ServerStream)

type Handler struct {
	Methods       map[string]HandleFunc
	StreamMethods map[string]StreamFunc
}

func NewHandler() *Handler {
	return &Handler{
		Methods:       make(map[string]HandleFunc),
		StreamMethods: make(map[string]StreamFunc),
	}
}

func (h *Handler) AddHandler(method string, fn HandleFunc) {
	h.Methods[method] = fn
}

func (h *Handler) Get(method string) (HandleFunc, bool) {
	fn, ok := h.Methods[method]
	return fn, ok
}

func (h *Handler) AddStreamHandler(method string, fn StreamFunc) {
	h.StreamMethods[method] = fn
}

func (h *Handler) GetStreamMethod(method string) (StreamFunc, bool) {
	fn, ok := h.StreamMethods[method]
	return fn, ok
}
