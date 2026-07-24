package rpc

import pb "github.com/jsndz/bottle/rpc/proto"

type HandleFunc func(*pb.Request) *pb.Response

type Handler struct {
	Methods map[string]HandleFunc
}

func NewHandler() *Handler {
	return &Handler{
		Methods: make(map[string]HandleFunc),
	}
}

func (h *Handler) AddHandler(method string, fn HandleFunc) {
	h.Methods[method] = fn
}

func (h *Handler) Get(method string) (HandleFunc, bool) {
	fn, ok := h.Methods[method]
	return fn, ok
}
