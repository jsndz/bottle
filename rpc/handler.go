package rpc

import (
	"context"

	pb "github.com/jsndz/bottle/rpc/proto"
)

type HandleFunc func(context.Context, *pb.Message) *pb.Message

type StreamFunc func(context.Context, *pb.Message, *ServerStream)

type Middlewares func(HandleFunc) HandleFunc

type StreamMiddleware func(StreamFunc) StreamFunc

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

func (h *Handler) AddHandler(method string, fn HandleFunc, middlewares []Middlewares) {
	wrapped := fn

	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}

	h.Methods[method] = wrapped
}

func (h *Handler) Get(method string) (HandleFunc, bool) {
	fn, ok := h.Methods[method]
	return fn, ok
}

func (h *Handler) AddStreamHandler(method string, fn StreamFunc, middlewares []StreamMiddleware) {
	wrapped := fn
	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}
	h.StreamMethods[method] = wrapped
}

func (h *Handler) GetStreamMethod(method string) (StreamFunc, bool) {
	fn, ok := h.StreamMethods[method]
	return fn, ok
}
