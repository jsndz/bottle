package apiserver

import (
	"context"
	"net/http"

	"github.com/jsndz/bottle/kv"
	"github.com/jsndz/bottle/watch"
)

type Server struct {
	httpServer *http.Server
	pods       *PodRegistry
	nodes      *NodeRegistry
	hub        *watch.WatchHub
}

func NewServer(addr string, store *kv.KVStore, hub *watch.WatchHub) *Server {
	s := &Server{
		pods:  NewPodRegistry(store),
		nodes: NewNodeRegistry(store),
		hub:   hub,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/namespaces/{ns}/pods", s.handleCreatePod)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods/{name}", s.handleGetPod)
	mux.HandleFunc("GET /api/v1/namespaces/{ns}/pods", s.handleListOrWatchPods)
	mux.HandleFunc("PUT /api/v1/namespaces/{ns}/pods/{name}", s.handleUpdatePod)
	mux.HandleFunc("DELETE /api/v1/namespaces/{ns}/pods/{name}", s.handleDeletePod)

	mux.HandleFunc("POST /api/v1/nodes", s.handleCreateNode)
	mux.HandleFunc("GET /api/v1/nodes/{name}", s.handleGetNode)
	mux.HandleFunc("GET /api/v1/nodes", s.handleListOrWatchNodes)
	mux.HandleFunc("PUT /api/v1/nodes/{name}", s.handleUpdateNode)
	mux.HandleFunc("DELETE /api/v1/nodes/{name}", s.handleDeleteNode)
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
