package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jsndz/bottle/rpc"
	"github.com/jsndz/bottle/rpc/proto"
	"github.com/jsndz/bottle/watch"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrAlreadyExists):
		status = http.StatusConflict
	case errors.Is(err, ErrConflict):
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) handleCreatePod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	var pod Pod
	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
		writeError(w, err)
		return
	}
	if pod.Metadata.Namespace == "" {
		pod.Metadata.Namespace = ns
	}
	if pod.Metadata.Namespace != ns {
		writeError(w, errors.New("metadata.namespace does not match url namespace"))
		return
	}
	created, err := s.pods.Create(&pod)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleGetPod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	pod, err := s.pods.Get(ns, name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pod)
}

func (s *Server) handleListOrWatchPods(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	if r.URL.Query().Get("watch") == "true" {
		s.streamWatch(w, r, s.pods.KeyPrefix(ns))
		return
	}
	list, err := s.pods.ListPods(ns)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleUpdatePod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	var pod Pod
	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
		writeError(w, err)
		return
	}
	pod.Metadata.Name = name
	pod.Metadata.Namespace = ns

	updated, err := s.pods.Update(&pod)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDeletePod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	if err := s.pods.Delete(ns, name); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	var node Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		writeError(w, err)
		return
	}
	created, err := s.nodes.Create(&node)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	node, err := s.nodes.Get("", name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) handleListOrWatchNodes(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("watch") == "true" {
		s.streamWatch(w, r, s.nodes.KeyPrefix(""))
		return
	}
	list, err := s.nodes.ListNodes()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleUpdateNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var node Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		writeError(w, err)
		return
	}
	node.Metadata.Name = name
	node.Metadata.Namespace = ""

	updated, err := s.nodes.Update(&node)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDeleteNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.nodes.Delete("", name); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *Server) streamWatch(w http.ResponseWriter, r *http.Request, prefix string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, errors.New("streaming unsupported"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	watcher := s.hub.Watch(r.Context(), prefix)

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-watcher.Buffer:
			if !ok {
				return
			}
			if err := json.NewEncoder(w).Encode(event); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func RegisterWatchRPC(rpcserver *rpc.Server, hub *watch.WatchHub) {
	rpcserver.Handler.AddStreamHandler("watch.resource", func(ctx context.Context, m *proto.Message, ss *rpc.ServerStream) {
		prefix := string(m.Payload)
		watcher := hub.Watch(ctx, prefix)
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Buffer:
			if !ok {
				ss.Finish()
				return
			}
			eventBytes, _ := json.Marshal(event)
			if err := ss.Send(eventBytes); err != nil {
				return
			}
		}
	}, nil)
}
