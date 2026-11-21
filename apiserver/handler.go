package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

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

func (s *Server) handleBindPod(w http.ResponseWriter, r *http.Request) {
	ns := r.PathValue("ns")
	name := r.PathValue("name")
	var binding Binding
	if err := json.NewDecoder(r.Body).Decode(&binding); err != nil {
		writeError(w, err)
		return
	}
	if binding.Namespace == "" {
		binding.Namespace = ns
	}
	if binding.Name == "" {
		binding.Name = name
	}
	if binding.NodeName == "" {
		writeError(w, errors.New("nodeName required"))
		return
	}

	pod, err := s.pods.Get(binding.GetNamespace(), binding.GetName())
	if err != nil {
		writeError(w, err)
		return
	}
	if pod.Spec.NodeName != "" {
		writeError(w, errors.New("pod already assigned"))
		return
	}
	rv := binding.GetResourceVersion()
	if rv != 0 && pod.Metadata.ResourceVersion != rv {
		writeError(w, ErrConflict)
		return
	}
	pod.Spec.NodeName = binding.NodeName
	updated, err := s.pods.Update(pod)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleBindPodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var binding Binding
	if err := json.Unmarshal(m.Payload, &binding); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	ns := binding.GetNamespace()
	name := binding.GetName()
	if name == "" {
		return rpc.NewUnaryResponse(m, nil, "pod name required")
	}
	if binding.NodeName == "" {
		return rpc.NewUnaryResponse(m, nil, "nodeName required")
	}

	pod, err := s.pods.Get(ns, name)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	if pod.Spec.NodeName != "" {
		return rpc.NewUnaryResponse(m, nil, "pod already assigned")
	}
	rv := binding.GetResourceVersion()
	if rv != 0 && pod.Metadata.ResourceVersion != rv {
		return rpc.NewUnaryResponse(m, nil, ErrConflict.Error())
	}

	pod.Spec.NodeName = binding.NodeName
	updated, err := s.pods.Update(pod)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}

	data, err := json.Marshal(updated)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleGetPodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var req struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	if err := json.Unmarshal(m.Payload, &req); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}
	pod, err := s.pods.Get(ns, req.Name)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, err := json.Marshal(pod)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleListPodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	ns := string(m.Payload)
	list, err := s.pods.ListPods(ns)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, err := json.Marshal(list)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleCreatePodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var pod Pod
	if err := json.Unmarshal(m.Payload, &pod); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	created, err := s.pods.Create(&pod)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, err := json.Marshal(created)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleDeletePodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var req struct {
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}
	if err := json.Unmarshal(m.Payload, &req); err != nil || req.Name == "" {
		key := string(m.Payload)
		parts := strings.Split(strings.Trim(key, "/"), "/")
		if len(parts) >= 2 {
			req.Namespace = parts[0]
			req.Name = parts[1]
		} else if len(parts) == 1 {
			req.Name = parts[0]
		}
	}
	if req.Namespace == "" {
		req.Namespace = "default"
	}
	if err := s.pods.Delete(req.Namespace, req.Name); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, []byte(`{"status":"deleted"}`), "")
}

func (s *Server) handleGetNodeRPC(ctx context.Context, m *proto.Message) *proto.Message {
	name := string(m.Payload)
	node, err := s.nodes.Get("", name)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, err := json.Marshal(node)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleListNodeRPC(ctx context.Context, m *proto.Message) *proto.Message {
	list, err := s.nodes.ListNodes()
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, err := json.Marshal(list)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleRegisterNodeRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var node Node
	if err := json.Unmarshal(m.Payload, &node); err != nil || node.Metadata.Name == "" {
		var partial struct {
			Name    string `json:"name"`
			Address string `json:"address"`
		}
		if err2 := json.Unmarshal(m.Payload, &partial); err2 == nil && partial.Name != "" {
			node.Metadata.Name = partial.Name
			if node.Spec.Address == "" {
				node.Spec.Address = partial.Address
			}
		} else {
			rawName := string(m.Payload)
			if rawName != "" {
				node.Metadata.Name = rawName
			}
		}
	}
	if node.Metadata.Name == "" {
		return rpc.NewUnaryResponse(m, nil, "node name required")
	}
	if node.Spec.Address == "" {
		node.Spec.Address = "127.0.0.1:8001"
	}

	existing, err := s.nodes.Get("", node.Metadata.Name)
	if err != nil {
		node.SetReady(true)
		node.Status.LastHeartbeat = time.Now().Unix()
		created, createErr := s.nodes.Create(&node)
		if createErr != nil {
			return rpc.NewUnaryResponse(m, nil, createErr.Error())
		}
		data, _ := json.Marshal(created)
		return rpc.NewUnaryResponse(m, data, "")
	}

	existing.Spec.Address = node.Spec.Address
	if node.Status.Capacity.CPU > 0 {
		existing.Status.Capacity.CPU = node.Status.Capacity.CPU
	}
	if node.Status.Capacity.Memory > 0 {
		existing.Status.Capacity.Memory = node.Status.Capacity.Memory
	}
	existing.SetReady(true)
	existing.Status.LastHeartbeat = time.Now().Unix()

	updated, updateErr := s.nodes.Update(existing)
	if updateErr != nil {
		return rpc.NewUnaryResponse(m, nil, updateErr.Error())
	}
	data, _ := json.Marshal(updated)
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleHeartbeatNodeRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var name string
	var hb NodeHeartbeat
	if err := json.Unmarshal(m.Payload, &hb); err == nil && hb.Name != "" {
		name = hb.Name
	} else {
		name = string(m.Payload)
	}

	if name == "" {
		return rpc.NewUnaryResponse(m, nil, "node name required")
	}

	node, err := s.nodes.Get("", name)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}

	node.SetReady(true)
	node.Status.LastHeartbeat = time.Now().Unix()
	updated, err := s.nodes.Update(node)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, _ := json.Marshal(updated)
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleUpdateNodeRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var node Node
	if err := json.Unmarshal(m.Payload, &node); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	updated, err := s.nodes.Update(&node)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, _ := json.Marshal(updated)
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleUpdatePodRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var pod Pod
	if err := json.Unmarshal(m.Payload, &pod); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	updated, err := s.pods.Update(&pod)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, _ := json.Marshal(updated)
	return rpc.NewUnaryResponse(m, data, "")
}

func (s *Server) handleUpdatePodStatusRPC(ctx context.Context, m *proto.Message) *proto.Message {
	var update PodStatusUpdate
	if err := json.Unmarshal(m.Payload, &update); err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	if update.Name == "" {
		return rpc.NewUnaryResponse(m, nil, "pod name required")
	}
	if update.Namespace == "" {
		update.Namespace = "default"
	}

	pod, err := s.pods.Get(update.Namespace, update.Name)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}

	if update.Phase != "" {
		pod.Status.Phase = update.Phase
	}
	if update.PodIP != "" {
		pod.Status.PodIP = update.PodIP
	}
	if update.StartTime > 0 {
		pod.Status.StartTime = update.StartTime
	}

	updated, err := s.pods.Update(pod)
	if err != nil {
		return rpc.NewUnaryResponse(m, nil, err.Error())
	}
	data, _ := json.Marshal(updated)
	return rpc.NewUnaryResponse(m, data, "")
}

func RegisterWatchRPC(rpcserver *rpc.Server, hub *watch.WatchHub) {
	rpcserver.Handler.AddStreamHandler("watch.resource", func(ctx context.Context, m *proto.Message, ss *rpc.ServerStream) {
		prefix := string(m.Payload)
		watcher := hub.Watch(ctx, prefix)
		for {
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
		}
	}, nil)
}

func (s *Server) RegisterRPC(rpcserver *rpc.Server) {
	RegisterWatchRPC(rpcserver, s.hub)
	rpcserver.Handler.AddHandler("pod.bind", s.handleBindPodRPC, nil)
	rpcserver.Handler.AddHandler("pod.create", s.handleCreatePodRPC, nil)
	rpcserver.Handler.AddHandler("pod.delete", s.handleDeletePodRPC, nil)
	rpcserver.Handler.AddHandler("pod.get", s.handleGetPodRPC, nil)
	rpcserver.Handler.AddHandler("pod.list", s.handleListPodRPC, nil)
	rpcserver.Handler.AddHandler("pod.update", s.handleUpdatePodRPC, nil)
	rpcserver.Handler.AddHandler("pod.updateStatus", s.handleUpdatePodStatusRPC, nil)
	rpcserver.Handler.AddHandler("node.register", s.handleRegisterNodeRPC, nil)
	rpcserver.Handler.AddHandler("node.heartbeat", s.handleHeartbeatNodeRPC, nil)
	rpcserver.Handler.AddHandler("node.get", s.handleGetNodeRPC, nil)
	rpcserver.Handler.AddHandler("node.list", s.handleListNodeRPC, nil)
	rpcserver.Handler.AddHandler("node.update", s.handleUpdateNodeRPC, nil)
}

func RegisterAPIRPC(rpcserver *rpc.Server, server *Server) {
	server.RegisterRPC(rpcserver)
}
