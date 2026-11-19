package apiserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jsndz/bottle/kv"
	"github.com/jsndz/bottle/rpc"
	"github.com/jsndz/bottle/watch"
)

func setupTestServer(t *testing.T) (*Server, *httptest.Server, *kv.KVStore, *watch.WatchHub) {
	store := kv.NewKVStore()
	hub := watch.NewWatchHub()

	store.SetEventCallback(func(e watch.Event) {
		hub.Publish(e)
	})

	s := NewServer(":0", store, hub)
	ts := httptest.NewServer(s.httpServer.Handler)
	return s, ts, store, hub
}

func TestHTTPServerPodCRUD(t *testing.T) {
	_, ts, _, _ := setupTestServer(t)
	defer ts.Close()

	pod := Pod{
		Metadata: Metadata{Name: "web"},
		Spec:     PodSpec{Containers: []Container{{Name: "web", Image: "nginx"}}},
	}
	body, _ := json.Marshal(pod)

	resp, err := http.Post(ts.URL+"/api/v1/namespaces/default/pods", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var created Pod
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()

	if created.Metadata.Name != "web" || created.Metadata.Namespace != "default" {
		t.Fatalf("unexpected created pod: %+v", created)
	}

	resp, err = http.Get(ts.URL + "/api/v1/namespaces/default/pods/web")
	if err != nil {
		t.Fatalf("failed GET: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var got Pod
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()

	if got.Metadata.ResourceVersion != created.Metadata.ResourceVersion {
		t.Fatalf("expected rev %d, got %d", created.Metadata.ResourceVersion, got.Metadata.ResourceVersion)
	}

	resp, err = http.Get(ts.URL + "/api/v1/namespaces/default/pods")
	if err != nil {
		t.Fatalf("failed list: %v", err)
	}
	var podList PodList
	_ = json.NewDecoder(resp.Body).Decode(&podList)
	resp.Body.Close()

	if len(podList.Items) != 1 || podList.Items[0].Metadata.Name != "web" {
		t.Fatalf("expected 1 pod in list, got %d", len(podList.Items))
	}

	got.Spec.NodeName = "worker-1"
	updateBody, _ := json.Marshal(got)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/namespaces/default/pods/web", bytes.NewReader(updateBody))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed PUT: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on update, got %d", resp.StatusCode)
	}
	var updated Pod
	_ = json.NewDecoder(resp.Body).Decode(&updated)
	resp.Body.Close()

	if updated.Spec.NodeName != "worker-1" {
		t.Fatalf("expected worker-1, got %s", updated.Spec.NodeName)
	}

	stalePod := Pod{
		Metadata: Metadata{Name: "web", Namespace: "default", ResourceVersion: created.Metadata.ResourceVersion},
		Spec:     PodSpec{Containers: []Container{{Name: "web", Image: "nginx"}}},
	}
	staleBody, _ := json.Marshal(stalePod)
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/api/v1/namespaces/default/pods/web", bytes.NewReader(staleBody))
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 conflict, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/namespaces/default/pods/web", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(ts.URL + "/api/v1/namespaces/default/pods/web")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTPServerNodeCRUD(t *testing.T) {
	_, ts, _, _ := setupTestServer(t)
	defer ts.Close()

	node := Node{
		Metadata: Metadata{Name: "worker-1"},
		Spec:     NodeSpec{Address: "192.168.1.100"},
		Status:   NodeStatus{Capacity: NodeResources{CPU: 4000, Memory: 8192}},
	}
	body, _ := json.Marshal(node)

	resp, err := http.Post(ts.URL+"/api/v1/nodes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed POST: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(ts.URL + "/api/v1/nodes/worker-1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var got Node
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()

	if got.Spec.Address != "192.168.1.100" {
		t.Fatalf("unexpected address: %s", got.Spec.Address)
	}

	resp, _ = http.Get(ts.URL + "/api/v1/nodes")
	var nodeList NodeList
	_ = json.NewDecoder(resp.Body).Decode(&nodeList)
	resp.Body.Close()
	if len(nodeList.Items) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodeList.Items))
	}

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/v1/nodes/worker-1", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTPServerValidationErrors(t *testing.T) {
	_, ts, _, _ := setupTestServer(t)
	defer ts.Close()

	invalidPod := Pod{
		Metadata: Metadata{Name: "invalid-pod"},
		Spec:     PodSpec{Containers: []Container{}},
	}
	body, _ := json.Marshal(invalidPod)
	resp, _ := http.Post(ts.URL+"/api/v1/namespaces/default/pods", "application/json", bytes.NewReader(body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid pod, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTPServerWatchStreaming(t *testing.T) {
	_, ts, _, _ := setupTestServer(t)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/namespaces/default/pods?watch=true", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed watch request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for watch, got %d", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)

	time.Sleep(50 * time.Millisecond)

	pod := Pod{
		Metadata: Metadata{Name: "streamed-pod"},
		Spec:     PodSpec{Containers: []Container{{Name: "c1", Image: "img"}}},
	}
	body, _ := json.Marshal(pod)
	createResp, err := http.Post(ts.URL+"/api/v1/namespaces/default/pods", "application/json", bytes.NewReader(body))
	if err != nil || createResp.StatusCode != http.StatusCreated {
		t.Fatalf("failed create pod: %v", err)
	}
	createResp.Body.Close()

	lineChan := make(chan []byte, 1)
	go func() {
		line, _ := reader.ReadBytes('\n')
		lineChan <- line
	}()

	select {
	case line := <-lineChan:
		var event watch.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("failed to decode watch event: %v (raw: %s)", err, string(line))
		}
		if event.Key != "/registry/pods/default/streamed-pod" {
			t.Fatalf("unexpected event key: %s", event.Key)
		}
		if event.Type != watch.PUT {
			t.Fatalf("expected PUT event, got %s", event.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
}

func TestHTTPServerBindPod(t *testing.T) {
	_, ts, _, _ := setupTestServer(t)
	defer ts.Close()

	pod := Pod{
		Metadata: Metadata{Name: "bind-pod"},
		Spec:     PodSpec{Containers: []Container{{Name: "c1", Image: "img"}}},
	}
	body, _ := json.Marshal(pod)
	createResp, err := http.Post(ts.URL+"/api/v1/namespaces/default/pods", "application/json", bytes.NewReader(body))
	if err != nil || createResp.StatusCode != http.StatusCreated {
		t.Fatalf("failed create: %v", err)
	}
	createResp.Body.Close()

	binding := Binding{
		NodeName: "node-1",
	}
	bBytes, _ := json.Marshal(binding)
	bindResp, err := http.Post(ts.URL+"/api/v1/namespaces/default/pods/bind-pod/binding", "application/json", bytes.NewReader(bBytes))
	if err != nil || bindResp.StatusCode != http.StatusOK {
		t.Fatalf("failed bind: %v, status: %d", err, bindResp.StatusCode)
	}
	var bound Pod
	_ = json.NewDecoder(bindResp.Body).Decode(&bound)
	bindResp.Body.Close()

	if bound.Spec.NodeName != "node-1" {
		t.Fatalf("expected node-1, got %s", bound.Spec.NodeName)
	}
}

func TestRPCServerBindPod(t *testing.T) {
	s, _, _, _ := setupTestServer(t)

	rpcServer := rpc.NewServer("0")
	s.RegisterRPC(rpcServer)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lis.Close()

	go rpcServer.ServeListener(lis)

	pool := rpc.NewGlobalPool()
	client := rpc.NewClient(lis.Addr().String(), pool)

	pod := Pod{
		Metadata: Metadata{Name: "rpc-pod", Namespace: "default"},
		Spec:     PodSpec{Containers: []Container{{Name: "c1", Image: "img"}}},
	}
	_, err = s.pods.Create(&pod)
	if err != nil {
		t.Fatalf("failed create: %v", err)
	}

	binding := Binding{
		Namespace: "default",
		Name:      "rpc-pod",
		NodeName:  "worker-1",
	}
	bBytes, _ := json.Marshal(binding)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Call(ctx, "pod.bind", bBytes, nil)
	if err != nil {
		t.Fatalf("rpc call failed: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected rpc error: %s", resp.Error)
	}

	var bound Pod
	if err := json.Unmarshal(resp.Payload, &bound); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if bound.Spec.NodeName != "worker-1" {
		t.Fatalf("expected worker-1, got %s", bound.Spec.NodeName)
	}

	conflictResp, err := client.Call(ctx, "pod.bind", bBytes, nil)
	if err != nil {
		t.Fatalf("second bind call failed: %v", err)
	}
	if conflictResp.Error == "" {
		t.Fatal("expected conflict or error when already assigned")
	}
}

func TestRPCServerNodeRegistrationAndHeartbeat(t *testing.T) {
	s, _, _, _ := setupTestServer(t)

	rpcServer := rpc.NewServer("0")
	s.RegisterRPC(rpcServer)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lis.Close()

	go rpcServer.ServeListener(lis)

	pool := rpc.NewGlobalPool()
	client := rpc.NewClient(lis.Addr().String(), pool)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	nodePayload, _ := json.Marshal(Node{
		Metadata: Metadata{Name: "worker-agent-1"},
		Spec:     NodeSpec{Address: "127.0.0.1:8001"},
		Status: NodeStatus{
			Capacity: NodeResources{CPU: 4, Memory: 8192},
		},
	})

	resp, err := client.Call(ctx, "node.register", nodePayload, nil)
	if err != nil || resp.Error != "" {
		t.Fatalf("node.register failed: %v, %s", err, resp.Error)
	}

	var regNode Node
	_ = json.Unmarshal(resp.Payload, &regNode)
	if !regNode.IsReady() || regNode.Status.LastHeartbeat == 0 {
		t.Fatalf("expected node to be ready with heartbeat, got %+v", regNode)
	}

	nodePayload2, _ := json.Marshal(Node{
		Metadata: Metadata{Name: "worker-agent-1"},
		Spec:     NodeSpec{Address: "127.0.0.1:8002"},
	})
	resp, err = client.Call(ctx, "node.register", nodePayload2, nil)
	if err != nil || resp.Error != "" {
		t.Fatalf("second node.register failed: %v, %s", err, resp.Error)
	}
	var regNode2 Node
	_ = json.Unmarshal(resp.Payload, &regNode2)
	if regNode2.Spec.Address != "127.0.0.1:8002" {
		t.Fatalf("expected updated address 127.0.0.1:8002, got %s", regNode2.Spec.Address)
	}

	hbPayload, _ := json.Marshal(NodeHeartbeat{Name: "worker-agent-1"})
	hbResp, err := client.Call(ctx, "node.heartbeat", hbPayload, nil)
	if err != nil || hbResp.Error != "" {
		t.Fatalf("node.heartbeat failed: %v, %s", err, hbResp.Error)
	}

	pod := Pod{
		Metadata: Metadata{Name: "status-pod", Namespace: "default"},
		Spec:     PodSpec{Containers: []Container{{Name: "c1", Image: "img"}}},
	}
	_, err = s.pods.Create(&pod)
	if err != nil {
		t.Fatalf("failed create pod: %v", err)
	}

	statusPayload, _ := json.Marshal(PodStatusUpdate{
		Namespace: "default",
		Name:      "status-pod",
		Phase:     "Running",
		PodIP:     "10.0.0.1",
	})
	statusResp, err := client.Call(ctx, "pod.updateStatus", statusPayload, nil)
	if err != nil || statusResp.Error != "" {
		t.Fatalf("pod.updateStatus failed: %v, %s", err, statusResp.Error)
	}

	var updatedPod Pod
	_ = json.Unmarshal(statusResp.Payload, &updatedPod)
	if updatedPod.Status.Phase != "Running" || updatedPod.Status.PodIP != "10.0.0.1" {
		t.Fatalf("expected Running and 10.0.0.1, got %+v", updatedPod)
	}
}
