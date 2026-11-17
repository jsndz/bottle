package apiserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jsndz/bottle/kv"
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
