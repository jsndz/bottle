package worker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jsndz/bottle/apiserver"
	"github.com/jsndz/bottle/kv"
	"github.com/jsndz/bottle/rpc"
	"github.com/jsndz/bottle/watch"
)

func setupTestAPIRPC(t *testing.T) (*apiserver.Server, string, func()) {
	store := kv.NewKVStore()
	hub := watch.NewWatchHub()
	store.SetEventCallback(func(e watch.Event) {
		hub.Publish(e)
	})

	apiServer := apiserver.NewServer(":0", store, hub)
	rpcServer := rpc.NewServer("0")
	apiServer.RegisterRPC(rpcServer)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	go rpcServer.ServeListener(lis)

	cleanup := func() {
		_ = lis.Close()
	}
	return apiServer, lis.Addr().String(), cleanup
}

func TestWorkerSelfRegistrationAndHeartbeat(t *testing.T) {
	apiServer, apiAddr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	cfg := WorkerConfig{
		NodeName:          "test-worker-1",
		APIServerAddr:     apiAddr,
		ListenAddr:        "127.0.0.1:0",
		HeartbeatInterval: 100 * time.Millisecond,
	}

	w := NewWorker(cfg, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start failed: %v", err)
	}
	defer w.Stop()

	var node *apiserver.Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := apiServer.Nodes().Get("", "test-worker-1")
		if err == nil && n != nil && n.IsReady() {
			node = n
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if node == nil {
		t.Fatal("node was not registered as ready")
	}
	if node.Spec.Address != w.Address {
		t.Fatalf("expected address %s, got %s", w.Address, node.Spec.Address)
	}
}

func TestWorkerRunPodLifecycle(t *testing.T) {
	apiServer, apiAddr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	cfg := WorkerConfig{
		NodeName:          "test-worker-run",
		APIServerAddr:     apiAddr,
		ListenAddr:        "127.0.0.1:0",
		HeartbeatInterval: 200 * time.Millisecond,
	}

	w := NewWorker(cfg, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start failed: %v", err)
	}
	defer w.Stop()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "quick-echo-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName: "test-worker-run",
			Containers: []apiserver.Container{
				{Name: "c1", Image: "busybox", Command: []string{"echo", "bottle-success"}},
			},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("create pod failed: %v", err)
	}

	var finalPhase string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p, err := apiServer.Pods().Get("default", "quick-echo-pod")
		if err == nil && p.Status.Phase == "Succeeded" {
			finalPhase = p.Status.Phase
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalPhase != "Succeeded" {
		t.Fatalf("expected pod to succeed, got %s", finalPhase)
	}
}

func TestWorkerRunFailingPod(t *testing.T) {
	apiServer, apiAddr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	cfg := WorkerConfig{
		NodeName:          "test-worker-fail",
		APIServerAddr:     apiAddr,
		ListenAddr:        "127.0.0.1:0",
		HeartbeatInterval: 200 * time.Millisecond,
	}

	w := NewWorker(cfg, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start failed: %v", err)
	}
	defer w.Stop()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "fail-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName: "test-worker-fail",
			Containers: []apiserver.Container{
				{Name: "c1", Image: "busybox", Command: []string{"sh", "-c", "exit 1"}},
			},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("create pod failed: %v", err)
	}

	var finalPhase string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p, err := apiServer.Pods().Get("default", "fail-pod")
		if err == nil && p.Status.Phase == "Failed" {
			finalPhase = p.Status.Phase
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalPhase != "Failed" {
		t.Fatalf("expected pod to fail, got %s", finalPhase)
	}
}

func TestWorkerPodDeletion(t *testing.T) {
	apiServer, apiAddr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	cfg := WorkerConfig{
		NodeName:          "test-worker-del",
		APIServerAddr:     apiAddr,
		ListenAddr:        "127.0.0.1:0",
		HeartbeatInterval: 200 * time.Millisecond,
	}

	w := NewWorker(cfg, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start failed: %v", err)
	}
	defer w.Stop()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "sleep-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName: "test-worker-del",
			Containers: []apiserver.Container{
				{Name: "c1", Image: "busybox", Command: []string{"sleep", "30"}},
			},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("create pod failed: %v", err)
	}

	running := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.IsPodRunning("default", "sleep-pod") {
			running = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !running {
		t.Fatal("pod did not start running locally")
	}

	if err := apiServer.Pods().Delete("default", "sleep-pod"); err != nil {
		t.Fatalf("failed delete pod: %v", err)
	}

	killed := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !w.IsPodRunning("default", "sleep-pod") {
			killed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !killed {
		t.Fatal("expected process to be killed and removed after pod deletion")
	}
}

func TestWorkerIgnoreOtherNodePods(t *testing.T) {
	apiServer, apiAddr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	cfg := WorkerConfig{
		NodeName:          "my-worker",
		APIServerAddr:     apiAddr,
		ListenAddr:        "127.0.0.1:0",
		HeartbeatInterval: 200 * time.Millisecond,
	}

	w := NewWorker(cfg, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("worker start failed: %v", err)
	}
	defer w.Stop()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "other-node-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName: "some-other-worker",
			Containers: []apiserver.Container{
				{Name: "c1", Image: "busybox", Command: []string{"echo", "hi"}},
			},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("create pod failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if w.IsPodRunning("default", "other-node-pod") {
		t.Fatal("worker should not run pods assigned to other nodes")
	}
}
