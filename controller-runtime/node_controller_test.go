package controllerruntime

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
		lis.Close()
	}
	return apiServer, lis.Addr().String(), cleanup
}

func TestNodeReconcilerHealthyNode(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	node := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "healthy-node"},
		Spec:     apiserver.NodeSpec{Address: "127.0.0.1:8001"},
		Status: apiserver.NodeStatus{
			Conditions:    []string{"Ready"},
			LastHeartbeat: time.Now().Unix(),
		},
	}
	if _, err := apiServer.Nodes().Create(node); err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	pool := rpc.NewGlobalPool()
	client := rpc.NewClient(addr, pool)
	rec := NewNodeReconciler(client, 5*time.Second)

	res, err := rec.Reconcile(context.Background(), Request{Name: "healthy-node"})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if res.RequeueAfter <= 0 {
		t.Fatalf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	storedNode, err := apiServer.Nodes().Get("", "healthy-node")
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if !storedNode.IsReady() {
		t.Fatal("expected node to remain Ready")
	}
}

func TestNodeReconcilerDeadNodeEvictsPods(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	deadNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "dead-node"},
		Spec:     apiserver.NodeSpec{Address: "127.0.0.1:8001"},
		Status: apiserver.NodeStatus{
			Conditions:    []string{"Ready"},
			LastHeartbeat: time.Now().Unix() - 30,
		},
	}
	if _, err := apiServer.Nodes().Create(deadNode); err != nil {
		t.Fatalf("failed to create dead node: %v", err)
	}

	healthyNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "healthy-node"},
		Spec:     apiserver.NodeSpec{Address: "127.0.0.1:8002"},
		Status: apiserver.NodeStatus{
			Conditions:    []string{"Ready"},
			LastHeartbeat: time.Now().Unix(),
		},
	}
	if _, err := apiServer.Nodes().Create(healthyNode); err != nil {
		t.Fatalf("failed to create healthy node: %v", err)
	}

	podOnDeadNode := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "victim-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName:   "dead-node",
			Containers: []apiserver.Container{{Name: "c1", Image: "img1"}},
		},
		Status: apiserver.PodStatus{Phase: "Running"},
	}
	if _, err := apiServer.Pods().Create(podOnDeadNode); err != nil {
		t.Fatalf("failed to create pod on dead node: %v", err)
	}

	podOnHealthyNode := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "safe-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName:   "healthy-node",
			Containers: []apiserver.Container{{Name: "c2", Image: "img2"}},
		},
		Status: apiserver.PodStatus{Phase: "Running"},
	}
	if _, err := apiServer.Pods().Create(podOnHealthyNode); err != nil {
		t.Fatalf("failed to create pod on healthy node: %v", err)
	}

	pool := rpc.NewGlobalPool()
	client := rpc.NewClient(addr, pool)
	rec := NewNodeReconciler(client, 10*time.Second)

	res, err := rec.Reconcile(context.Background(), Request{Name: "dead-node"})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	storedNode, err := apiServer.Nodes().Get("", "dead-node")
	if err != nil {
		t.Fatalf("failed to get dead node: %v", err)
	}
	if storedNode.IsReady() {
		t.Fatal("expected dead node to be marked NotReady")
	}

	rescuedPod, err := apiServer.Pods().Get("default", "victim-pod")
	if err != nil {
		t.Fatalf("failed to get victim pod: %v", err)
	}
	if rescuedPod.Spec.NodeName != "" {
		t.Fatalf("expected rescued pod NodeName to be empty, got %q", rescuedPod.Spec.NodeName)
	}
	if rescuedPod.Status.Phase != "Pending" {
		t.Fatalf("expected rescued pod Phase to be Pending, got %q", rescuedPod.Status.Phase)
	}

	safePod, err := apiServer.Pods().Get("default", "safe-pod")
	if err != nil {
		t.Fatalf("failed to get safe pod: %v", err)
	}
	if safePod.Spec.NodeName != "healthy-node" {
		t.Fatalf("expected safe pod NodeName to remain healthy-node, got %q", safePod.Spec.NodeName)
	}
}

func TestNodeControllerFullIntegration(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	ctrl := NewNodeController(addr, pool, 1*time.Second)
	defer ctrl.Stop()

	if err := ctrl.Start(); err != nil {
		t.Fatalf("failed to start controller: %v", err)
	}

	node := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "dying-node"},
		Spec:     apiserver.NodeSpec{Address: "127.0.0.1:8003"},
		Status: apiserver.NodeStatus{
			Conditions:    []string{"Ready"},
			LastHeartbeat: time.Now().Unix() - 10,
		},
	}
	if _, err := apiServer.Nodes().Create(node); err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "pod-to-rescue", Namespace: "default"},
		Spec: apiserver.PodSpec{
			NodeName:   "dying-node",
			Containers: []apiserver.Container{{Name: "app", Image: "nginx"}},
		},
		Status: apiserver.PodStatus{Phase: "Running"},
	}
	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	ctrl.Queue().Add("dying-node")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := apiServer.Nodes().Get("", "dying-node")
		if err == nil && !n.IsReady() {
			p, err := apiServer.Pods().Get("default", "pod-to-rescue")
			if err == nil && p.Spec.NodeName == "" && p.Status.Phase == "Pending" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("timed out waiting for dying node to be marked NotReady and pod to be rescued")
}
