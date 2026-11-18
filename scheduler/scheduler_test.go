package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/jsndz/bottle/apiserver"
	"github.com/jsndz/bottle/rpc"
)

func TestSchedulerScheduleOne(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	s.AddNode(apiserver.Node{
		Metadata: apiserver.Metadata{Name: "small-node"},
		Status: apiserver.NodeStatus{
			Capacity:   apiserver.NodeResources{CPU: 2, Memory: 4096},
			Conditions: []string{"Ready"},
		},
	})
	s.AddNode(apiserver.Node{
		Metadata: apiserver.Metadata{Name: "big-node"},
		Status: apiserver.NodeStatus{
			Capacity:   apiserver.NodeResources{CPU: 8, Memory: 16384},
			Conditions: []string{"Ready"},
		},
	})

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "app-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			Containers: []apiserver.Container{{Name: "app", Image: "nginx"}},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	created, err := apiServer.Pods().Create(pod)
	if err != nil {
		t.Fatalf("failed create: %v", err)
	}
	pod.Metadata.ResourceVersion = created.Metadata.ResourceVersion

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.ScheduleOne(ctx, pod); err != nil {
		t.Fatalf("schedule failed: %v", err)
	}

	if pod.Spec.NodeName != "big-node" {
		t.Fatalf("expected big-node, got %s", pod.Spec.NodeName)
	}

	persisted, err := apiServer.Pods().Get("default", "app-pod")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if persisted.Spec.NodeName != "big-node" {
		t.Fatalf("expected big-node in registry, got %s", persisted.Spec.NodeName)
	}
}

func TestSchedulerScheduleOneNoNodes(t *testing.T) {
	_, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "app-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			Containers: []apiserver.Container{{Name: "app", Image: "nginx"}},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	ctx := context.Background()
	err := s.ScheduleOne(ctx, pod)
	if err != ErrNoNodesAvailable {
		t.Fatalf("expected ErrNoNodesAvailable, got %v", err)
	}

	s.AddNode(apiserver.Node{
		Metadata: apiserver.Metadata{Name: "not-ready-node"},
		Status: apiserver.NodeStatus{
			Capacity:   apiserver.NodeResources{CPU: 4, Memory: 8192},
			Conditions: []string{"NotReady"},
		},
	})

	err = s.ScheduleOne(ctx, pod)
	if err != ErrNoNodesAvailable {
		t.Fatalf("expected ErrNoNodesAvailable for not ready node, got %v", err)
	}
}

func TestSchedulerIntegrationWatchAndSchedule(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("scheduler start failed: %v", err)
	}

	node1 := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "worker-1"},
		Spec:     apiserver.NodeSpec{Address: "192.168.1.10"},
		Status: apiserver.NodeStatus{
			Capacity:   apiserver.NodeResources{CPU: 2, Memory: 2048},
			Conditions: []string{"Ready"},
		},
	}
	node2 := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "worker-2"},
		Spec:     apiserver.NodeSpec{Address: "192.168.1.11"},
		Status: apiserver.NodeStatus{
			Capacity:   apiserver.NodeResources{CPU: 8, Memory: 16384},
			Conditions: []string{"Ready"},
		},
	}

	if _, err := apiServer.Nodes().Create(node1); err != nil {
		t.Fatalf("failed create node1: %v", err)
	}
	if _, err := apiServer.Nodes().Create(node2); err != nil {
		t.Fatalf("failed create node2: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "scheduled-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			Containers: []apiserver.Container{{Name: "web", Image: "nginx"}},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	if _, err := apiServer.Pods().Create(pod); err != nil {
		t.Fatalf("failed create pod: %v", err)
	}

	var assignedNode string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		p, err := apiServer.Pods().Get("default", "scheduled-pod")
		if err == nil && p.Spec.NodeName != "" {
			assignedNode = p.Spec.NodeName
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if assignedNode != "worker-2" {
		t.Fatalf("expected worker-2, got %s", assignedNode)
	}

	if err := apiServer.Nodes().Delete("", "worker-2"); err != nil {
		t.Fatalf("failed delete node: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if _, exists := s.GetNode("worker-2"); exists {
		t.Fatal("expected worker-2 to be removed from scheduler cache after deletion")
	}
}
