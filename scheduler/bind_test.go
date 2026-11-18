package scheduler

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

func TestBindPodSuccess(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "test-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			Containers: []apiserver.Container{{Name: "c1", Image: "img"}},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	httpPod, err := apiServer.Pods().Create(pod)
	if err != nil {
		t.Fatalf("failed create: %v", err)
	}
	pod.Metadata.ResourceVersion = httpPod.Metadata.ResourceVersion

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.Bind(ctx, pod, "node-1"); err != nil {
		t.Fatalf("bind failed: %v", err)
	}

	if pod.Spec.NodeName != "node-1" {
		t.Fatalf("expected node-1, got %s", pod.Spec.NodeName)
	}

	updated, err := apiServer.Pods().Get("default", "test-pod")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if updated.Spec.NodeName != "node-1" {
		t.Fatalf("expected node-1 in registry, got %s", updated.Spec.NodeName)
	}
}

func TestBindValidation(t *testing.T) {
	_, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	ctx := context.Background()

	if err := s.Bind(ctx, nil, "node-1"); err == nil {
		t.Fatal("expected error for nil pod")
	}

	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "p1", Namespace: "default"}}
	if err := s.Bind(ctx, pod, ""); err == nil {
		t.Fatal("expected error for empty nodeName")
	}
}

func TestBindConflict(t *testing.T) {
	apiServer, addr, cleanup := setupTestAPIRPC(t)
	defer cleanup()

	pool := rpc.NewGlobalPool()
	s := NewScheduler(addr, pool)
	defer s.Close()

	pod := &apiserver.Pod{
		Metadata: apiserver.Metadata{Name: "conflict-pod", Namespace: "default"},
		Spec: apiserver.PodSpec{
			Containers: []apiserver.Container{{Name: "c1", Image: "img"}},
		},
		Status: apiserver.PodStatus{Phase: "Pending"},
	}

	created, err := apiServer.Pods().Create(pod)
	if err != nil {
		t.Fatalf("failed create: %v", err)
	}

	created.Status.Phase = "Running"
	if _, err := apiServer.Pods().Update(created); err != nil {
		t.Fatalf("failed update: %v", err)
	}

	pod.Metadata.ResourceVersion = 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = s.Bind(ctx, pod, "node-1")
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
}
