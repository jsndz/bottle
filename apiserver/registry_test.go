package apiserver

import (
	"errors"
	"testing"

	"github.com/jsndz/bottle/kv"
)

func TestPodRegistryCRUD(t *testing.T) {
	store := kv.NewKVStore()
	reg := NewRegistry[*Pod](store, "pods", true, ValidatePod, DefaultPod)

	pod := &Pod{
		Metadata: Metadata{Name: "web", Namespace: "default"},
		Spec: PodSpec{
			Containers: []Container{{Name: "web", Image: "nginx"}},
		},
	}

	created, err := reg.Create(pod)
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	if created.Metadata.UID == "" {
		t.Fatal("expected UID to be generated")
	}
	if created.Metadata.ResourceVersion == 0 {
		t.Fatal("expected ResourceVersion to be assigned")
	}
	if created.Status.Phase != "Pending" {
		t.Fatalf("expected Pending phase, got %s", created.Status.Phase)
	}

	_, err = reg.Create(pod)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := reg.Get("default", "web")
	if err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if got.Metadata.Name != "web" || got.Spec.Containers[0].Image != "nginx" {
		t.Fatalf("unexpected got pod: %+v", got)
	}
	if got.Metadata.ResourceVersion != created.Metadata.ResourceVersion {
		t.Fatalf("expected rev %d, got %d", created.Metadata.ResourceVersion, got.Metadata.ResourceVersion)
	}

	_, err = reg.Get("default", "non-existent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	list, err := reg.List("default")
	if err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if len(list) != 1 || list[0].Metadata.Name != "web" {
		t.Fatalf("expected 1 pod in list, got %d", len(list))
	}

	oldRev := got.Metadata.ResourceVersion
	got.Spec.NodeName = "node-1"
	updated, err := reg.Update(got)
	if err != nil {
		t.Fatalf("failed to update pod: %v", err)
	}
	if updated.Metadata.ResourceVersion <= oldRev {
		t.Fatalf("expected resourceVersion to increase, got %d <= %d", updated.Metadata.ResourceVersion, oldRev)
	}

	stalePod := &Pod{
		Metadata: Metadata{
			Name:            "web",
			Namespace:       "default",
			ResourceVersion: oldRev,
		},
		Spec: PodSpec{
			Containers: []Container{{Name: "web", Image: "nginx"}},
		},
	}
	_, err = reg.Update(stalePod)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict on stale update, got %v", err)
	}

	if err := reg.Delete("default", "web"); err != nil {
		t.Fatalf("failed to delete pod: %v", err)
	}

	_, err = reg.Get("default", "web")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	if err := reg.Delete("default", "web"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound on second delete, got %v", err)
	}
}

func TestNodeRegistryCRUD(t *testing.T) {
	store := kv.NewKVStore()
	reg := NewRegistry[*Node](store, "nodes", false, ValidateNode, DefaultNode)

	node := &Node{
		Metadata: Metadata{Name: "worker-1"},
		Spec:     NodeSpec{Address: "192.168.1.10:8000"},
		Status: NodeStatus{
			Capacity: NodeResources{CPU: 4000, Memory: 8192},
		},
	}

	created, err := reg.Create(node)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	if created.Metadata.Namespace != "" {
		t.Fatal("expected empty namespace for node")
	}

	got, err := reg.Get("", "worker-1")
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if got.Spec.Address != "192.168.1.10:8000" {
		t.Fatalf("unexpected node address: %s", got.Spec.Address)
	}

	list, err := reg.List("")
	if err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}
	if len(list) != 1 || list[0].Metadata.Name != "worker-1" {
		t.Fatalf("expected 1 node, got %d", len(list))
	}

	if err := reg.Delete("", "worker-1"); err != nil {
		t.Fatalf("failed to delete node: %v", err)
	}

	_, err = reg.Get("", "worker-1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestTypedPodAndNodeRegistry(t *testing.T) {
	store := kv.NewKVStore()

	podReg := NewPodRegistry(store)
	pod := &Pod{
		Metadata: Metadata{Name: "app"},
		Spec:     PodSpec{Containers: []Container{{Name: "c1", Image: "img"}}},
	}
	if pod.IsScheduled() || pod.IsRunning() || pod.IsTerminated() {
		t.Fatal("unexpected pod status initial values")
	}

	createdPod, err := podReg.Create(pod)
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	if createdPod.Metadata.Namespace != "default" {
		t.Fatalf("expected default namespace, got %s", createdPod.Metadata.Namespace)
	}

	podList, err := podReg.ListPods("default")
	if err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if podList.TypeMeta.Kind != "PodList" || len(podList.Items) != 1 {
		t.Fatalf("unexpected pod list: %+v", podList)
	}

	createdPod.Spec.NodeName = "w1"
	createdPod.Status.Phase = "Running"
	if !createdPod.IsScheduled() || !createdPod.IsRunning() {
		t.Fatal("expected pod to be scheduled and running")
	}

	createdPod.Status.Phase = "Succeeded"
	if !createdPod.IsTerminated() {
		t.Fatal("expected pod to be terminated")
	}

	nodeReg := NewNodeRegistry(store)
	node := &Node{
		Metadata: Metadata{Name: "w1"},
		Spec:     NodeSpec{Address: "10.0.0.1"},
		Status:   NodeStatus{Conditions: []string{"Ready"}},
	}
	if !node.IsReady() {
		t.Fatal("expected node to be ready")
	}

	_, err = nodeReg.Create(node)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	nodeList, err := nodeReg.ListNodes()
	if err != nil {
		t.Fatalf("failed to list nodes: %v", err)
	}
	if nodeList.TypeMeta.Kind != "NodeList" || len(nodeList.Items) != 1 {
		t.Fatalf("unexpected node list: %+v", nodeList)
	}
}
