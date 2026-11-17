package apiserver

import (
	"errors"
	"testing"
)

func TestValidatePodSuccess(t *testing.T) {
	pod := &Pod{
		Metadata: Metadata{
			Name:      "nginx-web",
			Namespace: "default",
		},
		Spec: PodSpec{
			Containers: []Container{
				{Name: "nginx", Image: "nginx:alpine"},
			},
		},
	}
	if err := ValidatePod(pod); err != nil {
		t.Fatalf("expected valid pod, got: %v", err)
	}
}

func TestValidatePodFailures(t *testing.T) {
	if err := ValidatePod(nil); err == nil {
		t.Fatal("expected error for nil pod")
	}

	p1 := &Pod{}
	if err := ValidatePod(p1); !errors.Is(err, ErrNameRequired) {
		t.Fatalf("expected ErrNameRequired, got: %v", err)
	}

	p2 := &Pod{Metadata: Metadata{Name: "-invalid-"}}
	if err := ValidatePod(p2); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("expected ErrNameInvalid, got: %v", err)
	}

	p3 := &Pod{Metadata: Metadata{Name: "web", Namespace: "INVALID_NS"}}
	if err := ValidatePod(p3); !errors.Is(err, ErrNamespaceInvalid) {
		t.Fatalf("expected ErrNamespaceInvalid, got: %v", err)
	}

	p4 := &Pod{Metadata: Metadata{Name: "web", Namespace: "default"}}
	if err := ValidatePod(p4); !errors.Is(err, ErrNoContainers) {
		t.Fatalf("expected ErrNoContainers, got: %v", err)
	}

	p5 := &Pod{
		Metadata: Metadata{Name: "web"},
		Spec: PodSpec{
			Containers: []Container{{Name: "", Image: "nginx"}},
		},
	}
	if err := ValidatePod(p5); !errors.Is(err, ErrContainerNameEmpty) {
		t.Fatalf("expected ErrContainerNameEmpty, got: %v", err)
	}

	p6 := &Pod{
		Metadata: Metadata{Name: "web"},
		Spec: PodSpec{
			Containers: []Container{
				{Name: "app", Image: "nginx"},
				{Name: "app", Image: "redis"},
			},
		},
	}
	if err := ValidatePod(p6); err == nil {
		t.Fatal("expected duplicate container name error")
	}

	p7 := &Pod{
		Metadata: Metadata{Name: "web"},
		Spec: PodSpec{
			Containers: []Container{{Name: "app", Image: ""}},
		},
	}
	if err := ValidatePod(p7); !errors.Is(err, ErrContainerImageEmpty) {
		t.Fatalf("expected ErrContainerImageEmpty, got: %v", err)
	}

	p8 := &Pod{
		Metadata: Metadata{Name: "web"},
		Spec: PodSpec{
			Containers: []Container{{Name: "app", Image: "nginx"}},
		},
		Status: PodStatus{Phase: "InvalidPhase"},
	}
	if err := ValidatePod(p8); !errors.Is(err, ErrInvalidPhase) {
		t.Fatalf("expected ErrInvalidPhase, got: %v", err)
	}
}

func TestDefaultPod(t *testing.T) {
	pod := &Pod{
		Metadata: Metadata{Name: "web"},
		Spec: PodSpec{
			Containers: []Container{{Name: "web", Image: "nginx"}},
		},
	}
	DefaultPod(pod)

	if pod.TypeMeta.Kind != "Pod" || pod.TypeMeta.APIVersion != "v1" {
		t.Fatalf("unexpected typemeta: %+v", pod.TypeMeta)
	}
	if pod.Metadata.Namespace != "default" {
		t.Fatalf("expected default namespace, got %q", pod.Metadata.Namespace)
	}
	if pod.Status.Phase != "Pending" {
		t.Fatalf("expected Pending phase, got %q", pod.Status.Phase)
	}
}

func TestValidateNodeSuccess(t *testing.T) {
	node := &Node{
		Metadata: Metadata{Name: "worker-1"},
		Spec:     NodeSpec{Address: "192.168.1.10:8000"},
		Status: NodeStatus{
			Capacity: NodeResources{CPU: 4000, Memory: 8192},
		},
	}
	if err := ValidateNode(node); err != nil {
		t.Fatalf("expected valid node, got: %v", err)
	}
}

func TestValidateNodeFailures(t *testing.T) {
	if err := ValidateNode(nil); err == nil {
		t.Fatal("expected error for nil node")
	}

	n1 := &Node{}
	if err := ValidateNode(n1); !errors.Is(err, ErrNameRequired) {
		t.Fatalf("expected ErrNameRequired, got: %v", err)
	}

	n2 := &Node{Metadata: Metadata{Name: "worker-1", Namespace: "default"}}
	if err := ValidateNode(n2); !errors.Is(err, ErrNodeNamespaceNotEmpty) {
		t.Fatalf("expected ErrNodeNamespaceNotEmpty, got: %v", err)
	}

	n3 := &Node{Metadata: Metadata{Name: "worker-1"}}
	if err := ValidateNode(n3); !errors.Is(err, ErrNodeAddressEmpty) {
		t.Fatalf("expected ErrNodeAddressEmpty, got: %v", err)
	}

	n4 := &Node{
		Metadata: Metadata{Name: "worker-1"},
		Spec:     NodeSpec{Address: "192.168.1.10"},
		Status:   NodeStatus{Capacity: NodeResources{CPU: -1, Memory: 100}},
	}
	if err := ValidateNode(n4); !errors.Is(err, ErrNegativeCPU) {
		t.Fatalf("expected ErrNegativeCPU, got: %v", err)
	}

	n5 := &Node{
		Metadata: Metadata{Name: "worker-1"},
		Spec:     NodeSpec{Address: "192.168.1.10"},
		Status:   NodeStatus{Capacity: NodeResources{CPU: 100, Memory: -1}},
	}
	if err := ValidateNode(n5); !errors.Is(err, ErrNegativeMemory) {
		t.Fatalf("expected ErrNegativeMemory, got: %v", err)
	}
}

func TestDefaultNode(t *testing.T) {
	node := &Node{
		Metadata: Metadata{Name: "worker-1", Namespace: "some-ns"},
		Spec:     NodeSpec{Address: "192.168.1.10"},
	}
	DefaultNode(node)

	if node.TypeMeta.Kind != "Node" || node.TypeMeta.APIVersion != "v1" {
		t.Fatalf("unexpected typemeta: %+v", node.TypeMeta)
	}
	if node.Metadata.Namespace != "" {
		t.Fatalf("expected empty namespace for node, got %q", node.Metadata.Namespace)
	}
}
