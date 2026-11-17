package apiserver

import (
	"errors"
	"fmt"
)

var (
	ErrNameRequired          = errors.New("metadata.name is required")
	ErrNameInvalid           = errors.New("metadata.name must consist of lowercase alphanumeric characters or '-' and start/end with an alphanumeric character")
	ErrNamespaceInvalid      = errors.New("metadata.namespace must consist of lowercase alphanumeric characters or '-'")
	ErrNodeNamespaceNotEmpty = errors.New("metadata.namespace must be empty for node")
	ErrNoContainers          = errors.New("pod must have at least one container")
	ErrContainerNameEmpty    = errors.New("container name is required")
	ErrContainerImageEmpty   = errors.New("container image is required")
	ErrNodeAddressEmpty      = errors.New("node spec.address is required")
	ErrNegativeCPU           = errors.New("node capacity CPU cannot be negative")
	ErrNegativeMemory        = errors.New("node capacity memory cannot be negative")
	ErrInvalidPhase          = errors.New("invalid pod phase")
)

func isValidName(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	if s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		return false
	}
	return true
}

func ValidatePod(pod *Pod) error {
	if pod == nil {
		return errors.New("pod cannot be nil")
	}
	if pod.Metadata.Name == "" {
		return ErrNameRequired
	}
	if !isValidName(pod.Metadata.Name) {
		return ErrNameInvalid
	}
	if pod.Metadata.Namespace != "" && !isValidName(pod.Metadata.Namespace) {
		return ErrNamespaceInvalid
	}
	if len(pod.Spec.Containers) == 0 {
		return ErrNoContainers
	}
	names := make(map[string]bool)
	for _, c := range pod.Spec.Containers {
		if c.Name == "" {
			return ErrContainerNameEmpty
		}
		if !isValidName(c.Name) {
			return fmt.Errorf("invalid container name %q", c.Name)
		}
		if names[c.Name] {
			return fmt.Errorf("duplicate container name %q", c.Name)
		}
		names[c.Name] = true
		if c.Image == "" {
			return fmt.Errorf("container %q: %w", c.Name, ErrContainerImageEmpty)
		}
	}
	if pod.Status.Phase != "" {
		switch pod.Status.Phase {
		case "Pending", "Running", "Succeeded", "Failed":
		default:
			return ErrInvalidPhase
		}
	}
	return nil
}

func ValidateNode(node *Node) error {
	if node == nil {
		return errors.New("node cannot be nil")
	}
	if node.Metadata.Name == "" {
		return ErrNameRequired
	}
	if !isValidName(node.Metadata.Name) {
		return ErrNameInvalid
	}
	if node.Metadata.Namespace != "" {
		return ErrNodeNamespaceNotEmpty
	}
	if node.Spec.Address == "" {
		return ErrNodeAddressEmpty
	}
	if node.Status.Capacity.CPU < 0 {
		return ErrNegativeCPU
	}
	if node.Status.Capacity.Memory < 0 {
		return ErrNegativeMemory
	}
	return nil
}

func DefaultPod(pod *Pod) {
	if pod == nil {
		return
	}
	if pod.TypeMeta.Kind == "" {
		pod.TypeMeta.Kind = "Pod"
	}
	if pod.TypeMeta.APIVersion == "" {
		pod.TypeMeta.APIVersion = "v1"
	}
	if pod.Metadata.Namespace == "" {
		pod.Metadata.Namespace = "default"
	}
	if pod.Status.Phase == "" {
		pod.Status.Phase = "Pending"
	}
}

func DefaultNode(node *Node) {
	if node == nil {
		return
	}
	if node.TypeMeta.Kind == "" {
		node.TypeMeta.Kind = "Node"
	}
	if node.TypeMeta.APIVersion == "" {
		node.TypeMeta.APIVersion = "v1"
	}
	node.Metadata.Namespace = ""
}
