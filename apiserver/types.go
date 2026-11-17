package apiserver

type TypeMeta struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
}

type Metadata struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	ResourceVersion int64             `json:"resourceVersion"`
	Labels          map[string]string `json:"labels,omitempty"`
}

type Container struct {
	Name    string   `json:"name"`
	Image   string   `json:"image"`
	Command []string `json:"command,omitempty"`
}

type PodSpec struct {
	NodeName   string      `json:"nodeName,omitempty"`
	Containers []Container `json:"containers"`
}

type PodStatus struct {
	Phase     string `json:"phase"`
	PodIP     string `json:"podIP,omitempty"`
	StartTime int64  `json:"startTime,omitempty"`
}

type Pod struct {
	TypeMeta TypeMeta  `json:"typeMeta"`
	Metadata Metadata  `json:"metadata"`
	Spec     PodSpec   `json:"spec"`
	Status   PodStatus `json:"status"`
}

type NodeResources struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

type NodeSpec struct {
	Address string `json:"address"`
}

type NodeStatus struct {
	Capacity   NodeResources `json:"capacity"`
	Conditions []string      `json:"conditions"`
}

type Node struct {
	TypeMeta TypeMeta   `json:"typeMeta"`
	Metadata Metadata   `json:"metadata"`
	Spec     NodeSpec   `json:"spec"`
	Status   NodeStatus `json:"status"`
}

func (p *Pod) GetMetaData() *Metadata {
	return &p.Metadata
}

func (p *Pod) GetTypeMeta() *TypeMeta {
	return &p.TypeMeta
}

func (n *Node) GetMetaData() *Metadata {
	return &n.Metadata
}

func (n *Node) GetTypeMeta() *TypeMeta {
	return &n.TypeMeta
}

func (p *Pod) IsScheduled() bool {
	return p.Spec.NodeName != ""
}

func (p *Pod) IsRunning() bool {
	return p.Status.Phase == "Running"
}

func (p *Pod) IsTerminated() bool {
	return p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed"
}

func (n *Node) IsReady() bool {
	for _, cond := range n.Status.Conditions {
		if cond == "Ready" {
			return true
		}
	}
	return false
}

type ListMeta struct {
	ResourceVersion int64 `json:"resourceVersion"`
}

type PodList struct {
	TypeMeta TypeMeta `json:"typeMeta"`
	Metadata ListMeta `json:"metadata"`
	Items    []Pod    `json:"items"`
}

type NodeList struct {
	TypeMeta TypeMeta `json:"typeMeta"`
	Metadata ListMeta `json:"metadata"`
	Items    []Node   `json:"items"`
}
