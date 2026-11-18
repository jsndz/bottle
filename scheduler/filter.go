package scheduler

import (
	"github.com/jsndz/bottle/apiserver"
)

type FilterFunc func(pod *apiserver.Pod, node *apiserver.Node) bool

func NodeReadyFilter(pod *apiserver.Pod, node *apiserver.Node) bool {
	if node == nil {
		return false
	}
	return node.IsReady()
}

func NodeCapacityFilter(pod *apiserver.Pod, node *apiserver.Node) bool {
	if node == nil {
		return false
	}
	return node.Status.Capacity.CPU > 0 && node.Status.Capacity.Memory > 0
}

func DefaultFilters() []FilterFunc {
	return []FilterFunc{
		NodeReadyFilter,
		NodeCapacityFilter,
	}
}

func FilterNodes(pod *apiserver.Pod, nodes map[string]apiserver.Node, filters ...FilterFunc) []*apiserver.Node {
	if len(filters) == 0 {
		filters = DefaultFilters()
	}

	var candidates []*apiserver.Node
	for _, node := range nodes {
		n := node
		passed := true
		for _, filter := range filters {
			if !filter(pod, &n) {
				passed = false
				break
			}
		}
		if passed {
			candidates = append(candidates, &n)
		}
	}
	return candidates
}

func (s *Scheduler) FilterNodes(pod *apiserver.Pod) []*apiserver.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return FilterNodes(pod, s.nodes)
}
