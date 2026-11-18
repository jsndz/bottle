package scheduler

import (
	"errors"

	"github.com/jsndz/bottle/apiserver"
)

var ErrNoNodesAvailable = errors.New("no nodes available for scheduling")

type ScoreFunc func(pod *apiserver.Pod, node *apiserver.Node) int64

func LeastAllocatedScore(pod *apiserver.Pod, node *apiserver.Node) int64 {
	if node == nil {
		return 0
	}
	return node.Status.Capacity.CPU + node.Status.Capacity.Memory
}

func ScoreNodes(pod *apiserver.Pod, nodes []*apiserver.Node, scorers ...ScoreFunc) (string, error) {
	if len(nodes) == 0 {
		return "", ErrNoNodesAvailable
	}
	if len(nodes) == 1 {
		if nodes[0] != nil {
			return nodes[0].Metadata.Name, nil
		}
		return "", ErrNoNodesAvailable
	}

	if len(scorers) == 0 {
		scorers = []ScoreFunc{LeastAllocatedScore}
	}

	var bestNode string
	var maxScore int64 = -1

	for _, node := range nodes {
		if node == nil {
			continue
		}
		var totalScore int64
		for _, scorer := range scorers {
			totalScore += scorer(pod, node)
		}
		if totalScore > maxScore || bestNode == "" || (totalScore == maxScore && node.Metadata.Name < bestNode) {
			maxScore = totalScore
			bestNode = node.Metadata.Name
		}
	}

	if bestNode == "" {
		return "", ErrNoNodesAvailable
	}
	return bestNode, nil
}

func (s *Scheduler) ScoreNodes(pod *apiserver.Pod, nodes []*apiserver.Node) (string, error) {
	return ScoreNodes(pod, nodes)
}
