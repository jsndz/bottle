package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jsndz/bottle/apiserver"
	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
	"github.com/jsndz/bottle/watch"
)

type Nodescore struct {
	Nodename string
	Score    int64
}

type NodeScore = Nodescore

type Scheduler struct {
	mu       sync.RWMutex
	client   *rpc.Client
	podqueue chan *apiserver.Pod
	nodes    map[string]apiserver.Node
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewScheduler(addr string, pool *rpc.GlobalPool) *Scheduler {
	return &Scheduler{
		client:   rpc.NewClient(addr, pool),
		podqueue: make(chan *apiserver.Pod, 1000),
		nodes:    make(map[string]apiserver.Node),
		stopCh:   make(chan struct{}),
	}
}

func (s *Scheduler) AddNode(node apiserver.Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[node.Metadata.Name] = node
}

func (s *Scheduler) RemoveNode(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, name)
}

func (s *Scheduler) GetNode(name string) (apiserver.Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[name]
	return n, ok
}

func (s *Scheduler) EnqueuePod(pod *apiserver.Pod) {
	if pod != nil {
		s.podqueue <- pod
	}
}

func (s *Scheduler) ScheduleOne(ctx context.Context, pod *apiserver.Pod) error {
	if pod == nil {
		return errors.New("pod cannot be nil")
	}
	candidates := s.FilterNodes(pod)
	if len(candidates) == 0 {
		return ErrNoNodesAvailable
	}
	winner, err := s.ScoreNodes(pod, candidates)
	if err != nil {
		return err
	}
	return s.Bind(ctx, pod, winner)
}

func (s *Scheduler) Start(ctx context.Context) error {
	nodeListMsg, err := s.client.Call(ctx, "node.list", nil, nil)
	if err == nil && nodeListMsg.Error == "" {
		var list apiserver.NodeList
		if err := json.Unmarshal(nodeListMsg.Payload, &list); err == nil {
			s.mu.Lock()
			for _, n := range list.Items {
				s.nodes[n.Metadata.Name] = n
			}
			s.mu.Unlock()
		}
	}

	podCh, err := s.client.StartStream(ctx, "watch.resource", []byte("/registry/pods"), nil)
	if err != nil {
		return err
	}
	nodeCh, err := s.client.StartStream(ctx, "watch.resource", []byte("/registry/nodes"), nil)
	if err != nil {
		return err
	}

	go s.watchPods(ctx, podCh)
	go s.watchNodes(ctx, nodeCh)
	go s.run(ctx)

	return nil
}

func (s *Scheduler) watchPods(ctx context.Context, ch <-chan *pb.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev watch.Event
			if err := json.Unmarshal(msg.Payload, &ev); err == nil && ev.Type != "" {
				if ev.Type == watch.PUT {
					var pod apiserver.Pod
					if err := json.Unmarshal(ev.Value, &pod); err == nil {
						if pod.Spec.NodeName == "" && !pod.IsTerminated() {
							select {
							case s.podqueue <- &pod:
							default:
							}
						}
					}
				}
			} else {
				var pod apiserver.Pod
				if err := json.Unmarshal(msg.Payload, &pod); err == nil {
					if pod.Spec.NodeName == "" && !pod.IsTerminated() {
						select {
						case s.podqueue <- &pod:
						default:
						}
					}
				}
			}
		}
	}
}

func (s *Scheduler) watchNodes(ctx context.Context, ch <-chan *pb.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev watch.Event
			if err := json.Unmarshal(msg.Payload, &ev); err == nil && ev.Type != "" {
				if ev.Type == watch.DELETE {
					var node apiserver.Node
					if err := json.Unmarshal(ev.PrevValue, &node); err == nil && node.Metadata.Name != "" {
						s.RemoveNode(node.Metadata.Name)
					} else {
						parts := strings.Split(ev.Key, "/")
						if len(parts) > 0 {
							s.RemoveNode(parts[len(parts)-1])
						}
					}
				} else if ev.Type == watch.PUT {
					var node apiserver.Node
					if err := json.Unmarshal(ev.Value, &node); err == nil {
						s.AddNode(node)
					}
				}
			} else {
				var node apiserver.Node
				if err := json.Unmarshal(msg.Payload, &node); err == nil {
					s.AddNode(node)
				}
			}
		}
	}
}

func (s *Scheduler) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case pod, ok := <-s.podqueue:
			if !ok {
				return
			}
			if err := s.ScheduleOne(ctx, pod); err != nil {
				if errors.Is(err, ErrNoNodesAvailable) {
					go func(p *apiserver.Pod) {
						select {
						case <-time.After(50 * time.Millisecond):
							select {
							case s.podqueue <- p:
							default:
							}
						case <-ctx.Done():
						case <-s.stopCh:
						}
					}(pod)
				}
			}
		}
	}
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Scheduler) Close() {
	s.Stop()
}
