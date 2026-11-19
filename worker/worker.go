package worker

import (
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/jsndz/bottle/apiserver"
	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
	"github.com/jsndz/bottle/watch"
)

type WorkerConfig struct {
	NodeName          string
	APIServerAddr     string
	ListenAddr        string
	Capacity          apiserver.NodeResources
	HeartbeatInterval time.Duration
}

type Worker struct {
	Cfg      WorkerConfig
	Client   *rpc.Client
	Address  string
	listener net.Listener
	running  map[string]*exec.Cmd
	mu       sync.Mutex
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewWorker(cfg WorkerConfig, pool *rpc.GlobalPool) *Worker {
	if cfg.Capacity.CPU <= 0 {
		cfg.Capacity.CPU = int64(runtime.NumCPU())
	}
	if cfg.Capacity.Memory <= 0 {
		cfg.Capacity.Memory = 4096
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	client := rpc.NewClient(cfg.APIServerAddr, pool)
	return &Worker{
		Client:  client,
		Cfg:     cfg,
		Address: "",
		running: make(map[string]*exec.Cmd),
		stopCh:  make(chan struct{}),
	}
}

func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.Tick(w.Cfg.HeartbeatInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			case <-ticker:
				payload, _ := json.Marshal(apiserver.NodeHeartbeat{Name: w.Cfg.NodeName})
				_, _ = w.Client.Call(ctx, "node.heartbeat", payload, nil)
			}
		}
	}()
}

func (w *Worker) UpdatePodPhase(pod *apiserver.Pod, phase string) {
	if pod == nil {
		return
	}
	pod.Status.Phase = phase
	status := apiserver.PodStatusUpdate{
		Namespace: pod.Metadata.Namespace,
		Name:      pod.Metadata.Name,
		Phase:     phase,
	}
	data, _ := json.Marshal(status)
	_, _ = w.Client.Call(context.Background(), "pod.updateStatus", data, nil)
}

func (w *Worker) runPod(pod *apiserver.Pod) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return
	}
	c := pod.Spec.Containers[0]
	if len(c.Command) == 0 {
		return
	}

	podKey := pod.Metadata.Namespace + "/" + pod.Metadata.Name

	w.mu.Lock()
	if _, exists := w.running[podKey]; exists {
		w.mu.Unlock()
		return
	}

	cmd := exec.Command(c.Command[0], c.Command[1:]...)
	if err := cmd.Start(); err != nil {
		w.mu.Unlock()
		w.UpdatePodPhase(pod, "Failed")
		return
	}

	w.running[podKey] = cmd
	w.mu.Unlock()

	w.UpdatePodPhase(pod, "Running")

	go func() {
		err := cmd.Wait()

		w.mu.Lock()
		delete(w.running, podKey)
		w.mu.Unlock()

		if err != nil {
			w.UpdatePodPhase(pod, "Failed")
		} else {
			w.UpdatePodPhase(pod, "Succeeded")
		}
	}()
}

func (w *Worker) watchPods(ctx context.Context, ch <-chan *pb.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev watch.Event
			var pod apiserver.Pod
			if err := json.Unmarshal(msg.Payload, &ev); err == nil && ev.Type != "" {
				if ev.Type == watch.DELETE {
					var deletedPod apiserver.Pod
					if err := json.Unmarshal(ev.PrevValue, &deletedPod); err == nil && deletedPod.Metadata.Name != "" {
						podKey := deletedPod.Metadata.Namespace + "/" + deletedPod.Metadata.Name
						w.killPod(podKey)
					}
					continue
				}
				if err := json.Unmarshal(ev.Value, &pod); err != nil {
					continue
				}
			} else {
				if err := json.Unmarshal(msg.Payload, &pod); err != nil {
					continue
				}
			}

			if pod.Spec.NodeName != w.Cfg.NodeName {
				continue
			}

			if !pod.IsTerminated() {
				w.runPod(&pod)
			}
		}
	}
}

func (w *Worker) killPod(podKey string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cmd, ok := w.running[podKey]; ok {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		delete(w.running, podKey)
	}
}

func (w *Worker) Start(ctx ...context.Context) error {
	runCtx := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		runCtx = ctx[0]
	}

	listener, err := net.Listen("tcp", w.Cfg.ListenAddr)
	if err != nil {
		return err
	}
	w.listener = listener
	w.Address = listener.Addr().String()
	go w.serve(listener)

	nodePayload, _ := json.Marshal(apiserver.Node{
		Metadata: apiserver.Metadata{Name: w.Cfg.NodeName},
		Spec:     apiserver.NodeSpec{Address: w.Address},
		Status: apiserver.NodeStatus{
			Capacity: w.Cfg.Capacity,
		},
	})

	if _, err := w.Client.Call(runCtx, "node.register", nodePayload, nil); err != nil {
		_ = listener.Close()
		return err
	}

	w.heartbeat(runCtx)

	listMsg, err := w.Client.Call(runCtx, "pod.list", []byte(""), nil)
	if err == nil && listMsg != nil && listMsg.Error == "" {
		var list apiserver.PodList
		if err := json.Unmarshal(listMsg.Payload, &list); err == nil {
			for _, pod := range list.Items {
				if pod.Spec.NodeName == w.Cfg.NodeName && !pod.IsTerminated() {
					p := pod
					w.runPod(&p)
				}
			}
		}
	}

	podCh, err := w.Client.StartStream(runCtx, "watch.resource", []byte("/registry/pods"), nil)
	if err != nil {
		_ = listener.Close()
		return err
	}
	go w.watchPods(runCtx, podCh)

	return nil
}

func (w *Worker) serve(list net.Listener) {
	for {
		conn, err := list.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}
}

func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopCh)
		if w.listener != nil {
			_ = w.listener.Close()
		}
		w.mu.Lock()
		for key, cmd := range w.running {
			if cmd != nil && cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			delete(w.running, key)
		}
		w.mu.Unlock()
	})
}

func (w *Worker) IsPodRunning(namespace, name string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.running[namespace+"/"+name]
	return ok
}
