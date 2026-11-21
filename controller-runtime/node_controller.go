package controllerruntime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jsndz/bottle/apiserver"
	"github.com/jsndz/bottle/rpc"
)

type NodeReconciler struct {
	client      *rpc.Client
	gracePeriod time.Duration
}

func NewNodeReconciler(client *rpc.Client, gracePeriod time.Duration) *NodeReconciler {
	if gracePeriod <= 0 {
		gracePeriod = 15 * time.Second
	}
	return &NodeReconciler{
		client:      client,
		gracePeriod: gracePeriod,
	}
}

func (r *NodeReconciler) Reconcile(ctx context.Context, req Request) (Result, error) {
	nodeName := req.Name
	if nodeName == "" {
		return Result{}, nil
	}

	if r.client == nil {
		return Result{}, nil
	}

	msg, err := r.client.Call(ctx, "node.get", []byte(nodeName), nil)
	if err != nil {
		return Result{}, err
	}
	if msg == nil || msg.Error != "" {
		return Result{}, nil
	}

	var node apiserver.Node
	if err := json.Unmarshal(msg.Payload, &node); err != nil {
		return Result{}, nil
	}

	now := time.Now().Unix()
	lastHB := node.Status.LastHeartbeat
	var elapsed time.Duration
	if lastHB > 0 {
		elapsed = time.Duration(now-lastHB) * time.Second
	} else {
		elapsed = r.gracePeriod + time.Second
	}

	if elapsed > r.gracePeriod {
		if node.IsReady() {
			node.SetReady(false)
			nodePayload, _ := json.Marshal(node)
			_, _ = r.client.Call(ctx, "node.update", nodePayload, nil)
		}

		podListMsg, err := r.client.Call(ctx, "pod.list", []byte(""), nil)
		if err == nil && podListMsg != nil && podListMsg.Error == "" {
			var podList apiserver.PodList
			if err := json.Unmarshal(podListMsg.Payload, &podList); err == nil {
				for _, pod := range podList.Items {
					if pod.Spec.NodeName == node.Metadata.Name && !pod.IsTerminated() {
						r.evictAndRecreatePod(ctx, &pod)
					}
				}
			}
		}

		return Result{RequeueAfter: r.gracePeriod}, nil
	}

	if !node.IsReady() {
		node.SetReady(true)
		nodePayload, _ := json.Marshal(node)
		_, _ = r.client.Call(ctx, "node.update", nodePayload, nil)
	}

	remaining := r.gracePeriod - elapsed
	if remaining <= 0 {
		remaining = 100 * time.Millisecond
	}
	return Result{RequeueAfter: remaining}, nil
}

func (r *NodeReconciler) evictAndRecreatePod(ctx context.Context, pod *apiserver.Pod) {
	delReq, _ := json.Marshal(map[string]string{
		"namespace": pod.Metadata.Namespace,
		"name":      pod.Metadata.Name,
	})
	_, _ = r.client.Call(ctx, "pod.delete", delReq, nil)

	replacement := apiserver.Pod{
		TypeMeta: pod.TypeMeta,
		Metadata: apiserver.Metadata{
			Name:      pod.Metadata.Name,
			Namespace: pod.Metadata.Namespace,
			Labels:    pod.Metadata.Labels,
		},
		Spec: apiserver.PodSpec{
			NodeName:   "",
			Containers: pod.Spec.Containers,
		},
		Status: apiserver.PodStatus{
			Phase: "Pending",
		},
	}
	createPayload, _ := json.Marshal(replacement)
	_, _ = r.client.Call(ctx, "pod.create", createPayload, nil)
}

func (r *NodeReconciler) Sync(ctx context.Context, queue *WorkQueue) error {
	if r.client == nil || queue == nil {
		return nil
	}
	msg, err := r.client.Call(ctx, "node.list", nil, nil)
	if err != nil || msg == nil || msg.Error != "" {
		return err
	}
	var list apiserver.NodeList
	if err := json.Unmarshal(msg.Payload, &list); err != nil {
		return err
	}
	for _, n := range list.Items {
		queue.Add(n.Metadata.Name)
	}
	return nil
}

func (r *NodeReconciler) StartPeriodicSync(ctx context.Context, queue *WorkQueue, interval time.Duration) {
	if interval <= 0 {
		interval = r.gracePeriod
	}
	go func() {
		_ = r.Sync(ctx, queue)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = r.Sync(ctx, queue)
			}
		}
	}()
}

func NewNodeController(addr string, pool *rpc.GlobalPool, gracePeriod time.Duration) *Controller {
	var client *rpc.Client
	if pool != nil && addr != "" {
		client = rpc.NewClient(addr, pool)
	}
	rec := NewNodeReconciler(client, gracePeriod)
	return NewController("node-controller", "/registry/nodes", addr, pool, rec)
}
