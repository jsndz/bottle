package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jsndz/bottle/apiserver"
)

func (s *Scheduler) Bind(ctx context.Context, pod *apiserver.Pod, nodeName string) error {
	if pod == nil {
		return errors.New("pod cannot be nil")
	}
	if nodeName == "" {
		return errors.New("nodeName cannot be empty")
	}

	binding := apiserver.Binding{
		Namespace:       pod.Metadata.Namespace,
		Name:            pod.Metadata.Name,
		NodeName:        nodeName,
		ResourceVersion: pod.Metadata.ResourceVersion,
	}

	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}

	resp, err := s.client.Call(ctx, "pod.bind", payload, nil)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		if strings.Contains(resp.Error, "conflict") {
			return apiserver.ErrConflict
		}
		return errors.New(resp.Error)
	}

	var updated apiserver.Pod
	if err := json.Unmarshal(resp.Payload, &updated); err == nil {
		*pod = updated
	}

	return nil
}
