package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	dockerConfig, hostConfig, netConfig := toSandboxContainerConfig(config)
	name := sandboxContainerName(config)

	resp, err := b.client.ContainerCreate(ctx, dockerConfig, hostConfig, netConfig, nil, name)
	if err != nil {
		return "", fmt.Errorf("create sandbox: %w", err)
	}

	if err := b.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		b.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("start sandbox: %w", err)
	}

	return resp.ID, nil
}

func (b *Backend) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	// Stop all containers in this sandbox first
	containers, err := b.ListContainers(ctx, &runtimeapi.ContainerFilter{
		PodSandboxId: podSandboxID,
	})
	if err != nil {
		return err
	}
	for _, c := range containers {
		b.StopContainer(ctx, c.Id, 0)
	}

	timeout := 10
	err = b.client.ContainerStop(ctx, podSandboxID, container.StopOptions{Timeout: &timeout})
	if err != nil && isNotFoundOrNotRunning(err) {
		return nil
	}
	return err
}

func (b *Backend) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	// Remove all containers in this sandbox first
	containers, err := b.ListContainers(ctx, &runtimeapi.ContainerFilter{
		PodSandboxId: podSandboxID,
	})
	if err != nil {
		return err
	}
	for _, c := range containers {
		b.RemoveContainer(ctx, c.Id)
	}

	err = b.client.ContainerRemove(ctx, podSandboxID, container.RemoveOptions{Force: true})
	if err != nil && isNotFoundOrNotRunning(err) {
		return nil
	}
	return err
}

func (b *Backend) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	inspect, err := b.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	state := dockerStateToPodState(inspect.State.Running)

	metadata := &runtimeapi.PodSandboxMetadata{
		Name:      inspect.Config.Labels[labelSandboxName],
		Uid:       inspect.Config.Labels[labelSandboxUID],
		Namespace: inspect.Config.Labels[labelSandboxNamespace],
	}

	status := &runtimeapi.PodSandboxStatus{
		Id:        podSandboxID,
		Metadata:  metadata,
		State:     state,
		CreatedAt: createdAt.UnixNano(),
		Network: &runtimeapi.PodSandboxNetworkStatus{
			Ip: getIPFromInspect(inspect),
		},
		Labels:      extractLabels(inspect.Config.Labels),
		Annotations: extractAnnotations(inspect.Config.Labels),
	}

	resp := &runtimeapi.PodSandboxStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (b *Backend) ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	f := filters.NewArgs()
	f.Add("label", labelContainerType+"="+containerTypeSandbox)
	f.Add("label", labelManagedBy+"="+managedByNanokube)

	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.State != nil {
			if filter.State.State == runtimeapi.PodSandboxState_SANDBOX_READY {
				f.Add("status", "running")
			} else {
				f.Add("status", "exited")
			}
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := b.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		return nil, err
	}

	var result []*runtimeapi.PodSandbox
	for _, c := range containers {
		createdAt := time.Unix(c.Created, 0)
		state := dockerStateToPodState(c.State == "running")
		result = append(result, &runtimeapi.PodSandbox{
			Id: c.ID,
			Metadata: &runtimeapi.PodSandboxMetadata{
				Name:      c.Labels[labelSandboxName],
				Uid:       c.Labels[labelSandboxUID],
				Namespace: c.Labels[labelSandboxNamespace],
			},
			State:       state,
			CreatedAt:   createdAt.UnixNano(),
			Labels:      extractLabels(c.Labels),
			Annotations: extractAnnotations(c.Labels),
		})
	}
	return result, nil
}

func getIPFromInspect(inspect container.InspectResponse) string {
	if inspect.NetworkSettings != nil {
		for _, n := range inspect.NetworkSettings.Networks {
			if n.IPAddress != "" {
				return n.IPAddress
			}
		}
		if inspect.NetworkSettings.IPAddress != "" {
			return inspect.NetworkSettings.IPAddress
		}
	}
	return ""
}
