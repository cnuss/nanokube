package docker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	dockerConfig, hostConfig, netConfig := toSandboxContainerConfig(config, b.name)
	name := sandboxContainerName(config)

	logger.Debug().Str("name", name).Str("uid", config.GetMetadata().GetUid()).Msg("CRI RunPodSandbox")

	// Remove any stale sandbox with the same name from a previous run
	b.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})

	resp, err := b.client.ContainerCreate(ctx, dockerConfig, hostConfig, netConfig, nil, name)
	if err != nil {
		return "", fmt.Errorf("create sandbox: %w", err)
	}

	if err := b.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		b.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("start sandbox: %w", err)
	}

	logger.Debug().Str("id", resp.ID[:12]).Str("name", name).Msg("CRI sandbox started")
	return resp.ID, nil
}

func (b *Backend) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	logger.Debug().Str("id", podSandboxID[:12]).Msg("CRI StopPodSandbox")
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
	logger.Debug().Str("id", podSandboxID[:12]).Msg("CRI RemovePodSandbox")

	// Capture pod UID before removing the sandbox container (for volume cleanup)
	var podUID string
	if inspect, err := b.client.ContainerInspect(ctx, podSandboxID); err == nil {
		podUID = inspect.Config.Labels[labelSandboxUID]
	}

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
	if err != nil && !isNotFoundOrNotRunning(err) {
		return err
	}

	// Clean up volumes associated with this pod
	if podUID != "" {
		volumes, listErr := b.ListVolumes(ctx, map[string]string{labelSandboxUID: podUID})
		if listErr == nil {
			for _, v := range volumes {
				if rmErr := b.RemoveVolume(ctx, v); rmErr != nil {
					logger.Warn().Err(rmErr).Str("volume", v).Msg("failed to remove pod volume")
				}
			}
		}
	}

	return nil
}

// RemovePodSandboxes removes all sandboxes managed by this backend.
func (b *Backend) RemovePodSandboxes(ctx context.Context) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", labelContainerType+"="+containerTypeSandbox)
	f.Add("label", labelManagedBy+"="+b.name)

	sandboxes, err := b.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, fmt.Errorf("list managed sandboxes: %w", err)
	}

	var removed []string
	var errs []error
	for _, s := range sandboxes {
		if err := b.client.ContainerRemove(ctx, s.ID, container.RemoveOptions{Force: true}); err != nil {
			errs = append(errs, fmt.Errorf("remove sandbox %s: %w", s.ID[:12], err))
		} else {
			removed = append(removed, s.ID)
			logger.Info().Str("id", s.ID[:12]).Msg("cleanup: removed sandbox")
		}
	}
	return removed, errors.Join(errs...)
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
	f.Add("label", labelManagedBy+"="+b.name)

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
		logger.Debug().Str("id", c.ID[:12]).Str("uid", c.Labels[labelSandboxUID]).Str("name", c.Labels[labelSandboxName]).Str("state", c.State).Msg("CRI ListPodSandbox entry")
	}
	logger.Debug().Int("count", len(result)).Msg("CRI ListPodSandbox")
	return result, nil
}

func getIPFromInspect(inspect container.InspectResponse) string {
	if inspect.NetworkSettings != nil {
		// On Docker Desktop (macOS), bridge IPs are not routable from the
		// host. If we're on the bridge network with a published port whose
		// HostIp is empty, return 127.0.0.1 so traffic goes through
		// Docker's port-forwarding proxy.
		if _, onBridge := inspect.NetworkSettings.Networks["bridge"]; onBridge {
			for _, bindings := range inspect.NetworkSettings.Ports {
				for _, b := range bindings {
					if b.HostIP == "" || b.HostIP == "0.0.0.0" {
						return "127.0.0.1"
					}
				}
			}
		}
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
