package docker

import (
	"time"

	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubelettypes "k8s.io/kubelet/pkg/types"
)

var _ backend.Into[string, container.Summary] = &DockerInto{}

type DockerInto struct {
	labels labels.LabelProvider
}

func (i *DockerInto) Filters(b *labels.LabelBuilder) filters.Args {
	f := filters.NewArgs()
	for k, v := range b.InternalLabels() {
		f.Add("label", k+"="+v)
	}
	return f
}

func (i *DockerInto) ContainerState(state string) runtimeapi.ContainerState {
	switch state {
	case "created":
		return runtimeapi.ContainerState_CONTAINER_CREATED
	case "running":
		return runtimeapi.ContainerState_CONTAINER_RUNNING
	default:
		return runtimeapi.ContainerState_CONTAINER_EXITED
	}
}

func (i *DockerInto) PodState(state string) runtimeapi.PodSandboxState {
	switch state {
	case "created", "running":
		return runtimeapi.PodSandboxState_SANDBOX_READY
	default:
		return runtimeapi.PodSandboxState_SANDBOX_NOTREADY
	}
}

func (i *DockerInto) ContainerStatus(state runtimeapi.ContainerState) string {
	switch state {
	case runtimeapi.ContainerState_CONTAINER_CREATED:
		return "created"
	case runtimeapi.ContainerState_CONTAINER_RUNNING:
		return "running"
	default:
		return "exited"
	}
}

func (i *DockerInto) PodStatuses(state runtimeapi.PodSandboxState) []string {
	if state == runtimeapi.PodSandboxState_SANDBOX_READY {
		return []string{"created", "running"}
	}
	return []string{"exited"}
}

func (i *DockerInto) Container(c container.Summary) *runtimeapi.Container {
	return &runtimeapi.Container{
		Id:           c.ID,
		PodSandboxId: i.labels.SandboxID(c.Labels),
		Metadata: &runtimeapi.ContainerMetadata{
			Name: c.Labels[kubelettypes.KubernetesContainerNameLabel],
		},
		Image:       &runtimeapi.ImageSpec{Image: c.Image},
		ImageRef:    c.ImageID,
		State:       i.ContainerState(c.State),
		CreatedAt:   time.Unix(c.Created, 0).UnixNano(),
		Labels:      i.labels.ExtractLabels(c.Labels),
		Annotations: i.labels.ExtractAnnotations(c.Labels),
	}
}

func (i *DockerInto) PodSandbox(c container.Summary) *runtimeapi.PodSandbox {
	return &runtimeapi.PodSandbox{
		Id: c.ID,
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      c.Labels[kubelettypes.KubernetesPodNameLabel],
			Namespace: c.Labels[kubelettypes.KubernetesPodNamespaceLabel],
			Uid:       c.Labels[kubelettypes.KubernetesPodUIDLabel],
		},
		State:       i.PodState(c.State),
		CreatedAt:   time.Unix(c.Created, 0).UnixNano(),
		Labels:      i.labels.ExtractLabels(c.Labels),
		Annotations: i.labels.ExtractAnnotations(c.Labels),
	}
}
