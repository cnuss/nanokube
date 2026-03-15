package docker

import (
	"time"

	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var _ backend.Into[string, container.Summary] = &DockerInto{}

type DockerInto struct {
	labels labels.LabelProvider
}

func (i *DockerInto) Filters(b *labels.LabelBuilder) filters.Args {
	f := filters.NewArgs()
	for k, v := range b.InternalLabels() {
		if v == "" {
			continue
		}
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
	if state == "running" {
		return runtimeapi.PodSandboxState_SANDBOX_READY
	}
	return runtimeapi.PodSandboxState_SANDBOX_NOTREADY
}

func (i *DockerInto) CreatedAt(ts string) int64 {
	t, _ := time.Parse(time.RFC3339Nano, ts)
	return t.UnixNano()
}

func (i *DockerInto) Container(c container.Summary) *runtimeapi.Container {
	return &runtimeapi.Container{
		Id:           c.ID,
		PodSandboxId: i.labels.ParentUID(c.Labels),
		Metadata: &runtimeapi.ContainerMetadata{
			Name:    i.labels.GetName(c.Labels),
			Attempt: i.labels.Attempt(c.Labels),
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
			Name:      i.labels.GetName(c.Labels),
			Namespace: i.labels.Namespace(c.Labels),
			Uid:       i.labels.UID(c.Labels),
		},
		State:       i.PodState(c.State),
		CreatedAt:   time.Unix(c.Created, 0).UnixNano(),
		Labels:      i.labels.ExtractLabels(c.Labels),
		Annotations: i.labels.ExtractAnnotations(c.Labels),
	}
}
