package labels

import (
	"fmt"
	"maps"

	kubelettypes "k8s.io/kubelet/pkg/types"
)

// LabelBuilder constructs a label map in layers without mutating input maps.
type LabelBuilder struct {
	lp     *LabelProviderImpl
	labels map[string]string
}

// NewBuilder starts a builder seeded with the user-supplied labels.
func (l *LabelProviderImpl) NewBuilder(userLabels map[string]string) *LabelBuilder {
	b := &LabelBuilder{
		lp:     l,
		labels: make(map[string]string, len(userLabels)+8),
	}
	maps.Copy(b.labels, userLabels)
	// Defaults
	b.labels[l.Prefix(managedByKey)] = l.name
	b.labels[l.Prefix(typeKey)] = unknownType
	return b
}

// WithSandbox marks this as a sandbox and sets the sandbox ID.
func (b *LabelBuilder) WithSandbox(uid string) *LabelBuilder {
	b.labels[b.lp.Prefix(typeKey)] = sandboxType
	b.labels[b.lp.Prefix(sandboxIDKey)] = uid
	return b
}

// WithContainer marks this as a container and sets the container ID.
func (b *LabelBuilder) WithContainer(sandboxID, containerID string) *LabelBuilder {
	b.labels[b.lp.Prefix(typeKey)] = containerType
	b.labels[b.lp.Prefix(sandboxIDKey)] = sandboxID
	b.labels[b.lp.Prefix(containerIDKey)] = containerID
	return b
}

// WithVolume marks this as a volume.
func (b *LabelBuilder) WithVolume(sandboxID, containerID, volumeID string) *LabelBuilder {
	b.labels[b.lp.Prefix(typeKey)] = volumeType
	b.labels[b.lp.Prefix(sandboxIDKey)] = sandboxID
	b.labels[b.lp.Prefix(containerIDKey)] = containerID
	b.labels[b.lp.Prefix(volumeIDKey)] = volumeID
	return b
}

// WithPod sets name, namespace, and UID together.
func (b *LabelBuilder) WithPod(name, namespace, uid string) *LabelBuilder {
	return b.WithName(name).WithNamespace(namespace).WithUID(uid)
}

// WithName sets the pod name label.
func (b *LabelBuilder) WithName(name string) *LabelBuilder {
	b.labels[kubelettypes.KubernetesPodNameLabel] = name
	return b
}

// WithNamespace sets the pod namespace label.
func (b *LabelBuilder) WithNamespace(namespace string) *LabelBuilder {
	b.labels[kubelettypes.KubernetesPodNamespaceLabel] = namespace
	return b
}

// WithUID sets the pod UID label.
func (b *LabelBuilder) WithUID(uid string) *LabelBuilder {
	b.labels[kubelettypes.KubernetesPodUIDLabel] = uid
	return b
}

// BuildName returns the generated container name.
func (b *LabelBuilder) BuildName() string {
	return fmt.Sprintf("k8s_%s_%s_%s_%s",
		b.labels[b.lp.Prefix(typeKey)],
		b.labels[kubelettypes.KubernetesPodNameLabel],
		b.labels[kubelettypes.KubernetesPodNamespaceLabel],
		b.labels[kubelettypes.KubernetesPodUIDLabel],
	)
}

// WithLabels merges additional labels into the builder.
func (b *LabelBuilder) WithLabels(labels map[string]string) *LabelBuilder {
	maps.Copy(b.labels, labels)
	return b
}

// WithAnnotations stores CRI annotations as prefixed labels.
func (b *LabelBuilder) WithAnnotations(annotations map[string]string) *LabelBuilder {
	for k, v := range annotations {
		b.labels[b.lp.AnnotationPrefix(k)] = v
	}
	return b
}

// BuildLabels returns the final label map.
func (b *LabelBuilder) BuildLabels() map[string]string {
	return b.labels
}
