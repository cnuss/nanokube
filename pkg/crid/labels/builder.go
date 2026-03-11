package labels

import (
	"errors"
	"fmt"
	"maps"
	"sync"

	kubelettypes "k8s.io/kubelet/pkg/types"
)

var errBuilderSealed = errors.New("LabelBuilder: mutation after BuildLabels")

// LabelBuilder constructs a label map in layers without mutating input maps.
// After BuildLabels is called, the builder is sealed; further With* calls
// record an error that BuildLabels returns on a subsequent call.
type LabelBuilder struct {
	mu     sync.Mutex
	lp     *LabelProviderImpl
	labels map[string]string
	built  bool
	err    error
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

// set writes a label, recording an error if the builder is already sealed.
func (b *LabelBuilder) set(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.built {
		b.err = errBuilderSealed
		return
	}
	b.labels[key] = value
}

// WithType sets the resource type label.
func (b *LabelBuilder) WithType(t string) *LabelBuilder {
	b.set(b.lp.Prefix(typeKey), t)
	return b
}

// WithoutType clears the resource type label, leaving it empty.
func (b *LabelBuilder) WithoutType() *LabelBuilder {
	b.set(b.lp.Prefix(typeKey), "")
	return b
}

// WithSandbox marks this as a sandbox and sets the sandbox ID.
func (b *LabelBuilder) WithSandbox(uid string) *LabelBuilder {
	b.set(b.lp.Prefix(typeKey), sandboxType)
	b.set(b.lp.Prefix(sandboxIDKey), uid)
	return b
}

// WithContainer marks this as a container and sets the container ID.
func (b *LabelBuilder) WithContainer(sandboxID, name string) *LabelBuilder {
	b.set(b.lp.Prefix(typeKey), containerType)
	b.set(b.lp.Prefix(sandboxIDKey), sandboxID)
	b.set(kubelettypes.KubernetesContainerNameLabel, name)
	return b
}

// WithVolume marks this as a volume.
func (b *LabelBuilder) WithVolume(sandboxID, volumeID string) *LabelBuilder {
	b.set(b.lp.Prefix(typeKey), volumeType)
	b.set(b.lp.Prefix(sandboxIDKey), sandboxID)
	b.set(b.lp.Prefix(volumeIDKey), volumeID)
	return b
}

// WithPod sets name, namespace, and UID together.
func (b *LabelBuilder) WithPod(name, namespace, uid string) *LabelBuilder {
	b.set(kubelettypes.KubernetesPodNameLabel, name)
	b.set(kubelettypes.KubernetesPodNamespaceLabel, namespace)
	b.set(kubelettypes.KubernetesPodUIDLabel, uid)
	return b
}

// WithName sets the pod name label.
func (b *LabelBuilder) WithName(name string) *LabelBuilder {
	b.set(kubelettypes.KubernetesPodNameLabel, name)
	return b
}

// WithNamespace sets the pod namespace label.
func (b *LabelBuilder) WithNamespace(namespace string) *LabelBuilder {
	b.set(kubelettypes.KubernetesPodNamespaceLabel, namespace)
	return b
}

// WithUID sets the pod UID label.
func (b *LabelBuilder) WithUID(uid string) *LabelBuilder {
	b.set(kubelettypes.KubernetesPodUIDLabel, uid)
	return b
}

// WithLabels merges additional labels into the builder.
func (b *LabelBuilder) WithLabels(labels map[string]string) *LabelBuilder {
	for k, v := range labels {
		b.set(k, v)
	}
	return b
}

// WithAnnotations stores CRI annotations as prefixed labels.
func (b *LabelBuilder) WithAnnotations(annotations map[string]string) *LabelBuilder {
	for k, v := range annotations {
		b.set(b.lp.AnnotationPrefix(k), v)
	}
	return b
}

// WithLogDirectory sets the CRI log directory label (typically on sandboxes).
func (b *LabelBuilder) WithLogDirectory(logDir string) *LabelBuilder {
	if logDir != "" {
		b.set(b.lp.Prefix(logDirKey), logDir)
	}
	return b
}

// WithLogPath sets the CRI container log path label.
func (b *LabelBuilder) WithLogPath(logPath string) *LabelBuilder {
	if logPath != "" {
		b.set(b.lp.Prefix(logPathKey), logPath)
	}
	return b
}

// Build seals the builder and returns the generated name and label map.
// Returns an error if any With* method was called after a prior Build.
func (b *LabelBuilder) Build() (string, map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return "", nil, b.err
	}
	b.built = true
	containerName := b.labels[kubelettypes.KubernetesContainerNameLabel]
	name := fmt.Sprintf("k8s_%s_%s_%s_%s_%s",
		b.labels[b.lp.Prefix(typeKey)],
		containerName,
		b.labels[kubelettypes.KubernetesPodNameLabel],
		b.labels[kubelettypes.KubernetesPodNamespaceLabel],
		b.labels[kubelettypes.KubernetesPodUIDLabel],
	)
	return name, b.labels, nil
}

// InternalLabels returns the internal management labels, skipping empty values.
func (b *LabelBuilder) InternalLabels() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]string)
	for k, v := range b.labels {
		if v == "" {
			continue
		}
		if b.lp.IsInternal(k) {
			out[k] = v
		}
	}
	return out
}
