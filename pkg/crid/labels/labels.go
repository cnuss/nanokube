package labels

var (
	typeKey        = "type"
	managedByKey   = "managed-by"
	sandboxIDKey   = "sandbox-id"
	containerIDKey = "container-id"
	volumeIDKey    = "volume-id"

	unknownType   = "unknown"
	sandboxType   = "sandbox"
	containerType = "container"
	volumeType    = "volume"
)

type LabelProvider interface {
	Prefix(key string) string
	AnnotationPrefix(key string) string
	Default() map[string]string
	SandboxLabels(sandboxID string) map[string]string
	ContainerLabels(sandboxID string, containerID string) map[string]string
	VolumeLabels(sandboxID string, containerID string, volumeID string) map[string]string
	Annotate(labels map[string]string, annotations map[string]string)
}

func NewLabels(name string) LabelProvider {
	labels := &LabelProviderImpl{name: name}
	return labels
}

type LabelProviderImpl struct {
	name string
}

func (l *LabelProviderImpl) Prefix(key string) string {
	return l.name + "." + key
}

func (l *LabelProviderImpl) AnnotationPrefix(key string) string {
	return l.Prefix("annotation." + key)
}

func (l *LabelProviderImpl) Default() map[string]string {
	labels := make(map[string]string)
	labels[l.Prefix(managedByKey)] = l.name
	labels[l.Prefix(typeKey)] = unknownType
	return labels
}

func (l *LabelProviderImpl) SandboxLabels(sandboxID string) map[string]string {
	labels := l.Default()
	labels[l.Prefix(sandboxIDKey)] = sandboxID
	labels[l.Prefix(typeKey)] = sandboxType
	return labels
}

func (l *LabelProviderImpl) ContainerLabels(sandboxID string, containerID string) map[string]string {
	labels := l.SandboxLabels(sandboxID)
	labels[l.Prefix(containerIDKey)] = containerID
	labels[l.Prefix(typeKey)] = containerType
	return labels
}

func (l *LabelProviderImpl) VolumeLabels(sandboxID string, containerID string, volumeID string) map[string]string {
	labels := l.ContainerLabels(sandboxID, containerID)
	labels[l.Prefix(volumeIDKey)] = volumeID
	labels[l.Prefix(typeKey)] = volumeType
	return labels
}

func (l *LabelProviderImpl) Annotate(labels map[string]string, annotations map[string]string) {
	for k, v := range annotations {
		labels[l.AnnotationPrefix(k)] = v
	}
}
