package labels

import "strings"

var (
	typeKey        = "type"
	managedByKey   = "managed-by"
	sandboxIDKey   = "sandbox-id"
	containerIDKey = "container-id"
	volumeIDKey    = "volume-id"
	logPathKey     = "container-log-path"

	unknownType   = "unknown"
	sandboxType   = "sandbox"
	containerType = "container"
	volumeType    = "volume"
)

type LabelProvider interface {
	Name() string
	Prefix(key string) string
	AnnotationPrefix(key string) string
	NewBuilder(userLabels map[string]string) *LabelBuilder
	LogPath(dockerLabels map[string]string) string
	SandboxID(dockerLabels map[string]string) string
	ManagedByFilter() string
	TypeFilter(t string) string
	SandboxIDFilter(id string) string
	IsInternal(key string) bool
	IsAnnotation(key string) bool
	ExtractLabels(dockerLabels map[string]string) map[string]string
	ExtractAnnotations(dockerLabels map[string]string) map[string]string
}

func NewLabels(name string) LabelProvider {
	labels := &LabelProviderImpl{name: name}
	return labels
}

type LabelProviderImpl struct {
	name string
}

func (l *LabelProviderImpl) Name() string {
	return l.name
}

func (l *LabelProviderImpl) Prefix(key string) string {
	return l.name + "." + key
}

func (l *LabelProviderImpl) AnnotationPrefix(key string) string {
	return l.Prefix("annotation." + key)
}

// LogPath extracts the CRI log path from a Docker labels map.
func (l *LabelProviderImpl) LogPath(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(logPathKey)]
}

// SandboxID extracts the sandbox ID from a Docker labels map.
func (l *LabelProviderImpl) SandboxID(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(sandboxIDKey)]
}

// ManagedByFilter returns a Docker label filter string for managed-by.
func (l *LabelProviderImpl) ManagedByFilter() string {
	return l.Prefix(managedByKey) + "=" + l.name
}

// TypeFilter returns a Docker label filter string for a given type.
func (l *LabelProviderImpl) TypeFilter(t string) string {
	return l.Prefix(typeKey) + "=" + t
}

// SandboxIDFilter returns a Docker label filter string for a given sandbox ID.
func (l *LabelProviderImpl) SandboxIDFilter(id string) string {
	return l.Prefix(sandboxIDKey) + "=" + id
}

// IsInternal returns true if the label key is an internal management label.
func (l *LabelProviderImpl) IsInternal(key string) bool {
	prefix := l.name + "."
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	suffix := key[len(prefix):]
	switch suffix {
	case typeKey, managedByKey, sandboxIDKey, containerIDKey, logPathKey:
		return true
	}
	return false
}

// IsAnnotation returns true if the label key is a stored annotation.
func (l *LabelProviderImpl) IsAnnotation(key string) bool {
	return strings.HasPrefix(key, l.Prefix("annotation."))
}

// ExtractLabels extracts CRI labels from Docker labels, excluding internal
// management labels and annotation-prefixed labels.
func (l *LabelProviderImpl) ExtractLabels(dockerLabels map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range dockerLabels {
		if l.IsInternal(k) || l.IsAnnotation(k) {
			continue
		}
		out[k] = v
	}
	return out
}

// ExtractAnnotations extracts CRI annotations from Docker labels.
func (l *LabelProviderImpl) ExtractAnnotations(dockerLabels map[string]string) map[string]string {
	annPrefix := l.Prefix("annotation.")
	out := make(map[string]string)
	for k, v := range dockerLabels {
		if strings.HasPrefix(k, annPrefix) {
			out[strings.TrimPrefix(k, annPrefix)] = v
		}
	}
	return out
}
