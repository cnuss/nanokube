package labels

import (
	"path/filepath"
	"strconv"
	"strings"
)

var (
	typeKey             = "type"
	managedByKey        = "managed-by"
	sandboxIDKey        = "sandbox-id"
	containerIDKey      = "container-id"
	containerAttemptKey = "container-attempt"
	containerNameKey    = "container-name"
	podNameKey          = "pod-name"
	podNamespaceKey     = "pod-namespace"
	podUIDKey           = "pod-uid"
	volumeIDKey         = "volume-id"
	logDirKey           = "log-directory"
	logPathKey          = "container-log-path"

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
	LogDirectory(dockerLabels map[string]string) string
	LogPath(dockerLabels map[string]string) string
	SandboxID(dockerLabels map[string]string) string
	Attempt(dockerLabels map[string]string) uint32
	ContainerName(dockerLabels map[string]string) string
	PodName(dockerLabels map[string]string) string
	PodNamespace(dockerLabels map[string]string) string
	PodUID(dockerLabels map[string]string) string
	PodUIDKey() string
	ContainerNameKey() string
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

// LogDirectory extracts the CRI log directory from a Docker labels map.
func (l *LabelProviderImpl) LogDirectory(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(logDirKey)]
}

// LogPath returns the full CRI log path by joining the log directory and
// container log path labels. Returns empty if either is missing.
func (l *LabelProviderImpl) LogPath(dockerLabels map[string]string) string {
	dir := dockerLabels[l.Prefix(logDirKey)]
	path := dockerLabels[l.Prefix(logPathKey)]
	if dir == "" || path == "" {
		return ""
	}
	return filepath.Join(dir, path)
}

// SandboxID extracts the sandbox ID from a Docker labels map.
func (l *LabelProviderImpl) SandboxID(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(sandboxIDKey)]
}

// Attempt extracts the container attempt count from a Docker labels map.
func (l *LabelProviderImpl) Attempt(dockerLabels map[string]string) uint32 {
	v, _ := strconv.ParseUint(dockerLabels[l.Prefix(containerAttemptKey)], 10, 32)
	return uint32(v)
}

// ContainerName extracts the container name label.
func (l *LabelProviderImpl) ContainerName(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(containerNameKey)]
}

// PodName extracts the pod name label.
func (l *LabelProviderImpl) PodName(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(podNameKey)]
}

// PodNamespace extracts the pod namespace label.
func (l *LabelProviderImpl) PodNamespace(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(podNamespaceKey)]
}

// PodUID extracts the pod UID label.
func (l *LabelProviderImpl) PodUID(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(podUIDKey)]
}

// PodUIDKey returns the prefixed pod UID label key.
func (l *LabelProviderImpl) PodUIDKey() string {
	return l.Prefix(podUIDKey)
}

// ContainerNameKey returns the prefixed container name label key.
func (l *LabelProviderImpl) ContainerNameKey() string {
	return l.Prefix(containerNameKey)
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
	case typeKey, managedByKey, sandboxIDKey, containerIDKey, containerAttemptKey, containerNameKey, podNameKey, podNamespaceKey, podUIDKey, logDirKey, logPathKey:
		return true
	}
	return false
}

// IsAnnotation returns true if the label key is a stored annotation.
func (l *LabelProviderImpl) IsAnnotation(key string) bool {
	return strings.HasPrefix(key, l.Prefix("annotation."))
}

// ExtractLabels extracts CRI labels from Docker labels, excluding
// annotation-prefixed labels.
func (l *LabelProviderImpl) ExtractLabels(dockerLabels map[string]string) map[string]string {
	out := make(map[string]string)
	for k, v := range dockerLabels {
		if l.IsAnnotation(k) {
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
