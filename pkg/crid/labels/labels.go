package labels

import (
	"encoding/base64"
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
	TranslateKey(key string) (string, bool)
	TranslateKeys(labels map[string]string) map[string]string
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

// encodeAnnotationKey base64-encodes an annotation key to keep Docker labels
// free of io.kubernetes.* strings.
func encodeAnnotationKey(k string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(k))
}

// decodeAnnotationKey reverses encodeAnnotationKey.
func decodeAnnotationKey(k string) string {
	b, err := base64.RawURLEncoding.DecodeString(k)
	if err != nil {
		return k // pass through if not encoded
	}
	return string(b)
}

// kubeToInternal maps io.kubernetes.* label keys to internal key suffixes.
var kubeToInternal = map[string]string{
	"io.kubernetes.pod.name":       podNameKey,
	"io.kubernetes.pod.namespace":  podNamespaceKey,
	"io.kubernetes.pod.uid":        podUIDKey,
	"io.kubernetes.container.name": containerNameKey,
}

// TranslateKey maps an io.kubernetes.* label key to its nanokube-prefixed
// equivalent. Returns the translated key and true, or "" and false if the
// key does not need translation.
func (l *LabelProviderImpl) TranslateKey(key string) (string, bool) {
	if suffix, ok := kubeToInternal[key]; ok {
		return l.Prefix(suffix), true
	}
	return "", false
}

// TranslateKeys returns a copy of the map with any io.kubernetes.* keys
// replaced by their nanokube-prefixed equivalents.
func (l *LabelProviderImpl) TranslateKeys(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if translated, ok := l.TranslateKey(k); ok {
			out[translated] = v
		} else {
			out[k] = v
		}
	}
	return out
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

// ExtractLabels extracts CRI labels from Docker labels, excluding internal
// and annotation-prefixed labels, and reverse-translating nanokube-prefixed
// identity keys back to io.kubernetes.* for the kubelet.
func (l *LabelProviderImpl) ExtractLabels(dockerLabels map[string]string) map[string]string {
	// Build reverse map: nanokube.pod-name → io.kubernetes.pod.name
	reverse := make(map[string]string, len(kubeToInternal))
	for kubeKey, suffix := range kubeToInternal {
		reverse[l.Prefix(suffix)] = kubeKey
	}

	out := make(map[string]string)
	for k, v := range dockerLabels {
		if l.IsAnnotation(k) {
			continue
		}
		// Reverse-translate identity keys back to io.kubernetes.*
		if kubeKey, ok := reverse[k]; ok {
			out[kubeKey] = v
			continue
		}
		// Skip remaining internal management labels
		if l.IsInternal(k) {
			continue
		}
		out[k] = v
	}
	return out
}

// ExtractAnnotations extracts CRI annotations from Docker labels, expanding
// shortened k8s.* and k8s/* keys back to their io.kubernetes.* originals.
func (l *LabelProviderImpl) ExtractAnnotations(dockerLabels map[string]string) map[string]string {
	annPrefix := l.Prefix("annotation.")
	out := make(map[string]string)
	for k, v := range dockerLabels {
		if after, ok := strings.CutPrefix(k, annPrefix); ok {
			out[decodeAnnotationKey(after)] = v
		}
	}
	return out
}
