package labels

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
)

type ResourceType string

var (
	managedByKey   = "managed-by"
	typeKey        = "type"
	nameKey        = "name"
	namespaceKey   = "namespace"
	uidKey         = "uid"
	parentUIDKey   = "parent-uid"
	attemptKey     = "attempt"
	logDirKey      = "log-directory"
	logPathKey     = "log-path"
	labelsKey      = "labels"
	annotationsKey = "annotations"
	DNSAliasesKey  = "dns-aliases"

	TypeUnknown   ResourceType = "unknown"
	TypeSandbox   ResourceType = "sandbox"
	TypeContainer ResourceType = "container"
	TypeVolume    ResourceType = "volume"
)

type LabelProvider interface {
	Name() string
	Prefix(key string) string
	NewBuilder(userLabels map[string]string) *LabelBuilder

	// Accessors
	GetName(dockerLabels map[string]string) string
	Namespace(dockerLabels map[string]string) string
	UID(dockerLabels map[string]string) string
	ParentUID(dockerLabels map[string]string) string
	Attempt(dockerLabels map[string]string) uint32
	LogDirectory(dockerLabels map[string]string) string
	LogPath(dockerLabels map[string]string) string

	// Key accessors
	UIDKey() string
	NameKey() string
	DNSAliases(annotations map[string]string) []string

	// Filters
	ManagedByFilter() string
	TypeFilter(t string) string
	ParentUIDFilter(id string) string

	// Label management
	IsInternal(key string) bool
	ExtractLabels(dockerLabels map[string]string) map[string]string
	ExtractAnnotations(dockerLabels map[string]string) map[string]string
}

func NewLabels(name string) LabelProvider {
	return &LabelProviderImpl{name: name}
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

func (l *LabelProviderImpl) GetName(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(nameKey)]
}

func (l *LabelProviderImpl) Namespace(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(namespaceKey)]
}

func (l *LabelProviderImpl) UID(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(uidKey)]
}

func (l *LabelProviderImpl) ParentUID(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(parentUIDKey)]
}

func (l *LabelProviderImpl) Attempt(dockerLabels map[string]string) uint32 {
	v, _ := strconv.ParseUint(dockerLabels[l.Prefix(attemptKey)], 10, 32)
	return uint32(v)
}

func (l *LabelProviderImpl) LogDirectory(dockerLabels map[string]string) string {
	return dockerLabels[l.Prefix(logDirKey)]
}

func (l *LabelProviderImpl) LogPath(dockerLabels map[string]string) string {
	dir := dockerLabels[l.Prefix(logDirKey)]
	path := dockerLabels[l.Prefix(logPathKey)]
	if dir == "" || path == "" {
		return ""
	}
	return filepath.Join(dir, path)
}

func (l *LabelProviderImpl) UIDKey() string {
	return l.Prefix(uidKey)
}

func (l *LabelProviderImpl) NameKey() string {
	return l.Prefix(nameKey)
}

func (l *LabelProviderImpl) DNSAliases(annotations map[string]string) []string {
	if v, ok := annotations[l.Prefix(DNSAliasesKey)]; ok && v != "" {
		return strings.Split(v, ",")
	}
	return nil
}

func (l *LabelProviderImpl) ManagedByFilter() string {
	return l.Prefix(managedByKey) + "=" + l.name
}

func (l *LabelProviderImpl) TypeFilter(t string) string {
	return l.Prefix(typeKey) + "=" + t
}

func (l *LabelProviderImpl) ParentUIDFilter(id string) string {
	return l.Prefix(parentUIDKey) + "=" + id
}

func (l *LabelProviderImpl) IsInternal(key string) bool {
	prefix := l.name + "."
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	suffix := key[len(prefix):]
	switch suffix {
	case managedByKey, typeKey, nameKey, namespaceKey, uidKey, parentUIDKey, attemptKey, logDirKey, logPathKey, labelsKey, annotationsKey:
		return true
	}
	return false
}

func encodeLabelsBlob(m map[string]string) string {
	b, _ := json.Marshal(m)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeLabelsBlob(s string) map[string]string {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	var m map[string]string
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	return m
}

func (l *LabelProviderImpl) ExtractLabels(dockerLabels map[string]string) map[string]string {
	if blob, ok := dockerLabels[l.Prefix(labelsKey)]; ok {
		if m := decodeLabelsBlob(blob); m != nil {
			return m
		}
	}
	return make(map[string]string)
}

func (l *LabelProviderImpl) ExtractAnnotations(dockerLabels map[string]string) map[string]string {
	if blob, ok := dockerLabels[l.Prefix(annotationsKey)]; ok {
		if m := decodeLabelsBlob(blob); m != nil {
			return m
		}
	}
	return make(map[string]string)
}
