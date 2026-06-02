package nanokube

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

// ResourceType identifies what kind of runtime resource a tag set describes.
type ResourceType string

const (
	ResourceUnknown   ResourceType = "unknown"
	ResourceSandbox   ResourceType = "sandbox"
	ResourceContainer ResourceType = "container"
	ResourceVolume    ResourceType = "volume"
	ResourceNetwork   ResourceType = "network"
)

// Tag key suffixes — combined with a driver prefix to form full keys (e.g. "docker.name").
const (
	keyManagedBy       = "managed-by"
	keyType            = "type"
	keyName            = "name"
	keyNamespace       = "namespace"
	keyUID             = "uid"
	keySandboxUID      = "sandbox-uid"
	keyAttempt         = "attempt"
	keySandboxConfig   = "sandbox-config"
	keyContainerConfig = "container-config"
	KeyDNSAliases      = "dns-aliases"
	KeyHostAliases     = "host-aliases"
	KeySecurityContext = "security-context"
	KeyNetwork         = "network"
)

var (
	reNotDNS    = regexp.MustCompile(`[^a-z0-9-]`)
	reMultiDash = regexp.MustCompile(`-{2,}`)

	internalKeys = map[string]bool{
		keyManagedBy: true, keyType: true, keyName: true, keyNamespace: true,
		keyUID: true, keySandboxUID: true, keyAttempt: true,
	}
)

// normalize converts a string to a valid RFC 1123 DNS label.
func normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, ".", "-")
	s = reNotDNS.ReplaceAllString(s, "")
	s = reMultiDash.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	return s
}

// ---------------------------------------------------------------------------
// TagBuilder
// ---------------------------------------------------------------------------

// TagBuilder constructs a set of tags (stored as Docker labels) that encode
// nanokube metadata, Kubernetes labels, and Kubernetes annotations.
type TagBuilder struct {
	prefix string
	tags   map[string]string
}

func NewTagBuilder(driver v1.Driver) *TagBuilder {
	b := &TagBuilder{
		prefix: driver.Context().(v1.Nanokube).Options().Name(),
		tags:   make(map[string]string),
	}
	b.set(keyManagedBy, driver.Name())
	b.set(keyType, string(ResourceUnknown))
	return b
}

func (b *TagBuilder) WithTags(tags map[string]string) *TagBuilder {
	maps.Copy(b.tags, tags)
	return b
}

func (b *TagBuilder) get(key string) string {
	return b.tags[b.Key(key)]
}

func (b *TagBuilder) set(key, value string) {
	b.tags[b.Key(key)] = value
}

// Getters — read values back from a hydrated builder

func (b *TagBuilder) Prefix() string {
	return b.prefix
}

func (b *TagBuilder) Name() string {
	return b.get(keyName)
}

func (b *TagBuilder) Namespace() string {
	return b.get(keyNamespace)
}

func (b *TagBuilder) UID() string {
	return b.get(keyUID)
}

func (b *TagBuilder) SandboxUID() string {
	return b.get(keySandboxUID)
}

func (b *TagBuilder) Attempt() uint32 {
	v, _ := strconv.ParseUint(b.get(keyAttempt), 10, 32)
	return uint32(v)
}

func (b *TagBuilder) LogDirectory() string {
	return b.PodSandboxConfig().GetLogDirectory()
}

func (b *TagBuilder) LogPath() string {
	dir := b.PodSandboxConfig().GetLogDirectory()
	path := b.ContainerConfig().GetLogPath()
	if dir == "" || path == "" {
		return ""
	}
	return filepath.Join(dir, path)
}

func (b *TagBuilder) Labels() map[string]string {
	switch ResourceType(b.get(keyType)) {
	case ResourceContainer:
		if l := b.ContainerConfig().GetLabels(); l != nil {
			return l
		}
	case ResourceSandbox:
		if l := b.PodSandboxConfig().GetLabels(); l != nil {
			return l
		}
	}
	return make(map[string]string)
}

func (b *TagBuilder) Annotations() map[string]string {
	switch ResourceType(b.get(keyType)) {
	case ResourceContainer:
		if a := b.ContainerConfig().GetAnnotations(); a != nil {
			return a
		}
	case ResourceSandbox:
		if a := b.PodSandboxConfig().GetAnnotations(); a != nil {
			return a
		}
	}
	return make(map[string]string)
}

func (b *TagBuilder) DNSAliases() []string {
	if v := b.get(KeyDNSAliases); v != "" {
		return strings.Split(v, ",")
	}
	return nil
}

func (b *TagBuilder) ExtraHosts() []string {
	v := b.get(KeyHostAliases)
	if v == "" {
		return nil
	}
	var entries []corev1.HostAlias
	if json.Unmarshal([]byte(v), &entries) != nil {
		return nil
	}
	var extraHosts []string
	for _, e := range entries {
		for _, h := range e.Hostnames {
			extraHosts = append(extraHosts, h+":"+e.IP)
		}
	}
	return extraHosts
}

func (b *TagBuilder) SecurityContext() string {
	return b.get(KeySecurityContext)
}

// SecurityContextFor returns the per-container security context (uid[:gid])
// stashed by name, falling back to the pod-level value.
func (b *TagBuilder) SecurityContextFor(name string) string {
	if sc := b.get(KeySecurityContext + "/" + name); sc != "" {
		return sc
	}
	return b.get(KeySecurityContext)
}

func (b *TagBuilder) PodSandboxConfig() *runtimev1.PodSandboxConfig {
	cfg := &runtimev1.PodSandboxConfig{}
	if blob := b.get(keySandboxConfig); blob != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(blob); err == nil {
			json.Unmarshal(raw, cfg)
		}
	}
	return cfg
}

func (b *TagBuilder) ContainerConfig() *runtimev1.ContainerConfig {
	cfg := &runtimev1.ContainerConfig{}
	if blob := b.get(keyContainerConfig); blob != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(blob); err == nil {
			json.Unmarshal(raw, cfg)
		}
	}
	return cfg
}

func (b *TagBuilder) IsManaged() bool {
	return b.get(keyManagedBy) != ""
}

func (b *TagBuilder) Key(key string) string {
	return b.prefix + "." + key
}

func (b *TagBuilder) UIDKey() string {
	return b.Key(keyUID)
}

func (b *TagBuilder) SandboxUIDFilter(uid string) string {
	return b.Key(keySandboxUID) + "=" + uid
}

func (b *TagBuilder) TypeFilter(t ResourceType) string {
	return b.Key(keyType) + "=" + string(t)
}

func (b *TagBuilder) WithType(t ResourceType) *TagBuilder {
	b.set(keyType, string(t))
	return b
}

func (b *TagBuilder) WithName(name string) *TagBuilder {
	b.set(keyName, name)
	return b
}

func (b *TagBuilder) WithNamespace(namespace string) *TagBuilder {
	b.set(keyNamespace, namespace)
	return b
}

func (b *TagBuilder) WithUID(uid string) *TagBuilder {
	b.set(keyUID, uid)
	return b
}

func (b *TagBuilder) WithSandboxUID(sandboxUID string) *TagBuilder {
	b.set(keySandboxUID, sandboxUID)
	return b
}

func (b *TagBuilder) WithAttempt(attempt uint32) *TagBuilder {
	b.set(keyAttempt, fmt.Sprintf("%d", attempt))
	return b
}

func (b *TagBuilder) WithPodSandboxConfig(config *runtimev1.PodSandboxConfig) *TagBuilder {
	if config != nil {
		raw, _ := json.Marshal(config)
		b.set(keySandboxConfig, base64.RawURLEncoding.EncodeToString(raw))
	}
	return b
}

func (b *TagBuilder) WithContainerConfig(config *runtimev1.ContainerConfig) *TagBuilder {
	if config != nil {
		raw, _ := json.Marshal(config)
		b.set(keyContainerConfig, base64.RawURLEncoding.EncodeToString(raw))
	}
	return b
}

// Build generates the container/resource name and returns the final tag map.
func (b *TagBuilder) Build() (string, map[string]string, error) {
	switch ResourceType(b.get(keyType)) {
	case ResourceSandbox:
		return fmt.Sprintf("sandbox-0.%s.%s", b.PodSandboxConfig().GetMetadata().GetName(), b.PodSandboxConfig().GetMetadata().GetNamespace()), b.tags, nil
	case ResourceContainer:
		return fmt.Sprintf("%s-%s.%s.%s", b.get(keyName), b.get(keyAttempt), b.PodSandboxConfig().GetMetadata().GetName(), b.PodSandboxConfig().GetMetadata().GetNamespace()), b.tags, nil
	default:
		name := normalize(b.get(keyName))
		if attempt := b.get(keyAttempt); attempt != "" {
			name += "-" + attempt
		}
		if t := b.get(keyType); t != string(ResourceContainer) && t != string(ResourceVolume) {
			name += "-" + t
		}
		namespace := normalize(b.get(keyNamespace))
		if namespace != "" {
			name = name + "." + namespace
		}
		name = name + "." + normalize(b.prefix)
		return name, b.tags, nil
	}
}

// Clone returns a copy of the builder for safe reuse (e.g. retry with IncrementAttempt).
func (b *TagBuilder) Clone() *TagBuilder {
	clone := &TagBuilder{
		prefix: b.prefix,
		tags:   make(map[string]string, len(b.tags)),
	}
	maps.Copy(clone.tags, b.tags)
	return clone
}

// IncrementAttempt bumps the attempt counter by one.
func (b *TagBuilder) IncrementAttempt() *TagBuilder {
	current, _ := strconv.ParseUint(b.get(keyAttempt), 10, 32)
	b.set(keyAttempt, fmt.Sprintf("%d", current+1))
	return b
}

// InternalTags returns only the nanokube-managed tags (non-empty, prefixed keys).
func (b *TagBuilder) InternalTags() map[string]string {
	p := b.prefix + "."
	out := make(map[string]string)
	for k, v := range b.tags {
		if v == "" || !strings.HasPrefix(k, p) || !internalKeys[k[len(p):]] {
			continue
		}
		out[k] = v
	}
	return out
}
