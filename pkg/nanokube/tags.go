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
	keyLogDirectory    = "log-directory"
	keyLogPath         = "log-path"
	keyLabels          = "labels"
	keyAnnotations     = "annotations"
	KeyDNSAliases      = "dns-aliases"
	KeyHostAliases     = "host-aliases"
	KeySecurityContext = "security-context"
	KeyNetworks        = "networks"
)

var (
	reNotDNS    = regexp.MustCompile(`[^a-z0-9-]`)
	reMultiDash = regexp.MustCompile(`-{2,}`)

	internalKeys = map[string]bool{
		keyManagedBy: true, keyType: true, keyName: true, keyNamespace: true,
		keyUID: true, keySandboxUID: true, keyAttempt: true,
		keyLogDirectory: true, keyLogPath: true, keyLabels: true, keyAnnotations: true,
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

func encodeBlob(m map[string]string) string {
	b, _ := json.Marshal(m)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeBlob(s string) map[string]string {
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

// ---------------------------------------------------------------------------
// TagBuilder
// ---------------------------------------------------------------------------

// TagBuilder constructs a set of tags (stored as Docker labels) that encode
// nanokube metadata, Kubernetes labels, and Kubernetes annotations.
type TagBuilder struct {
	prefix string
	tags   map[string]string
}

func NewTagBuilder(driver Driver) *TagBuilder {
	b := &TagBuilder{
		prefix: driver.Options().Name(),
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
	return b.get(keyLogDirectory)
}

func (b *TagBuilder) LogPath() string {
	dir := b.get(keyLogDirectory)
	path := b.get(keyLogPath)
	if dir == "" || path == "" {
		return ""
	}
	return filepath.Join(dir, path)
}

func (b *TagBuilder) ExtractLabels() map[string]string {
	if blob := b.get(keyLabels); blob != "" {
		if m := decodeBlob(blob); m != nil {
			return m
		}
	}
	return make(map[string]string)
}

func (b *TagBuilder) ExtractAnnotations() map[string]string {
	if blob := b.get(keyAnnotations); blob != "" {
		if m := decodeBlob(blob); m != nil {
			return m
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

func (b *TagBuilder) WithLabels(labels map[string]string) *TagBuilder {
	if len(labels) == 0 {
		return b
	}
	blobKey := b.Key(keyLabels)
	packed := decodeBlob(b.tags[blobKey])
	if packed == nil {
		packed = make(map[string]string, len(labels))
	}
	maps.Copy(packed, labels)
	b.tags[blobKey] = encodeBlob(packed)
	return b
}

func (b *TagBuilder) WithAnnotations(annotations map[string]string) *TagBuilder {
	if len(annotations) == 0 {
		return b
	}
	blobKey := b.Key(keyAnnotations)
	packed := decodeBlob(b.tags[blobKey])
	if packed == nil {
		packed = make(map[string]string, len(annotations))
	}
	maps.Copy(packed, annotations)
	b.tags[blobKey] = encodeBlob(packed)
	return b
}

func (b *TagBuilder) WithHostAliases(aliases []corev1.HostAlias) *TagBuilder {
	if len(aliases) == 0 {
		return b
	}
	raw, _ := json.Marshal(aliases)
	return b.WithAnnotations(map[string]string{
		b.Key(KeyHostAliases): string(raw),
	})
}

func (b *TagBuilder) WithLogDirectory(logDir string) *TagBuilder {
	if logDir != "" {
		b.set(keyLogDirectory, logDir)
	}
	return b
}

func (b *TagBuilder) WithLogPath(logPath string) *TagBuilder {
	if logPath != "" {
		b.set(keyLogPath, logPath)
	}
	return b
}

// Build generates the container/resource name and returns the final tag map.
func (b *TagBuilder) Build() (string, map[string]string, error) {
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
