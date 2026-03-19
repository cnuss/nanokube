package labels

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
)

var (
	errBuilderSealed = errors.New("LabelBuilder: mutation after Build")
	reNotDNS         = regexp.MustCompile(`[^a-z0-9-]`)
	reMultiDash      = regexp.MustCompile(`-{2,}`)
)

// dnsNormalize converts a string to a valid RFC 1123 DNS label:
// lowercase, alphanumeric + hyphens, no leading/trailing hyphens, max 63 chars.
func normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, ".", "-")
	s = reNotDNS.ReplaceAllString(s, "")
	s = reMultiDash.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	return s
}

type LabelBuilder struct {
	mu     sync.Mutex
	lp     *LabelProviderImpl
	labels map[string]string
	built  bool
	err    error
}

func (l *LabelProviderImpl) NewBuilder(userLabels map[string]string) *LabelBuilder {
	b := &LabelBuilder{
		lp:     l,
		labels: make(map[string]string, len(userLabels)+8),
	}
	b.set(managedByKey, l.name)
	b.set(typeKey, string(TypeUnknown))
	b.WithLabels(userLabels)
	return b
}

func (b *LabelBuilder) get(key string) string {
	return b.labels[b.lp.Prefix(key)]
}

func (b *LabelBuilder) set(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.built {
		b.err = errBuilderSealed
		return
	}
	b.labels[b.lp.Prefix(key)] = value
}

func (b *LabelBuilder) WithType(t ResourceType) *LabelBuilder {
	b.set(typeKey, string(t))
	return b
}

func (b *LabelBuilder) WithName(name string) *LabelBuilder {
	b.set(nameKey, name)
	return b
}

func (b *LabelBuilder) WithNamespace(namespace string) *LabelBuilder {
	b.set(namespaceKey, namespace)
	return b
}

func (b *LabelBuilder) WithUid(uid string) *LabelBuilder {
	b.set(uidKey, uid)
	return b
}

func (b *LabelBuilder) WithParentUid(parentUid string) *LabelBuilder {
	b.set(parentUIDKey, parentUid)
	return b
}

func (b *LabelBuilder) WithAttempt(attempt uint32) *LabelBuilder {
	b.set(attemptKey, fmt.Sprintf("%d", attempt))
	return b
}

// Clone returns an unsealed copy of the builder.
func (b *LabelBuilder) Clone() *LabelBuilder {
	b.mu.Lock()
	defer b.mu.Unlock()
	clone := &LabelBuilder{
		lp:     b.lp,
		labels: make(map[string]string, len(b.labels)),
	}
	for k, v := range b.labels {
		clone.labels[k] = v
	}
	return clone
}

// IncrementAttempt bumps the attempt counter by one.
func (b *LabelBuilder) IncrementAttempt() *LabelBuilder {
	b.mu.Lock()
	current, _ := strconv.ParseUint(b.labels[b.lp.Prefix(attemptKey)], 10, 32)
	b.mu.Unlock()
	b.set(attemptKey, fmt.Sprintf("%d", current+1))
	return b
}

func (b *LabelBuilder) WithLabels(labels map[string]string) *LabelBuilder {
	if len(labels) == 0 {
		return b
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	blobKey := b.lp.Prefix(labelsKey)
	packed := decodeLabelsBlob(b.labels[blobKey])
	if packed == nil {
		packed = make(map[string]string, len(labels))
	}
	for k, v := range labels {
		packed[k] = v
	}
	b.labels[blobKey] = encodeLabelsBlob(packed)
	return b
}

func (b *LabelBuilder) WithAnnotations(annotations map[string]string) *LabelBuilder {
	if len(annotations) == 0 {
		return b
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	blobKey := b.lp.Prefix(annotationsKey)
	packed := decodeLabelsBlob(b.labels[blobKey])
	if packed == nil {
		packed = make(map[string]string, len(annotations))
	}
	for k, v := range annotations {
		packed[k] = v
	}
	b.labels[blobKey] = encodeLabelsBlob(packed)
	return b
}

func (b *LabelBuilder) WithHostAliases(aliases []v1.HostAlias) *LabelBuilder {
	if len(aliases) == 0 {
		return b
	}
	raw, _ := json.Marshal(aliases)
	return b.WithAnnotations(map[string]string{
		b.lp.Prefix(HostAliasesKey): string(raw),
	})
}

func (b *LabelBuilder) WithLogDirectory(logDir string) *LabelBuilder {
	if logDir != "" {
		b.set(logDirKey, logDir)
	}
	return b
}

func (b *LabelBuilder) WithLogPath(logPath string) *LabelBuilder {
	if logPath != "" {
		b.set(logPathKey, logPath)
	}
	return b
}

// Build seals the builder and returns the generated name and label map.
func (b *LabelBuilder) Build() (string, map[string]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return "", nil, b.err
	}
	b.built = true
	name := normalize(b.get(nameKey))
	if attempt := b.get(attemptKey); attempt != "" {
		name += "-" + attempt
	}
	if t := b.get(typeKey); t != string(TypeContainer) && t != string(TypeVolume) {
		name += "-" + t
	}
	namespace := normalize(b.get(namespaceKey))
	if namespace != "" {
		name = name + "." + namespace
	}
	name = name + "." + normalize(b.lp.name)

	return name, b.labels, nil
}

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
