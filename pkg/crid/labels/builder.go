package labels

import (
	"errors"
	"fmt"
	"sync"
)

var errBuilderSealed = errors.New("LabelBuilder: mutation after Build")

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
	name := fmt.Sprintf("%s_%s_%s_%s_%s_%s",
		b.lp.name,
		b.get(typeKey),
		b.get(nameKey),
		b.get(namespaceKey),
		b.get(uidKey),
		b.get(attemptKey),
	)
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
