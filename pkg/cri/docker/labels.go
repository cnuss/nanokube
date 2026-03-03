package docker

import (
	kubelettypes "k8s.io/kubelet/pkg/types"
)

const (
	labelPrefix = "nanokube."

	// Container type labels
	labelContainerType     = labelPrefix + "type"
	containerTypeSandbox   = "sandbox"
	containerTypeContainer = "container"

	// Sandbox labels
	labelSandboxID        = labelPrefix + "sandbox.id"
	labelSandboxNamespace = kubelettypes.KubernetesPodNamespaceLabel
	labelSandboxName      = kubelettypes.KubernetesPodNameLabel
	labelSandboxUID       = kubelettypes.KubernetesPodUIDLabel

	// Container labels
	labelContainerName    = kubelettypes.KubernetesContainerNameLabel
	labelContainerAttempt = labelPrefix + "container.attempt"
	labelLogPath          = labelPrefix + "container.logPath"

	// Volume labels
	labelVolumeName = labelPrefix + "volume.name"

	// Managed-by label to identify CRI-created resources
	labelManagedBy = labelPrefix + "managed-by"

	// Annotations stored as labels with this prefix
	labelAnnotationPrefix = labelPrefix + "annotation."

	// Pause image
	defaultPauseImage = "registry.k8s.io/pause:3.10"
)
