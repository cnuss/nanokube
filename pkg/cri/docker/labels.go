package docker

const (
	labelPrefix = "io.kubernetes."

	// Container type labels
	labelContainerType     = labelPrefix + "docker.type"
	containerTypeSandbox   = "sandbox"
	containerTypeContainer = "container"

	// Sandbox labels
	labelSandboxID        = labelPrefix + "sandbox.id"
	labelSandboxNamespace = labelPrefix + "pod.namespace"
	labelSandboxName      = labelPrefix + "pod.name"
	labelSandboxUID       = labelPrefix + "pod.uid"

	// Container labels
	labelContainerName = labelPrefix + "container.name"
	labelLogPath       = labelPrefix + "container.logPath"

	// Managed-by label to identify CRI-created containers
	labelManagedBy    = labelPrefix + "managed-by"
	managedByNanokube = "nanokube-cri"

	// Annotations stored as labels with this prefix
	labelAnnotationPrefix = labelPrefix + "annotation."

	// Pause image
	defaultPauseImage = "registry.k8s.io/pause:3.10"
)
