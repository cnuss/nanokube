package docker

import (
	"fmt"
	"strconv"
	"strings"

	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/go-connections/nat"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// sandboxLabels builds Docker labels for a CRI sandbox container.
func sandboxLabels(config *runtimeapi.PodSandboxConfig, name string) map[string]string {
	labels := map[string]string{
		labelContainerType: containerTypeSandbox,
		labelManagedBy:     name,
		labelSandboxName:   config.GetMetadata().GetName(),
		labelSandboxUID:    config.GetMetadata().GetUid(),
	}
	if ns := config.GetMetadata().GetNamespace(); ns != "" {
		labels[labelSandboxNamespace] = ns
	}
	for k, v := range config.GetLabels() {
		labels[k] = v
	}
	for k, v := range config.GetAnnotations() {
		labels[labelAnnotationPrefix+k] = v
	}
	return labels
}

// containerLabels builds Docker labels for a CRI container.
func containerLabels(sandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig, name string) map[string]string {
	labels := map[string]string{
		labelContainerType:    containerTypeContainer,
		labelManagedBy:        name,
		labelSandboxID:        sandboxID,
		labelContainerName:    config.GetMetadata().GetName(),
		labelContainerAttempt: fmt.Sprintf("%d", config.GetMetadata().GetAttempt()),
		labelSandboxName:      sandboxConfig.GetMetadata().GetName(),
		labelSandboxNamespace: sandboxConfig.GetMetadata().GetNamespace(),
		labelSandboxUID:       sandboxConfig.GetMetadata().GetUid(),
	}
	for k, v := range config.GetLabels() {
		labels[k] = v
	}
	for k, v := range config.GetAnnotations() {
		labels[labelAnnotationPrefix+k] = v
	}
	return labels
}

// containerName generates a Docker container name for a CRI container.
func containerName(sandboxConfig *runtimeapi.PodSandboxConfig, config *runtimeapi.ContainerConfig) string {
	return fmt.Sprintf("k8s_%s_%s_%s_%d",
		config.GetMetadata().GetName(),
		sandboxConfig.GetMetadata().GetName(),
		sandboxConfig.GetMetadata().GetNamespace(),
		config.GetMetadata().GetAttempt(),
	)
}

// sandboxContainerName generates a Docker container name for a CRI sandbox.
func sandboxContainerName(config *runtimeapi.PodSandboxConfig) string {
	return fmt.Sprintf("k8s_POD_%s_%s_%s",
		config.GetMetadata().GetName(),
		config.GetMetadata().GetNamespace(),
		config.GetMetadata().GetUid(),
	)
}

// toContainerConfig builds a Docker container config from CRI container config.
func toContainerConfig(config *runtimeapi.ContainerConfig, sandboxID string, sandboxConfig *runtimeapi.PodSandboxConfig, name string, mounts critypes.MountLookup) (*dockercontainer.Config, *dockercontainer.HostConfig) {
	image := config.GetImage().GetImage()

	envs := make([]string, 0, len(config.GetEnvs()))
	for _, kv := range config.GetEnvs() {
		envs = append(envs, kv.GetKey()+"="+kv.GetValue())
	}

	dockerConfig := &dockercontainer.Config{
		Image:      image,
		Entrypoint: strslice.StrSlice(config.GetCommand()),
		Cmd:        strslice.StrSlice(config.GetArgs()),
		Env:        envs,
		WorkingDir: config.GetWorkingDir(),
		Labels:     containerLabels(sandboxID, config, sandboxConfig, name),
		StdinOnce:  config.GetStdinOnce(),
		OpenStdin:  config.GetStdin(),
		Tty:        config.GetTty(),
	}

	hostConfig := &dockercontainer.HostConfig{
		NetworkMode: dockercontainer.NetworkMode("container:" + sandboxID),
		IpcMode:     dockercontainer.IpcMode("container:" + sandboxID),
		PidMode:     dockercontainer.PidMode("container:" + sandboxID),
	}

	// Mounts — check if host path is a tracked tmpfs or named volume; if so
	// use Docker native tmpfs/volume instead of a host bind mount. Everything
	// else uses Binds (legacy format).
	for _, m := range config.GetMounts() {
		if mounts != nil {
			if opts, ok := mounts.GetTmpfs(m.GetHostPath()); ok {
				if hostConfig.Tmpfs == nil {
					hostConfig.Tmpfs = make(map[string]string)
				}
				hostConfig.Tmpfs[m.GetContainerPath()] = opts
				continue
			}
			if volName, ok := mounts.GetVolume(m.GetHostPath()); ok {
				bind := name + "-" + volName + ":" + m.GetContainerPath()
				if m.GetReadonly() {
					bind += ":ro"
				}
				hostConfig.Binds = append(hostConfig.Binds, bind)
				continue
			}
		}
		bind := m.GetHostPath() + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		hostConfig.Binds = append(hostConfig.Binds, bind)
	}

	// Linux resources
	if linux := config.GetLinux(); linux != nil {
		if res := linux.GetResources(); res != nil {
			hostConfig.Resources.CPUShares = res.GetCpuShares()
			hostConfig.Resources.Memory = res.GetMemoryLimitInBytes()
			hostConfig.Resources.CPUQuota = res.GetCpuQuota()
			hostConfig.Resources.CPUPeriod = res.GetCpuPeriod()
			hostConfig.Resources.OomKillDisable = boolPtr(res.GetOomScoreAdj() == -998)
		}
		if sc := linux.GetSecurityContext(); sc != nil {
			if sc.GetPrivileged() {
				hostConfig.Privileged = true
			}
			if sc.GetReadonlyRootfs() {
				hostConfig.ReadonlyRootfs = true
			}
		}
	}

	// Log path
	if logPath := config.GetLogPath(); logPath != "" {
		hostConfig.LogConfig = dockercontainer.LogConfig{
			Type: "json-file",
			Config: map[string]string{
				"max-size": "10m",
				"max-file": "3",
			},
		}
	}

	return dockerConfig, hostConfig
}

// toSandboxContainerConfig builds a Docker config for a pause (sandbox) container.
func toSandboxContainerConfig(config *runtimeapi.PodSandboxConfig, name string) (*dockercontainer.Config, *dockercontainer.HostConfig, *network.NetworkingConfig) {
	dockerConfig := &dockercontainer.Config{
		Image:    defaultPauseImage,
		Hostname: config.GetHostname(),
		Labels:   sandboxLabels(config, name),
	}

	hostConfig := &dockercontainer.HostConfig{
		IpcMode: dockercontainer.IpcMode("shareable"),
	}

	// DNS
	if dns := config.GetDnsConfig(); dns != nil {
		hostConfig.DNS = dns.GetServers()
		hostConfig.DNSSearch = dns.GetSearches()
		hostConfig.DNSOptions = dns.GetOptions()
	}

	// Port mappings
	if len(config.GetPortMappings()) > 0 {
		hostConfig.PortBindings = nat.PortMap{}
		for _, pm := range config.GetPortMappings() {
			port := nat.Port(fmt.Sprintf("%d/%s", pm.GetContainerPort(), strings.ToLower(pm.GetProtocol().String())))
			hostConfig.PortBindings[port] = []nat.PortBinding{
				{HostIP: pm.GetHostIp(), HostPort: strconv.Itoa(int(pm.GetHostPort()))},
			}
		}
	}

	// Linux
	if linux := config.GetLinux(); linux != nil {
		if sc := linux.GetSecurityContext(); sc != nil {
			if sc.GetPrivileged() {
				hostConfig.Privileged = true
			}
		}
		if ns := linux.GetSecurityContext().GetNamespaceOptions(); ns != nil {
			if ns.GetNetwork() == runtimeapi.NamespaceMode_NODE {
				hostConfig.NetworkMode = "host"
			}
			if ns.GetPid() == runtimeapi.NamespaceMode_NODE {
				hostConfig.PidMode = "host"
			}
			if ns.GetIpc() == runtimeapi.NamespaceMode_NODE {
				hostConfig.IpcMode = "host"
			}
		}
	}

	// Connect non-host-network sandboxes to the cluster bridge network
	netConfig := &network.NetworkingConfig{}
	if hostConfig.NetworkMode != "host" {
		clusterNet := name + "-bridge"
		netConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			clusterNet: {},
		}
	}

	return dockerConfig, hostConfig, netConfig
}

// dockerStateToContainerState converts Docker container state to CRI state.
func dockerStateToContainerState(state string) runtimeapi.ContainerState {
	switch state {
	case "created":
		return runtimeapi.ContainerState_CONTAINER_CREATED
	case "running":
		return runtimeapi.ContainerState_CONTAINER_RUNNING
	default:
		return runtimeapi.ContainerState_CONTAINER_EXITED
	}
}

// dockerStateToPodState converts Docker container state to CRI sandbox state.
func dockerStateToPodState(running bool) runtimeapi.PodSandboxState {
	if running {
		return runtimeapi.PodSandboxState_SANDBOX_READY
	}
	return runtimeapi.PodSandboxState_SANDBOX_NOTREADY
}

// dockerImageToRuntimeImage converts a Docker image summary to CRI image.
func dockerImageToRuntimeImage(img dockerimage.Summary) *runtimeapi.Image {
	tags := img.RepoTags
	digests := img.RepoDigests
	return &runtimeapi.Image{
		Id:          img.ID,
		RepoTags:    tags,
		RepoDigests: digests,
		Size:        uint64(img.Size),
	}
}

// internalLabels are labels managed by the CRI backend that should not be
// returned to the kubelet as user-supplied labels.
var internalLabels = map[string]bool{
	labelContainerType:    true,
	labelManagedBy:        true,
	labelSandboxID:        true,
	labelContainerAttempt: true,
	labelLogPath:          true,
}

// extractLabels extracts CRI labels from Docker labels (excluding internal
// management labels and annotation-prefixed labels).
func extractLabels(dockerLabels map[string]string) map[string]string {
	labels := make(map[string]string)
	for k, v := range dockerLabels {
		if internalLabels[k] || strings.HasPrefix(k, labelAnnotationPrefix) {
			continue
		}
		labels[k] = v
	}
	return labels
}

// extractAnnotations extracts CRI annotations from Docker labels.
func extractAnnotations(dockerLabels map[string]string) map[string]string {
	annotations := make(map[string]string)
	for k, v := range dockerLabels {
		if strings.HasPrefix(k, labelAnnotationPrefix) {
			annotations[strings.TrimPrefix(k, labelAnnotationPrefix)] = v
		}
	}
	return annotations
}

func boolPtr(b bool) *bool {
	return &b
}

// isNotFoundOrNotRunning returns true if the error indicates the container
// was not found or is not running. Used for idempotent Stop/Remove operations.
func isNotFoundOrNotRunning(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "No such") ||
		strings.Contains(msg, "is not running") ||
		strings.Contains(msg, "not found")
}
