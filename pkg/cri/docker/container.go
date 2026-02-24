package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/rs/zerolog/log"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	dockerConfig, hostConfig := toContainerConfig(config, podSandboxID, sandboxConfig, b.name)
	name := containerName(sandboxConfig, config)

	log.Debug().Str("name", name).Str("image", config.GetImage().GetImage()).Str("sandbox", podSandboxID[:12]).Msg("CRI CreateContainer")

	// Store the full CRI log path as a label for symlink creation on start
	if logPath := config.GetLogPath(); logPath != "" {
		logDir := sandboxConfig.GetLogDirectory()
		if logDir != "" {
			dockerConfig.Labels[labelLogPath] = filepath.Join(logDir, logPath)
		}
	}

	for _, m := range config.GetMounts() {
		log.Debug().Str("host", m.GetHostPath()).Str("container", m.GetContainerPath()).Bool("ro", m.GetReadonly()).Msg("CRI mount")
	}

	// Remove any stale container with the same name from a previous run
	b.client.ContainerRemove(ctx, name, container.RemoveOptions{Force: true})

	resp, err := b.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}
	log.Debug().Str("id", resp.ID[:12]).Str("name", name).Msg("CRI container created")
	return resp.ID, nil
}

func (b *Backend) StartContainer(ctx context.Context, containerID string) error {
	log.Debug().Str("id", containerID[:12]).Msg("CRI StartContainer")
	if err := b.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return err
	}
	// Start CRI log writer if a log path was configured
	inspect, err := b.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil // Container started; log is best-effort
	}
	if criLogPath := inspect.Config.Labels[labelLogPath]; criLogPath != "" {
		b.startLogWriter(containerID, criLogPath)
	}
	return nil
}

// startLogWriter starts a CRI log writer goroutine for a container.
func (b *Backend) startLogWriter(containerID, logPath string) {
	os.MkdirAll(filepath.Dir(logPath), 0755)
	// Create the file immediately so it exists when checked
	if f, err := os.Create(logPath); err == nil {
		f.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.logMu.Lock()
	// Cancel any existing writer for this container
	if old, ok := b.logWriters[containerID]; ok {
		old()
	}
	b.logWriters[containerID] = cancel
	b.logMu.Unlock()
	go b.writeCRILog(ctx, containerID, logPath)
}

// stopLogWriter stops a CRI log writer goroutine for a container.
func (b *Backend) stopLogWriter(containerID string) {
	b.logMu.Lock()
	if cancel, ok := b.logWriters[containerID]; ok {
		cancel()
		delete(b.logWriters, containerID)
	}
	b.logMu.Unlock()
}

// writeCRILog streams Docker container logs and writes them in CRI log format.
// CRI format: "<timestamp> <stream> F <message>\n"
func (b *Backend) writeCRILog(ctx context.Context, containerID, logPath string) {
	defer b.stopLogWriter(containerID)
	reader, err := b.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	})
	if err != nil {
		return
	}
	defer reader.Close()

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	// Docker multiplexes stdout/stderr with an 8-byte header per frame:
	// [stream_type(1)][0][0][0][size(4 big-endian)]
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(reader, hdr); err != nil {
			return
		}
		stream := "stdout"
		if hdr[0] == 2 {
			stream = "stderr"
		}
		size := int(hdr[4])<<24 | int(hdr[5])<<16 | int(hdr[6])<<8 | int(hdr[7])
		if size <= 0 {
			continue
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return
		}
		// Docker timestamps format with --timestamps: "2024-01-01T00:00:00.000000000Z <message>"
		// Split on first space to extract timestamp
		line := string(payload)
		ts := time.Now().Format(time.RFC3339Nano)
		msg := line
		if idx := strings.IndexByte(line, ' '); idx > 0 {
			ts = line[:idx]
			msg = line[idx+1:]
		}
		// Ensure msg ends with newline for CRI format
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Fprintf(f, "%s %s F %s", ts, stream, msg)
		f.Sync()
	}
}

func (b *Backend) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	log.Debug().Str("id", containerID[:12]).Int64("timeout", timeout).Msg("CRI StopContainer")
	t := int(timeout)
	err := b.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &t})
	if err != nil && isNotFoundOrNotRunning(err) {
		return nil
	}
	return err
}

func (b *Backend) RemoveContainer(ctx context.Context, containerID string) error {
	log.Debug().Str("id", containerID[:12]).Msg("CRI RemoveContainer")
	err := b.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
	if err != nil && isNotFoundOrNotRunning(err) {
		return nil
	}
	return err
}

func (b *Backend) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	f := filters.NewArgs()
	f.Add("label", labelContainerType+"="+containerTypeContainer)
	f.Add("label", labelManagedBy+"="+b.name)

	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.PodSandboxId != "" {
			f.Add("label", labelSandboxID+"="+filter.PodSandboxId)
		}
		if filter.State != nil {
			switch filter.State.State {
			case runtimeapi.ContainerState_CONTAINER_CREATED:
				f.Add("status", "created")
			case runtimeapi.ContainerState_CONTAINER_RUNNING:
				f.Add("status", "running")
			case runtimeapi.ContainerState_CONTAINER_EXITED:
				f.Add("status", "exited")
			}
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := b.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		return nil, err
	}

	var result []*runtimeapi.Container
	for _, c := range containers {
		createdAt := time.Unix(c.Created, 0)
		state := dockerStateToContainerState(c.State)
		attempt, _ := strconv.ParseUint(c.Labels[labelContainerAttempt], 10, 32)
		result = append(result, &runtimeapi.Container{
			Id:           c.ID,
			PodSandboxId: c.Labels[labelSandboxID],
			Metadata: &runtimeapi.ContainerMetadata{
				Name:    c.Labels[labelContainerName],
				Attempt: uint32(attempt),
			},
			Image: &runtimeapi.ImageSpec{
				Image: c.Image,
			},
			ImageRef:    c.ImageID,
			State:       state,
			CreatedAt:   createdAt.UnixNano(),
			Labels:      extractLabels(c.Labels),
			Annotations: extractAnnotations(c.Labels),
		})
	}
	if filter != nil && filter.PodSandboxId != "" {
		log.Debug().Int("count", len(result)).Str("sandbox", filter.PodSandboxId[:12]).Msg("CRI ListContainers")
	} else {
		log.Debug().Int("count", len(result)).Msg("CRI ListContainers")
	}
	return result, nil
}

func (b *Backend) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	inspect, err := b.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	state := dockerStateToContainerState(inspect.State.Status)
	log.Debug().Str("id", containerID[:12]).Str("dockerState", inspect.State.Status).Int32("criState", int32(state)).Msg("CRI ContainerStatus")

	attempt, _ := strconv.ParseUint(inspect.Config.Labels[labelContainerAttempt], 10, 32)
	status := &runtimeapi.ContainerStatus{
		Id: containerID,
		Metadata: &runtimeapi.ContainerMetadata{
			Name:    inspect.Config.Labels[labelContainerName],
			Attempt: uint32(attempt),
		},
		State:     state,
		CreatedAt: createdAt.UnixNano(),
		Image: &runtimeapi.ImageSpec{
			Image: inspect.Config.Image,
		},
		ImageRef:    inspect.Image,
		Labels:      extractLabels(inspect.Config.Labels),
		Annotations: extractAnnotations(inspect.Config.Labels),
	}
	// Use CRI log path if available, else Docker's internal log path
	if criLogPath := inspect.Config.Labels[labelLogPath]; criLogPath != "" {
		status.LogPath = criLogPath
	} else {
		status.LogPath = inspect.LogPath
	}

	if inspect.State.StartedAt != "" && inspect.State.StartedAt != "0001-01-01T00:00:00Z" {
		startedAt, _ := time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
		status.StartedAt = startedAt.UnixNano()
	}
	if inspect.State.FinishedAt != "" && inspect.State.FinishedAt != "0001-01-01T00:00:00Z" {
		finishedAt, _ := time.Parse(time.RFC3339Nano, inspect.State.FinishedAt)
		status.FinishedAt = finishedAt.UnixNano()
	}
	if state == runtimeapi.ContainerState_CONTAINER_EXITED {
		status.ExitCode = int32(inspect.State.ExitCode)
		status.Reason = inspect.State.Error
	}

	// Mounts
	for _, m := range inspect.Mounts {
		status.Mounts = append(status.Mounts, &runtimeapi.Mount{
			ContainerPath: m.Destination,
			HostPath:      m.Source,
			Readonly:      !m.RW,
		})
	}

	resp := &runtimeapi.ContainerStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (b *Backend) UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error {
	updateConfig := container.UpdateConfig{}
	if linux := resources.GetLinux(); linux != nil {
		updateConfig.Resources.CPUShares = linux.GetCpuShares()
		updateConfig.Resources.Memory = linux.GetMemoryLimitInBytes()
		updateConfig.Resources.CPUQuota = linux.GetCpuQuota()
		updateConfig.Resources.CPUPeriod = linux.GetCpuPeriod()
	}
	_, err := b.client.ContainerUpdate(ctx, containerID, updateConfig)
	return err
}

func (b *Backend) ReopenContainerLog(ctx context.Context, containerID string) error {
	inspect, err := b.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return err
	}
	criLogPath := inspect.Config.Labels[labelLogPath]
	if criLogPath == "" {
		return nil
	}
	// Stop existing log writer and start a fresh one
	b.stopLogWriter(containerID)
	b.startLogWriter(containerID, criLogPath)
	return nil
}

func (b *Backend) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error) {
	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}

	exec, err := b.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return nil, nil, err
	}

	resp, err := b.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()

	var stdout, stderr bytes.Buffer

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Docker multiplexes stdout/stderr; use stdcopy to demux
	done := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, resp.Reader)
		done <- err
	}()

	select {
	case <-ctx.Done():
		return stdout.Bytes(), stderr.Bytes(), fmt.Errorf("exec timeout")
	case err := <-done:
		if err != nil {
			return nil, nil, err
		}
	}

	// Get exit code
	inspectResp, err := b.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return stdout.Bytes(), stderr.Bytes(), nil
	}
	if inspectResp.ExitCode != 0 {
		return stdout.Bytes(), stderr.Bytes(), fmt.Errorf("exit code: %d", inspectResp.ExitCode)
	}

	return stdout.Bytes(), stderr.Bytes(), nil
}

func (b *Backend) ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	resp, err := b.client.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	inspect, err := b.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	return &runtimeapi.ContainerStats{
		Attributes: &runtimeapi.ContainerAttributes{
			Id: containerID,
			Metadata: &runtimeapi.ContainerMetadata{
				Name: inspect.Config.Labels[labelContainerName],
			},
			Labels:      extractLabels(inspect.Config.Labels),
			Annotations: extractAnnotations(inspect.Config.Labels),
		},
		Cpu: &runtimeapi.CpuUsage{
			Timestamp: createdAt.UnixNano(),
		},
		Memory: &runtimeapi.MemoryUsage{
			Timestamp: createdAt.UnixNano(),
		},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp: createdAt.UnixNano(),
		},
	}, nil
}

func (b *Backend) ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	containers, err := b.ListContainers(ctx, &runtimeapi.ContainerFilter{
		Id:            filter.GetId(),
		PodSandboxId:  filter.GetPodSandboxId(),
		LabelSelector: filter.GetLabelSelector(),
	})
	if err != nil {
		return nil, err
	}
	var stats []*runtimeapi.ContainerStats
	for _, c := range containers {
		s, err := b.ContainerStats(ctx, c.Id)
		if err != nil {
			continue
		}
		stats = append(stats, s)
	}
	return stats, nil
}

func (b *Backend) PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	return &runtimeapi.PodSandboxStats{
		Attributes: &runtimeapi.PodSandboxAttributes{
			Id: podSandboxID,
		},
	}, nil
}

func (b *Backend) ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	sandboxes, err := b.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{
		Id: filter.GetId(),
	})
	if err != nil {
		return nil, err
	}
	var stats []*runtimeapi.PodSandboxStats
	for _, sb := range sandboxes {
		s, _ := b.PodSandboxStats(ctx, sb.Id)
		stats = append(stats, s)
	}
	return stats, nil
}
