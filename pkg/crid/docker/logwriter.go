package docker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
)

// logWriter manages CRI log writer goroutines for containers.
// It streams Docker container logs and writes them in CRI format:
// "<timestamp> <stream> F <message>\n"
type logWriter struct {
	mu      sync.Mutex
	ctx     context.Context
	client  *dockerclient.Client
	writers map[string]context.CancelFunc
}

func newLogWriter(ctx context.Context, client *dockerclient.Client) *logWriter {
	return &logWriter{
		ctx:     ctx,
		client:  client,
		writers: make(map[string]context.CancelFunc),
	}
}

// Start begins streaming logs for a container to the given path.
func (lw *logWriter) Start(containerID, logPath string) {
	os.MkdirAll(filepath.Dir(logPath), 0o755)
	if f, err := os.Create(logPath); err == nil {
		f.Close()
	}
	ctx, cancel := context.WithCancel(lw.ctx)
	lw.mu.Lock()
	if old, ok := lw.writers[containerID]; ok {
		old()
	}
	lw.writers[containerID] = cancel
	lw.mu.Unlock()
	go func() {
		defer lw.Stop(containerID)
		reader, err := lw.client.ContainerLogs(ctx, containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: true,
		})
		if err != nil {
			return
		}
		defer reader.Close()

		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
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
			line := string(payload)
			ts := time.Now().Format(time.RFC3339Nano)
			msg := line
			if idx := strings.IndexByte(line, ' '); idx > 0 {
				ts = line[:idx]
				msg = line[idx+1:]
			}
			if !strings.HasSuffix(msg, "\n") {
				msg += "\n"
			}
			fmt.Fprintf(f, "%s %s F %s", ts, stream, msg)
			f.Sync()
		}
	}()
}

// Stop cancels and removes the log writer for a container.
func (lw *logWriter) Stop(containerID string) {
	lw.mu.Lock()
	if cancel, ok := lw.writers[containerID]; ok {
		cancel()
		delete(lw.writers, containerID)
	}
	lw.mu.Unlock()
}
