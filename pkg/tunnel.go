package pkg

import (
	"fmt"
	"net"
	"os"
	"sync"
)

type Tunnel interface {
	Port() int
	Hostname() string
}

type TunnelImpl struct {
	config Config

	port     int
	portOnce sync.Once

	hostname     string
	hostnameCh   chan struct{}
	hostnameOnce sync.Once
}

var _ Tunnel = &TunnelImpl{}

func NewTunnel(config Config) Tunnel {
	return &TunnelImpl{
		config:     config,
		hostnameCh: make(chan struct{}),
	}
}

func (t *TunnelImpl) Port() int {
	t.portOnce.Do(func() {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to acquire tunnel port: %w", err)))
			return
		}
		defer listener.Close()
		t.port = listener.Addr().(*net.TCPAddr).Port
	})
	return t.port
}

func (t *TunnelImpl) Hostname() string {
	t.hostnameOnce.Do(func() {
		// TODO Set up a tunnel provider
		hostname, err := os.Hostname()
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to get hostname for tunnel: %w", err)))
			return
		}
		t.hostname = hostname
		close(t.hostnameCh)
	})
	<-t.hostnameCh
	return t.hostname
}
