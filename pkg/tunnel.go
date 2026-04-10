package pkg

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
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
	hostnameOnce sync.Once

	client *ssh.Client
}

var _ Tunnel = &TunnelImpl{}

func NewTunnel(config Config) Tunnel {
	return &TunnelImpl{
		config: config,
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
		config := &ssh.ClientConfig{
			User:            "nokey",
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}

		client, err := ssh.Dial("tcp", "localhost.run:22", config)
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to connect to localhost.run: %w", err)))
			return
		}
		t.client = client

		// SSH keepalive (equivalent to ServerAliveInterval=30)
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if _, _, err := client.SendRequest("keepalive@openssh.com", true, nil); err != nil {
						t.config.Cancel(NewFatalError(fmt.Errorf("tunnel ssh connection lost: %w", err)))
						return
					}
				case <-t.config.Context().Done():
					return
				}
			}
		}()

		// Request reverse port forward — localhost.run assigns a public hostname
		listener, err := client.Listen("tcp", "0.0.0.0:443")
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to request tunnel: %w", err)))
			return
		}

		// Open a session to read the banner containing the tunnel URL
		session, err := client.NewSession()
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to open ssh session: %w", err)))
			return
		}

		stdout, err := session.StdoutPipe()
		if err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to pipe ssh session: %w", err)))
			return
		}

		if err := session.Shell(); err != nil {
			t.config.Cancel(NewFatalError(fmt.Errorf("failed to start ssh shell: %w", err)))
			return
		}

		// Parse hostname from banner line: "xxxxx.lhr.life tunneled with tls termination, ..."
		// Match: "xxxxx.lhr.life tunneled with tls termination, ..."
		tunnelRe := regexp.MustCompile(`^(\S+\.\S+)\s+tunneled`)
		hostname := make(chan string, 1)
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				if m := tunnelRe.FindStringSubmatch(scanner.Text()); m != nil {
					hostname <- m[1]
					return
				}
			}
		}()

		// Forward incoming tunnel connections to the local port
		go func() {
			for {
				remote, err := listener.Accept()
				if err != nil {
					t.config.Cancel(NewFatalError(fmt.Errorf("tunnel listener closed: %w", err)))
					return
				}
				go t.forward(remote)
			}
		}()

		select {
		case h := <-hostname:
			t.hostname = h
		case <-t.config.Context().Done():
			t.config.Cancel(NewFatalError(fmt.Errorf("tunnel setup cancelled")))
		}
	})
	return t.hostname
}

func (t *TunnelImpl) forward(remote net.Conn) {
	defer remote.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", t.Port())
	for {
		select {
		case <-t.config.Context().Done():
			return
		default:
		}
		local, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		done := make(chan struct{}, 2)
		go func() { io.Copy(local, remote); done <- struct{}{} }()
		go func() { io.Copy(remote, local); done <- struct{}{} }()
		<-done
		local.Close()
	}
}
