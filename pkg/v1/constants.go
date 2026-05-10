package v1

import (
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
)

var (
	HTTP2 = true

	Backends         []BackendFunc = []BackendFunc{}
	DockerBackend    BackendName   = "docker"
	PodmanBackend    BackendName   = "podman"
	AWSLambdaBackend BackendName   = "awslambda"

	APIServerService         ServiceName = "apiserver"
	KubeletService           ServiceName = "kubelet"
	ControllerManagerService ServiceName = "controller-manager"
	SchedulerService         ServiceName = "scheduler"
	Node                     ServiceName = "node"

	DataDirLock       DataDir  = "lock"
	DataDirKubelet    DataDir  = "kubelet"
	DataDirCerts      DataDir  = "certs"
	DataDirLogs       DataDir  = "logs"
	DataDirEtcd       DataDir  = "etcd"
	DataDirKube       DataDir  = ".kube"
	DataDirStaticPods DataDir  = DataDir(filepath.Join(string(DataDirKubelet), "static-pods"))
	PidFile                    = func(options Options) FileName { return FileName(options.Name() + ".pid") }
	LogFile                    = func(options Options) FileName { return FileName(options.Name() + ".log") }
	CAFile            FileName = FileName(filepath.Join(string(DataDirCerts), "ca.crt"))
	CertFile          FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.crt"))
	KeyFile           FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.key"))
	KubeconfigFile    FileName = FileName(filepath.Join(string(DataDirKube), "config"))

	NetworkHost        NetworkType = "host"
	NetworkBridge      NetworkType = "bridge"
	NetworkSubnetSize              = 28
	NetworkProtocolTCP             = corev1.ProtocolTCP
	NetworkProtocolUDP             = corev1.ProtocolUDP

	SandboxExecSentinel = "__sandbox__"
)
