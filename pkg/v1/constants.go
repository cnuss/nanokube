package v1

import "path/filepath"

var (
	HTTP2 = true

	DataDirLock       DataDir  = "lock"
	DataDirKubelet    DataDir  = "kubelet"
	DataDirCerts      DataDir  = "certs"
	DataDirLogs       DataDir  = "logs"
	DataDirEtcd       DataDir  = "etcd"
	DataDirKube       DataDir  = ".kube"
	DataDirStaticPods DataDir  = DataDir(filepath.Join(string(DataDirKubelet), "static-pods"))
	CertFile          FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.crt"))
	KeyFile           FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.key"))
	KubeconfigFile    FileName = FileName(filepath.Join(string(DataDirKube), "config"))

	NetworkHost       NetworkType = "host"
	NetworkBridge     NetworkType = "bridge"
	NetworkSubnetSize             = 28
)
