package nanokube

type (
	DataDir    string
	FileName   string
	FileNameFn func(options Options) string
	Path       string
)

const (
	DataDirLock    DataDir = "lock"
	DataDirKubelet DataDir = "kubelet"
	DataDirCerts   DataDir = "certs"
	DataDirLogs    DataDir = "logs"
	DataDirEtcd    DataDir = "etcd"

	CertFile FileName = "apiserver.crt"
	KeyFile  FileName = "apiserver.key"
)

var (
	CertFileFn FileNameFn = func(options Options) string {
		return options.Name() + ".crt"
	}
	KeyFileFn FileNameFn = func(options Options) string {
		return options.Name() + ".key"
	}
)

type Options interface {
	Name() string
	Verbosity() int
	Clean() bool
	DataDir() string
	Standalone() bool

	DataDirAt(name DataDir) string
	FilePathAt(dir DataDir, path FileName) string
	FileAt(dir DataDir, name FileNameFn) (string, []byte)
}
