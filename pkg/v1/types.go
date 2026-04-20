package v1

type (
	BackendName string
	BackendFunc func(config Config) Backend
	DataDir     string
	FileName    string
	Path        string
	NetworkType string
	ServiceName string
)
