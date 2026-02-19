package internal

type Runtime int

const (
	RuntimeDocker Runtime = iota
	RuntimePodman
	RuntimeCRIO
)

func (r Runtime) String() string {
	switch r {
	case RuntimeDocker:
		return "docker"
	case RuntimePodman:
		return "podman"
	case RuntimeCRIO:
		return "cri-o"
	default:
		return "unknown"
	}
}
