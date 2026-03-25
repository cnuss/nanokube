package podman

import (
	"context"

	"github.com/cnuss/nanokube/pkg/crid/backend"
)

func init() {
	backend.Runtimes[backend.Podman] = Detect
}

func Detect(ctx context.Context, name, dataDir string) backend.Backend {
	return nil
}
