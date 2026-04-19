package nanokube

import (
	"context"
	"io"
)

// LogSource opens the runtime's raw stdout/stderr streams for a container.
// close is called to release the underlying resource (e.g. Docker ContainerLogs reader).
type LogSource func(ctx context.Context) (stdout io.Reader, stderr io.Reader, close func(), err error)
