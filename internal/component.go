package internal

import (
	"context"
)

type Component interface {
	Start(ctx context.Context) error
}
