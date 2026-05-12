package nanokube

import (
	"context"
	"sync"
)

// Await returns a channel that closes when either ctx is done or any of sigs
// closes, whichever fires first. Useful for "wait for one of these signals,
// but respect ctx" without scattering selects across call sites.
func Await(ctx context.Context, sigs ...<-chan struct{}) <-chan struct{} {
	out := make(chan struct{})
	var once sync.Once
	closeOut := func() { once.Do(func() { close(out) }) }

	go func() { <-ctx.Done(); closeOut() }()
	for _, sig := range sigs {
		go func(s <-chan struct{}) { <-s; closeOut() }(sig)
	}
	return out
}
