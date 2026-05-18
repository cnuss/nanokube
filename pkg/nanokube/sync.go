package nanokube

import (
	"context"
	"sync"
)

// Await returns a channel that closes once all of sigs have closed, or
// immediately if ctx is done. Useful for "wait for all of these signals,
// but respect ctx" without scattering selects across call sites.
func Await(ctx context.Context, sigs ...<-chan struct{}) <-chan struct{} {
	out := make(chan struct{})
	var once sync.Once
	closeOut := func() { once.Do(func() { close(out) }) }

	go func() { <-ctx.Done(); closeOut() }()
	go func() {
		for _, sig := range sigs {
			select {
			case <-sig:
			case <-ctx.Done():
				return
			}
		}
		closeOut()
	}()
	return out
}
