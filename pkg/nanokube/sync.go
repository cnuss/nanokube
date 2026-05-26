package nanokube

import "context"

// Await returns a channel that closes once all of sigs have closed, or
// immediately if ctx is done. Useful for "wait for all of these signals,
// but respect ctx" without scattering selects across call sites.
//
// Callers can't tell from the chan alone whether all sigs fired or ctx was
// canceled — check ctx.Err() after `<-Await(...)` if the distinction
// matters. Uses a single goroutine that exits cleanly on either path.
func Await(ctx context.Context, sigs ...<-chan struct{}) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		for _, sig := range sigs {
			select {
			case <-sig:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
