package types

// MountLookup checks whether a host path is a tracked tmpfs mount.
// Used by backends to create native tmpfs mounts instead of bind mounts.
type MountLookup interface {
	GetTmpfs(path string) (opts string, ok bool)
}
