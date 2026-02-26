package types

// MountLookup checks whether a host path is a tracked tmpfs or volume mount.
// Used by backends to create native tmpfs mounts or Docker named volumes
// instead of bind mounts.
type MountLookup interface {
	GetTmpfs(path string) (opts string, ok bool)
	GetVolume(path string) (volumeName string, ok bool)
}
