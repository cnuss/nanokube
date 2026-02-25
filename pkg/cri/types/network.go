package types

// NetworkType identifies a network type for pod sandboxes.
type NetworkType string

const (
	// NetworkBridge creates a dedicated bridge network for the cluster.
	NetworkBridge NetworkType = "bridge"

	// NetworkHost uses the host's network namespace directly.
	NetworkHost NetworkType = "host"

	// NetworkNone provides no network connectivity.
	NetworkNone NetworkType = "none"
)
