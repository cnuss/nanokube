package docker

import (
	"context"
	"errors"
	"fmt"

	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
)

// EnsureNetwork creates or reuses a network for this cluster.
// Host and none are built-in Docker networks and require no creation.
// Bridge creates a dedicated cluster network.
func (b *Backend) EnsureNetwork(ctx context.Context, networkType critypes.NetworkType) (string, error) {
	switch networkType {
	case critypes.NetworkHost, critypes.NetworkNone:
		return "", nil
	case critypes.NetworkBridge:
		return b.ensureBridgeNetwork(ctx)
	default:
		return "", fmt.Errorf("unsupported network type: %q", networkType)
	}
}

func (b *Backend) ensureBridgeNetwork(ctx context.Context) (string, error) {
	name := b.name + "-bridge"

	// Check if it already exists
	resp, err := b.client.NetworkInspect(ctx, name, network.InspectOptions{})
	if err == nil {
		b.networkID = resp.ID
		logger.Info().Str("network", name).Str("id", resp.ID[:12]).Msg("reusing existing cluster network")
		return resp.ID, nil
	}

	// Inspect the built-in bridge to copy its configuration
	builtin, err := b.client.NetworkInspect(ctx, "bridge", network.InspectOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect built-in bridge: %w", err)
	}

	createResp, err := b.client.NetworkCreate(ctx, name, network.CreateOptions{
		Driver:     builtin.Driver,
		Scope:      builtin.Scope,
		EnableIPv4: &builtin.EnableIPv4,
		EnableIPv6: &builtin.EnableIPv6,
		Internal:   builtin.Internal,
		Options:    builtin.Options,
		IPAM: &network.IPAM{
			Driver:  builtin.IPAM.Driver,
			Options: builtin.IPAM.Options,
		},
		Labels: map[string]string{labelManagedBy: b.name},
	})
	if err != nil {
		return "", err
	}

	b.networkID = createResp.ID
	logger.Info().Str("network", name).Str("id", createResp.ID[:12]).Msg("created cluster network")
	return createResp.ID, nil
}

// RemoveNetworks removes all networks managed by this backend.
// Returns the IDs of successfully removed networks and any collected errors.
func (b *Backend) RemoveNetworks(ctx context.Context) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", labelManagedBy+"="+b.name)

	networks, err := b.client.NetworkList(ctx, network.ListOptions{Filters: f})
	if err != nil {
		return nil, fmt.Errorf("list managed networks: %w", err)
	}

	var removed []string
	var errs []error
	for _, n := range networks {
		if err := b.client.NetworkRemove(ctx, n.ID); err != nil {
			errs = append(errs, fmt.Errorf("remove network %s: %w", n.ID[:12], err))
		} else {
			removed = append(removed, n.ID)
			logger.Info().Str("id", n.ID[:12]).Str("name", n.Name).Msg("cleanup: removed network")
		}
	}

	b.networkID = ""
	return removed, errors.Join(errs...)
}
