package docker

import (
	"context"
	"errors"
	"fmt"

	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
	"github.com/rs/zerolog/log"
)

// CreateVolume creates a Docker named volume.
// Returns the prefixed volume name that can be used for subsequent operations.
func (b *Backend) CreateVolume(ctx context.Context, name string) (string, error) {
	resp, err := b.client.VolumeCreate(ctx, volume.CreateOptions{
		Name:   b.name + "-" + name,
		Labels: map[string]string{labelManagedBy: b.name},
	})
	if err != nil {
		return "", err
	}
	log.Debug().Str("name", resp.Name).Msg("created volume")
	return resp.Name, nil
}

// RemoveVolume force-removes a Docker named volume.
func (b *Backend) RemoveVolume(ctx context.Context, name string) error {
	err := b.client.VolumeRemove(ctx, name, true)
	if err != nil {
		return err
	}
	log.Debug().Str("name", name).Msg("removed volume")
	return nil
}

// RemoveVolumes removes all volumes managed by this backend.
// Returns the names of successfully removed volumes and any collected errors.
func (b *Backend) RemoveVolumes(ctx context.Context) ([]string, error) {
	volumes, err := b.ListVolumes(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list managed volumes: %w", err)
	}

	var removed []string
	var errs []error
	for _, v := range volumes {
		if err := b.RemoveVolume(ctx, v); err != nil {
			errs = append(errs, fmt.Errorf("remove volume %s: %w", v, err))
		} else {
			removed = append(removed, v)
			log.Info().Str("name", v).Msg("cleanup: removed volume")
		}
	}
	return removed, errors.Join(errs...)
}

// ListVolumes returns names of Docker volumes matching the label filter.
func (b *Backend) ListVolumes(ctx context.Context, labelFilter map[string]string) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", labelManagedBy+"="+b.name)
	for k, v := range labelFilter {
		f.Add("label", k+"="+v)
	}

	resp, err := b.client.VolumeList(ctx, volume.ListOptions{Filters: f})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(resp.Volumes))
	for _, v := range resp.Volumes {
		names = append(names, v.Name)
	}
	return names, nil
}
