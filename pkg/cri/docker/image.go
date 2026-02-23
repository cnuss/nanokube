package docker

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/image"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	opts := image.ListOptions{}

	images, err := b.client.ImageList(ctx, opts)
	if err != nil {
		return nil, err
	}

	var result []*runtimeapi.Image
	for _, img := range images {
		result = append(result, dockerImageToRuntimeImage(img))
	}
	return result, nil
}

func (b *Backend) ImageStatus(ctx context.Context, imageSpec *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	ref := imageSpec.GetImage()
	if ref == "" {
		return &runtimeapi.ImageStatusResponse{}, nil
	}

	inspect, err := b.client.ImageInspect(ctx, ref)
	if err != nil {
		// Image not found is not an error in CRI
		return &runtimeapi.ImageStatusResponse{}, nil
	}

	// Filter RepoTags: exclude Docker placeholder "<none>:<none>" and digest refs
	var repoTags []string
	for _, t := range inspect.RepoTags {
		if t != "<none>:<none>" && !strings.Contains(t, "@") {
			repoTags = append(repoTags, t)
		}
	}
	img := &runtimeapi.Image{
		Id:          inspect.ID,
		RepoTags:    repoTags,
		RepoDigests: inspect.RepoDigests,
		Size:        uint64(inspect.Size),
	}

	// Extract UID from image config User field
	if inspect.Config != nil && inspect.Config.User != "" {
		// User can be "uid", "uid:gid", "username", "username:groupname"
		userPart, _, _ := strings.Cut(inspect.Config.User, ":")
		if uid, err := strconv.ParseInt(userPart, 10, 64); err == nil {
			// Numeric user: set Uid only
			img.Uid = &runtimeapi.Int64Value{Value: uid}
		} else {
			// Non-numeric user: set Username (without group)
			img.Username = userPart
		}
	}

	resp := &runtimeapi.ImageStatusResponse{Image: img}
	if verbose {
		resp.Info = map[string]string{
			"architecture": inspect.Architecture,
			"os":           inspect.Os,
		}
	}
	return resp, nil
}

func (b *Backend) PullImage(ctx context.Context, imageSpec *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, sandbox *runtimeapi.PodSandboxConfig) (string, error) {
	ref := imageSpec.GetImage()

	pullOpts := image.PullOptions{}

	reader, err := b.client.ImagePull(ctx, ref, pullOpts)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	// Drain the pull output
	decoder := json.NewDecoder(reader)
	for {
		var msg map[string]any
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
	}

	// Get the image ID after pull
	inspect, err := b.client.ImageInspect(ctx, ref)
	if err != nil {
		return "", err
	}
	return inspect.ID, nil
}

func (b *Backend) RemoveImage(ctx context.Context, imageSpec *runtimeapi.ImageSpec) error {
	_, err := b.client.ImageRemove(ctx, imageSpec.GetImage(), image.RemoveOptions{Force: true, PruneChildren: true})
	if err != nil && isNotFoundOrNotRunning(err) {
		return nil
	}
	return err
}

func (b *Backend) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	usage, err := b.client.DiskUsage(ctx, dockertypes.DiskUsageOptions{})
	if err != nil {
		return &runtimeapi.ImageFsInfoResponse{}, nil
	}

	var totalSize int64
	for _, img := range usage.Images {
		totalSize += img.Size
	}

	return &runtimeapi.ImageFsInfoResponse{
		ImageFilesystems: []*runtimeapi.FilesystemUsage{
			{
				FsId:      &runtimeapi.FilesystemIdentifier{Mountpoint: "/var/lib/docker"},
				UsedBytes: &runtimeapi.UInt64Value{Value: uint64(totalSize)},
			},
		},
	}, nil
}
