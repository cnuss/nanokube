package backend

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

func NewVolumes(backend *BackendImpl) volume.VolumePlugin {
	volumes := &VolumeImpl{backend: backend}
	// TODO: start
	return volumes
}

type VolumeImpl struct {
	backend *BackendImpl
}

// CanSupport implements [volume.VolumePlugin].
func (v *VolumeImpl) CanSupport(spec *volume.Spec) bool {
	panic("unimplemented")
}

// ConstructVolumeSpec implements [volume.VolumePlugin].
func (v *VolumeImpl) ConstructVolumeSpec(volumeName string, volumePath string) (volume.ReconstructedVolume, error) {
	panic("unimplemented")
}

// GetPluginName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetPluginName() string {
	panic("unimplemented")
}

// GetVolumeName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetVolumeName(spec *volume.Spec) (string, error) {
	panic("unimplemented")
}

// Init implements [volume.VolumePlugin].
func (v *VolumeImpl) Init(host volume.VolumeHost) error {
	panic("unimplemented")
}

// NewMounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	panic("unimplemented")
}

// NewUnmounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	panic("unimplemented")
}

// RequiresRemount implements [volume.VolumePlugin].
func (v *VolumeImpl) RequiresRemount(spec *volume.Spec) bool {
	panic("unimplemented")
}

// SupportsMountOption implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsMountOption() bool {
	panic("unimplemented")
}

// SupportsSELinuxContextMount implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	panic("unimplemented")
}

var _ volume.VolumePlugin = &VolumeImpl{}
