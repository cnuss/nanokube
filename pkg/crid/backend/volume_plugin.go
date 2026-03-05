package backend

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

func NewVolumePlugin(backend *BackendImpl) volume.VolumePlugin {
	volumes := &VolumeImpl{backend: backend}
	// TODO: start
	return volumes
}

type VolumeImpl struct {
	backend *BackendImpl
}

// CanSupport implements [volume.VolumePlugin].
func (v *VolumeImpl) CanSupport(spec *volume.Spec) bool {
	panic("CanSupport: unimplemented")
}

// ConstructVolumeSpec implements [volume.VolumePlugin].
func (v *VolumeImpl) ConstructVolumeSpec(volumeName string, volumePath string) (volume.ReconstructedVolume, error) {
	panic("ConstructVolumeSpec: unimplemented")
}

// GetPluginName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetPluginName() string {
	panic("GetPluginName: unimplemented")
}

// GetVolumeName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetVolumeName(spec *volume.Spec) (string, error) {
	panic("GetVolumeName: unimplemented")
}

// Init implements [volume.VolumePlugin].
func (v *VolumeImpl) Init(host volume.VolumeHost) error {
	panic("Init: unimplemented")
}

// NewMounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewMounter(spec *volume.Spec, podRef *v1.Pod) (volume.Mounter, error) {
	panic("NewMounter: unimplemented")
}

// NewUnmounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	panic("NewUnmounter: unimplemented")
}

// RequiresRemount implements [volume.VolumePlugin].
func (v *VolumeImpl) RequiresRemount(spec *volume.Spec) bool {
	panic("RequiresRemount: unimplemented")
}

// SupportsMountOption implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsMountOption() bool {
	panic("SupportsMountOption: unimplemented")
}

// SupportsSELinuxContextMount implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	panic("SupportsSELinuxContextMount: unimplemented")
}

var _ volume.VolumePlugin = &VolumeImpl{}
