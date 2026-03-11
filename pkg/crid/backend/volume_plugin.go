package backend

import (
	"fmt"
	"path/filepath"

	"github.com/cnuss/nanokube/pkg/component"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubelettypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

func NewVolumePlugin(backend *BackendImpl) volume.VolumePlugin {
	return &VolumeImpl{
		backend: backend,
		log:     component.NewLogger("volume-plugin"),
	}
}

type VolumeImpl struct {
	backend *BackendImpl
	log     component.Logger
	host    volume.VolumeHost
}

// Init implements [volume.VolumePlugin].
func (v *VolumeImpl) Init(host volume.VolumeHost) error {
	v.host = host
	if client := host.GetKubeClient(); client != nil {
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(v.backend.Name()),
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: v.GetPluginName(),
		}
		if _, err := client.StorageV1().StorageClasses().Create(v.backend.ctx, sc, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
			v.log.Warn().Err(err).Msg("failed to create default StorageClass")
		}
	}
	return nil
}

// GetPluginName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetPluginName() string {
	return v.backend.name + "/" + string(v.backend.Name())
}

// GetVolumeName implements [volume.VolumePlugin].
func (v *VolumeImpl) GetVolumeName(spec *volume.Spec) (string, error) {
	if spec == nil {
		return "", fmt.Errorf("nil volume spec")
	}
	return spec.Name(), nil
}

// CanSupport implements [volume.VolumePlugin].
func (v *VolumeImpl) CanSupport(spec *volume.Spec) bool {
	return spec != nil && spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil
}

// RequiresRemount implements [volume.VolumePlugin].
func (v *VolumeImpl) RequiresRemount(spec *volume.Spec) bool {
	return false
}

// NewMounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	if spec == nil {
		return nil, fmt.Errorf("nil volume spec")
	}
	sandboxID := v.lookupSandboxID(pod)
	return &volumeMounter{
		backend:    v.backend,
		volumeName: spec.Name(),
		sandboxID:  sandboxID,
		readOnly:   spec.ReadOnly,
		log:        v.log,
	}, nil
}

func (v *VolumeImpl) lookupSandboxID(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}
	rt := v.backend.Containers()
	if rt == nil {
		return ""
	}
	sandboxes, err := rt.ListPodSandbox(v.backend.ctx, &runtimeapi.PodSandboxFilter{
		LabelSelector: map[string]string{
			kubelettypes.KubernetesPodUIDLabel: string(pod.UID),
		},
	})
	if err != nil || len(sandboxes) == 0 {
		return ""
	}
	return sandboxes[0].Id
}

// NewUnmounter implements [volume.VolumePlugin].
func (v *VolumeImpl) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return &volumeMounter{
		backend:    v.backend,
		volumeName: name,
		log:        v.log,
	}, nil
}

// ConstructVolumeSpec implements [volume.VolumePlugin].
func (v *VolumeImpl) ConstructVolumeSpec(volumeName, volumePath string) (volume.ReconstructedVolume, error) {
	pv := &v1.PersistentVolume{}
	pv.Name = volumeName
	pv.Spec.PersistentVolumeSource.Local = &v1.LocalVolumeSource{Path: volumePath}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromPersistentVolume(pv, false),
	}, nil
}

// SupportsMountOption implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsMountOption() bool {
	return false
}

// SupportsSELinuxContextMount implements [volume.VolumePlugin].
func (v *VolumeImpl) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// NewProvisioner implements [volume.ProvisionableVolumePlugin].
func (v *VolumeImpl) NewProvisioner(_ klog.Logger, options volume.VolumeOptions) (volume.Provisioner, error) {
	return &volumeProvisioner{
		backend: v.backend,
		options: options,
		log:     v.log,
	}, nil
}

// NewDeleter implements [volume.DeletableVolumePlugin].
func (v *VolumeImpl) NewDeleter(_ klog.Logger, spec *volume.Spec) (volume.Deleter, error) {
	return &volumeMounter{
		backend:    v.backend,
		volumeName: spec.Name(),
		log:        v.log,
	}, nil
}

// volumeMounter implements volume.Mounter, volume.Unmounter, and volume.Deleter.
type volumeMounter struct {
	backend    *BackendImpl
	volumeName string
	sandboxID  string
	path       string
	readOnly   bool
	log        component.Logger
}

func (m *volumeMounter) GetPath() string                      { return m.path }
func (m *volumeMounter) GetMetrics() (*volume.Metrics, error) { return &volume.Metrics{}, nil }

func (m *volumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{ReadOnly: m.readOnly, Managed: true}
}

func (m *volumeMounter) SetUp(args volume.MounterArgs) error {
	return m.SetUpAt(m.path, args)
}

func (m *volumeMounter) SetUpAt(_ string, _ volume.MounterArgs) error {
	mountpoint, err := m.backend.Driver.CreateVolume(m.volumeName, m.sandboxID)
	if err != nil {
		return err
	}
	m.path = mountpoint
	return nil
}

func (m *volumeMounter) TearDown() error {
	return m.TearDownAt(m.path)
}

func (m *volumeMounter) TearDownAt(_ string) error {
	return m.backend.Driver.RemoveVolume(m.volumeName)
}

// Delete implements [volume.Deleter].
func (m *volumeMounter) Delete() error {
	return m.TearDownAt(m.path)
}

// volumeProvisioner implements volume.Provisioner.
type volumeProvisioner struct {
	backend *BackendImpl
	options volume.VolumeOptions
	log     component.Logger
}

func (p *volumeProvisioner) Provision(_ *v1.Node, _ []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	name := p.options.PVName
	pv := &v1.PersistentVolume{}
	pv.Name = name
	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceStorage: p.options.PVC.Spec.Resources.Requests[v1.ResourceStorage],
	}
	pv.Spec.AccessModes = p.options.PVC.Spec.AccessModes
	pv.Spec.PersistentVolumeReclaimPolicy = p.options.PersistentVolumeReclaimPolicy
	pv.Spec.PersistentVolumeSource.Local = &v1.LocalVolumeSource{
		Path: filepath.Join(p.backend.DataDir(), "volumes", name),
	}
	return pv, nil
}

var _ volume.VolumePlugin = &VolumeImpl{}
