package backend

import (
	"context"
	"fmt"

	"github.com/cnuss/nanokube/pkg/component"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	provisioner "sigs.k8s.io/sig-storage-lib-external-provisioner/v10/controller"
)

var _ provisioner.Provisioner = &csiProvisioner{}

type csiProvisioner struct {
	log        component.Logger
	driverName string
	controller csipb.ControllerServer
}

func (p *csiProvisioner) Provision(ctx context.Context, opts provisioner.ProvisionOptions) (*corev1.PersistentVolume, provisioner.ProvisioningState, error) {
	capacity := opts.PVC.Spec.Resources.Requests[corev1.ResourceStorage]

	p.log.Info().
		Str("pvc", opts.PVC.Name).
		Str("namespace", opts.PVC.Namespace).
		Str("pv", opts.PVName).
		Msg("provisioning volume")

	resp, err := p.controller.CreateVolume(ctx, &csipb.CreateVolumeRequest{
		Name: opts.PVName,
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: capacity.Value(),
		},
	})
	if err != nil {
		return nil, provisioner.ProvisioningFinished, fmt.Errorf("CreateVolume: %w", err)
	}

	vol := resp.GetVolume()

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: opts.PVName,
		},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%d", capacity.Value())),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:           p.driverName,
					VolumeHandle:     vol.GetVolumeId(),
					VolumeAttributes: vol.GetVolumeContext(),
				},
			},
			AccessModes:      opts.PVC.Spec.AccessModes,
			StorageClassName: p.driverName,
		},
	}

	return pv, provisioner.ProvisioningFinished, nil
}

func (p *csiProvisioner) Delete(ctx context.Context, pv *corev1.PersistentVolume) error {
	csiSource := pv.Spec.CSI
	if csiSource == nil {
		return fmt.Errorf("PV %s has no CSI source", pv.Name)
	}

	p.log.Info().Str("pv", pv.Name).Str("volume", csiSource.VolumeHandle).Msg("deleting volume")

	_, err := p.controller.DeleteVolume(ctx, &csipb.DeleteVolumeRequest{
		VolumeId: csiSource.VolumeHandle,
	})
	return err
}

// StartProvisioner reconciles the CSIDriver and StorageClass objects, then
// creates and runs a ProvisionController for this backend.
// It blocks until ctx is cancelled.
func (b *BackendImpl) StartProvisioner(ctx context.Context, client clientset.Interface, isDefault bool) {
	csi := b.CSI()
	if csi == nil {
		return
	}

	log := component.NewLogger("provisioner")

	fm := metav1.ApplyOptions{FieldManager: string(b.Name()), Force: true}

	if drv := csi.CSIDriver(); drv != nil {
		log.Info().Str("driver", *drv.GetName()).Msg("reconciling CSIDriver")
		if _, err := client.StorageV1().CSIDrivers().Apply(ctx, drv, fm); err != nil {
			log.Warn().Err(err).Str("driver", *drv.GetName()).Msg("failed to apply CSIDriver")
		}
	}

	if sc := b.StorageClass(); sc != nil {
		log.Info().Str("storageclass", *sc.GetName()).Msg("reconciling StorageClass")
		sc.WithAnnotations(map[string]string{
			"storageclass.kubernetes.io/is-default-class": fmt.Sprintf("%t", isDefault),
		})
		if _, err := client.StorageV1().StorageClasses().Apply(ctx, sc, fm); err != nil {
			log.Warn().Err(err).Str("storageclass", *sc.GetName()).Msg("failed to apply StorageClass")
		}
	}

	p := &csiProvisioner{
		log:        log,
		driverName: csi.DriverName(),
		controller: b.Driver.VolumeServer(),
	}

	ctrl := provisioner.NewProvisionController(
		klog.Background(),
		client,
		csi.DriverName(),
		p,
	)

	log.Info().Str("driver", csi.DriverName()).Msg("provisioner started")
	ctrl.Run(ctx)
}
