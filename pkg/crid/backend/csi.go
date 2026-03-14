package backend

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/cnuss/nanokube/pkg/component"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	storagev1ac "k8s.io/client-go/applyconfigurations/storage/v1"
	clientset "k8s.io/client-go/kubernetes"
	versionutil "k8s.io/component-base/version"
	pluginreg "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	provisioner "sigs.k8s.io/sig-storage-lib-external-provisioner/v10/controller"
)

func NewCSI(backend *BackendImpl) CSI {
	return &CSIImpl{
		ctx:        backend.ctx,
		backend:    backend,
		driverName: string(backend.Name()) + ".csi",
		log:        component.NewLogger("csi"),
	}
}

type CSI interface {
	csipb.IdentityServer
	csipb.NodeServer
	pluginreg.RegistrationServer
	provisioner.Provisioner

	Start(ctx context.Context, pluginsDir string, registrationDir string) error
	StartProvisioner(ctx context.Context, client clientset.Interface, isDefault bool)
	Stop()
}

type CSIImpl struct {
	pluginreg.UnimplementedRegistrationServer

	ctx         context.Context
	backend     *BackendImpl
	driverName  string
	log         component.Logger
	csiEndpoint string
	regEndpoint string
	csiSrv      *grpc.Server
	regSrv      *grpc.Server
	provisioner *provisioner.ProvisionController
}

// Start creates the CSI endpoint socket immediately and defers the
// registration socket until the Node is ready (signalled via nodeReady).
func (c *CSIImpl) Start(ctx context.Context, pluginsDir, registrationDir string) error {
	csiPluginDir := filepath.Join(pluginsDir, c.driverName)
	os.MkdirAll(csiPluginDir, 0o755)

	// Remove stale sockets from previous runs
	c.csiEndpoint = filepath.Join(csiPluginDir, c.driverName+"-csi.sock")
	c.regEndpoint = filepath.Join(registrationDir, c.driverName+"-reg.sock")
	os.Remove(c.csiEndpoint)
	os.Remove(c.regEndpoint)

	csiLis, err := net.Listen("unix", c.csiEndpoint)
	if err != nil {
		return component.WrapErr(c.log, err)
	}
	c.csiSrv = grpc.NewServer()
	csipb.RegisterIdentityServer(c.csiSrv, c)
	csipb.RegisterControllerServer(c.csiSrv, c.backend.VolumeServer())
	csipb.RegisterNodeServer(c.csiSrv, c)
	go func() {
		c.log.Info().Str("socket", c.csiEndpoint).Msg("CSI endpoint serving")
		if err := c.csiSrv.Serve(csiLis); err != nil {
			os.Remove(c.csiEndpoint)
			c.log.Error().Err(err).Msg("CSI endpoint exited")
		}
	}()

	// Registration socket — deferred until the kubelet has initialized
	// the CSI volume plugin with the correct nodeName and the Node object
	// exists (triggered by NodeHasSufficientPID/Memory/DiskPressure events).
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-c.backend.nodeReady:
		}

		c.log.Info().Msg("node ready, creating registration socket")
		os.Remove(c.regEndpoint)
		regLis, err := net.Listen("unix", c.regEndpoint)
		if err != nil {
			c.log.Error().Err(err).Msg("CSI registration listen failed")
			return
		}
		c.regSrv = grpc.NewServer()
		pluginreg.RegisterRegistrationServer(c.regSrv, c)
		c.log.Info().Str("socket", c.regEndpoint).Msg("CSI registration serving")
		if err := c.regSrv.Serve(regLis); err != nil {
			os.Remove(c.regEndpoint)
			c.log.Error().Err(err).Msg("CSI registration exited")
		}
	}()

	return nil
}

// StartProvisioner reconciles the CSIDriver and StorageClass objects, then
// creates and runs a ProvisionController for this backend.
// It blocks until ctx is cancelled.
func (c *CSIImpl) StartProvisioner(ctx context.Context, client clientset.Interface, isDefault bool) {
	fm := metav1.ApplyOptions{FieldManager: string(c.backend.Name()), Force: true}

	drv := c.CSIDriver()
	c.log.Info().Str("driver", *drv.GetName()).Msg("reconciling CSIDriver")
	if _, err := client.StorageV1().CSIDrivers().Apply(ctx, &drv, fm); err != nil {
		c.log.Warn().Err(err).Str("driver", *drv.GetName()).Msg("failed to apply CSIDriver")
	}

	sc := c.StorageClass()
	c.log.Info().Str("storageclass", *sc.GetName()).Msg("reconciling StorageClass")
	sc.WithAnnotations(map[string]string{
		"storageclass.kubernetes.io/is-default-class": fmt.Sprintf("%t", isDefault),
	})
	if _, err := client.StorageV1().StorageClasses().Apply(ctx, &sc, fm); err != nil {
		c.log.Warn().Err(err).Str("storageclass", *sc.GetName()).Msg("failed to apply StorageClass")
	}

	c.provisioner = provisioner.NewProvisionController(
		c.backend.klog,
		client,
		c.DriverName(),
		c,
	)

	c.log.Info().Str("driver", c.DriverName()).Msg("provisioner started")
	c.provisioner.Run(ctx)
}

// Stop gracefully shuts down both gRPC servers.
func (c *CSIImpl) Stop() {
	if c.csiSrv != nil {
		os.Remove(c.csiEndpoint)
		c.csiSrv.GracefulStop()
	}
	if c.regSrv != nil {
		os.Remove(c.regEndpoint)
		c.regSrv.GracefulStop()
	}
}

func (c *CSIImpl) DriverName() string {
	return c.driverName
}

// --- pluginregistration.Registration ---

func (c *CSIImpl) GetInfo(_ context.Context, _ *pluginreg.InfoRequest) (*pluginreg.PluginInfo, error) {
	c.log.Info().Msg("GetInfo called by plugin watcher")
	return &pluginreg.PluginInfo{
		Type:              pluginreg.CSIPlugin,
		Name:              c.driverName,
		Endpoint:          c.csiEndpoint,
		SupportedVersions: []string{"1.0.0"},
	}, nil
}

func (c *CSIImpl) NotifyRegistrationStatus(_ context.Context, status *pluginreg.RegistrationStatus) (*pluginreg.RegistrationStatusResponse, error) {
	if status.PluginRegistered {
		c.log.Info().Msg("CSI driver registered with kubelet")
	} else {
		c.log.Error().Str("error", status.Error).Msg("CSI driver registration failed")
	}
	return &pluginreg.RegistrationStatusResponse{}, nil
}

// --- csipb.IdentityServer ---

func (c *CSIImpl) GetPluginInfo(_ context.Context, _ *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          c.driverName,
		VendorVersion: versionutil.Get().GitVersion,
	}, nil
}

func (c *CSIImpl) GetPluginCapabilities(_ context.Context, _ *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	return &csipb.GetPluginCapabilitiesResponse{
		Capabilities: []*csipb.PluginCapability{
			{
				Type: &csipb.PluginCapability_Service_{
					Service: &csipb.PluginCapability_Service{
						Type: csipb.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (c *CSIImpl) Probe(_ context.Context, _ *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return nil, component.WrapErr(c.log, fmt.Errorf("not implemented"))
}

// --- csipb.NodeServer ---

func (c *CSIImpl) NodeGetInfo(_ context.Context, _ *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	hostInfo, err := c.backend.HostInfo()
	if err != nil {
		return nil, component.WrapErr(c.log, err)
	}
	return &csipb.NodeGetInfoResponse{
		NodeId: hostInfo.Hostname,
	}, nil
}

func (c *CSIImpl) NodeGetCapabilities(_ context.Context, _ *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{
		Capabilities: []*csipb.NodeServiceCapability{},
	}, nil
}

func (c *CSIImpl) NodePublishVolume(_ context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	mountpoint := req.GetVolumeContext()["mountpoint"]
	c.log.Info().Str("volume", volumeID).Str("target", targetPath).Str("mountpoint", mountpoint).Msg("NodePublishVolume")

	os.MkdirAll(filepath.Dir(targetPath), 0o755)
	os.Remove(targetPath)
	if err := os.Symlink(mountpoint, targetPath); err != nil {
		return nil, component.WrapErr(c.log, fmt.Errorf("symlink %s -> %s: %w", targetPath, mountpoint, err))
	}

	return &csipb.NodePublishVolumeResponse{}, nil
}

func (c *CSIImpl) NodeUnpublishVolume(_ context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	c.log.Info().Str("target", targetPath).Msg("NodeUnpublishVolume")
	os.Remove(targetPath)
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (c *CSIImpl) CSIDriver() storagev1ac.CSIDriverApplyConfiguration {
	return *storagev1ac.CSIDriver(c.driverName).
		WithSpec(storagev1ac.CSIDriverSpec().
			WithAttachRequired(false).
			WithPodInfoOnMount(true).
			WithVolumeLifecycleModes(storagev1.VolumeLifecyclePersistent))
}

func (c *CSIImpl) StorageClass() storagev1ac.StorageClassApplyConfiguration {
	sc := storagev1ac.StorageClass(c.driverName).
		WithProvisioner(c.driverName).
		WithReclaimPolicy(corev1.PersistentVolumeReclaimRetain).
		WithVolumeBindingMode(storagev1.VolumeBindingImmediate)

	caps, err := c.backend.VolumeServer().ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	if err != nil {
		return *sc
	}
	for _, cap := range caps.GetCapabilities() {
		if rpc := cap.GetRpc(); rpc != nil && rpc.GetType() == csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME {
			sc.WithAllowVolumeExpansion(true)
		}
	}
	return *sc
}

// --- csipb.NodeServer ---

// NodeExpandVolume implements [csi.NodeServer].
func (c *CSIImpl) NodeExpandVolume(context.Context, *csipb.NodeExpandVolumeRequest) (*csipb.NodeExpandVolumeResponse, error) {
	return nil, component.WrapErr(c.log, fmt.Errorf("not implemented"))
}

// NodeGetVolumeStats implements [csi.NodeServer].
func (c *CSIImpl) NodeGetVolumeStats(context.Context, *csipb.NodeGetVolumeStatsRequest) (*csipb.NodeGetVolumeStatsResponse, error) {
	return nil, component.WrapErr(c.log, fmt.Errorf("not implemented"))
}

// NodeStageVolume implements [csi.NodeServer].
func (c *CSIImpl) NodeStageVolume(context.Context, *csipb.NodeStageVolumeRequest) (*csipb.NodeStageVolumeResponse, error) {
	return nil, component.WrapErr(c.log, fmt.Errorf("not implemented"))
}

// NodeUnstageVolume implements [csi.NodeServer].
func (c *CSIImpl) NodeUnstageVolume(context.Context, *csipb.NodeUnstageVolumeRequest) (*csipb.NodeUnstageVolumeResponse, error) {
	return nil, component.WrapErr(c.log, fmt.Errorf("not implemented"))
}

// --- provisioner.Provisioner ---

// Provision implements [controller.Provisioner].
func (c *CSIImpl) Provision(ctx context.Context, opts provisioner.ProvisionOptions) (*corev1.PersistentVolume, provisioner.ProvisioningState, error) {
	capacity := opts.PVC.Spec.Resources.Requests[corev1.ResourceStorage]

	c.log.Info().
		Str("pvc", opts.PVC.Name).
		Str("namespace", opts.PVC.Namespace).
		Str("pv", opts.PVName).
		Msg("provisioning volume")

	resp, err := c.backend.VolumeServer().CreateVolume(ctx, &csipb.CreateVolumeRequest{
		Name: opts.PVName,
		CapacityRange: &csipb.CapacityRange{
			RequiredBytes: capacity.Value(),
		},
	})
	if err != nil {
		return nil, provisioner.ProvisioningFinished, component.WrapErr(c.log, err)
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
					Driver:           c.driverName,
					VolumeHandle:     vol.GetVolumeId(),
					VolumeAttributes: vol.GetVolumeContext(),
				},
			},
			AccessModes:      opts.PVC.Spec.AccessModes,
			StorageClassName: c.driverName,
		},
	}

	return pv, provisioner.ProvisioningFinished, nil
}

// Delete implements [controller.Provisioner].
func (c *CSIImpl) Delete(ctx context.Context, pv *corev1.PersistentVolume) error {
	csiSource := pv.Spec.CSI
	if csiSource == nil {
		return component.WrapErr(c.log, fmt.Errorf("PV %s has no CSI source", pv.Name))
	}

	c.log.Info().Str("pv", pv.Name).Str("volume", csiSource.VolumeHandle).Msg("deleting volume")

	_, err := c.backend.VolumeServer().DeleteVolume(ctx, &csipb.DeleteVolumeRequest{
		VolumeId: csiSource.VolumeHandle,
	})
	return err
}

var (
	_ csipb.IdentityServer         = &CSIImpl{}
	_ csipb.NodeServer             = &CSIImpl{}
	_ pluginreg.RegistrationServer = &CSIImpl{}
	_ provisioner.Provisioner      = &CSIImpl{}
)
