package backend

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/cnuss/nanokube/pkg/component"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc"
	versionutil "k8s.io/component-base/version"
	pluginreg "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

func csiDriverVersion() string {
	return versionutil.Get().GitVersion
}

func NewCSI(backend *BackendImpl) *CSI {
	return &CSI{
		backend:    backend,
		driverName: string(backend.Name()) + ".csi",
		log:        component.NewLogger("csi"),
	}
}

type CSI struct {
	csipb.IdentityServer
	csipb.NodeServer
	pluginreg.UnsafeRegistrationServer

	backend    *BackendImpl
	driverName string
	log        component.Logger
	endpoint   string // CSI endpoint socket path
	regSrv     *grpc.Server
	csiSrv     *grpc.Server
}

// Start creates the CSI endpoint socket immediately and defers the
// registration socket until the Node is ready (signalled via nodeReady).
func (c *CSI) Start(ctx context.Context, pluginsDir, registrationDir string) error {
	csiPluginDir := filepath.Join(pluginsDir, c.driverName)
	os.MkdirAll(csiPluginDir, 0o755)

	// CSI endpoint — serves Identity + Node gRPC
	c.endpoint = filepath.Join(csiPluginDir, "csi.sock")
	os.Remove(c.endpoint)
	csiLis, err := net.Listen("unix", c.endpoint)
	if err != nil {
		return fmt.Errorf("csi endpoint listen: %w", err)
	}
	c.csiSrv = grpc.NewServer()
	csipb.RegisterIdentityServer(c.csiSrv, c)
	csipb.RegisterNodeServer(c.csiSrv, c)
	go func() {
		c.log.Info().Str("socket", c.endpoint).Msg("CSI endpoint serving")
		if err := c.csiSrv.Serve(csiLis); err != nil {
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
		regSocket := filepath.Join(registrationDir, c.driverName+"-reg.sock")
		os.Remove(regSocket)
		regLis, err := net.Listen("unix", regSocket)
		if err != nil {
			c.log.Error().Err(err).Msg("CSI registration listen failed")
			return
		}
		c.regSrv = grpc.NewServer()
		pluginreg.RegisterRegistrationServer(c.regSrv, c)
		c.log.Info().Str("socket", regSocket).Msg("CSI registration serving")
		if err := c.regSrv.Serve(regLis); err != nil {
			c.log.Error().Err(err).Msg("CSI registration exited")
		}
	}()

	return nil
}

// Stop gracefully shuts down both gRPC servers.
func (c *CSI) Stop() {
	if c.csiSrv != nil {
		c.csiSrv.GracefulStop()
	}
	if c.regSrv != nil {
		c.regSrv.GracefulStop()
	}
}

// --- pluginregistration.Registration ---

func (c *CSI) GetInfo(_ context.Context, _ *pluginreg.InfoRequest) (*pluginreg.PluginInfo, error) {
	c.log.Info().Msg("GetInfo called by plugin watcher")
	return &pluginreg.PluginInfo{
		Type:              pluginreg.CSIPlugin,
		Name:              c.driverName,
		Endpoint:          c.endpoint,
		SupportedVersions: []string{"1.0.0"},
	}, nil
}

func (c *CSI) NotifyRegistrationStatus(_ context.Context, status *pluginreg.RegistrationStatus) (*pluginreg.RegistrationStatusResponse, error) {
	if status.PluginRegistered {
		c.log.Info().Msg("CSI driver registered with kubelet")
	} else {
		c.log.Error().Str("error", status.Error).Msg("CSI driver registration failed")
	}
	return &pluginreg.RegistrationStatusResponse{}, nil
}

// --- CSI Identity ---

func (c *CSI) GetPluginInfo(_ context.Context, _ *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          c.driverName,
		VendorVersion: csiDriverVersion(),
	}, nil
}

func (c *CSI) GetPluginCapabilities(_ context.Context, _ *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
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

func (c *CSI) Probe(_ context.Context, _ *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{Ready: &wrappers.BoolValue{Value: true}}, nil
}

// --- CSI Node ---

func (c *CSI) NodeGetInfo(_ context.Context, _ *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	hostInfo, err := c.backend.HostInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get host info: %w", err)
	}
	return &csipb.NodeGetInfoResponse{
		NodeId: hostInfo.Hostname,
	}, nil
}

func (c *CSI) NodeGetCapabilities(_ context.Context, _ *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{
		Capabilities: []*csipb.NodeServiceCapability{},
	}, nil
}

func (c *CSI) NodePublishVolume(_ context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	c.log.Info().Str("volume", volumeID).Str("target", targetPath).Msg("NodePublishVolume")

	// Create Docker volume
	mountpoint, err := c.backend.Driver.CreateVolume(volumeID, "")
	if err != nil {
		return nil, fmt.Errorf("create volume %q: %w", volumeID, err)
	}

	// Ensure target directory exists and symlink to the Docker volume mountpoint
	os.MkdirAll(filepath.Dir(targetPath), 0o755)
	os.Remove(targetPath)
	if err := os.Symlink(mountpoint, targetPath); err != nil {
		return nil, fmt.Errorf("symlink %s -> %s: %w", targetPath, mountpoint, err)
	}

	return &csipb.NodePublishVolumeResponse{}, nil
}

func (c *CSI) NodeUnpublishVolume(_ context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	targetPath := req.GetTargetPath()
	c.log.Info().Str("volume", volumeID).Str("target", targetPath).Msg("NodeUnpublishVolume")

	os.Remove(targetPath)

	if err := c.backend.Driver.RemoveVolume(volumeID); err != nil {
		c.log.Warn().Err(err).Str("volume", volumeID).Msg("failed to remove volume")
	}

	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

var (
	_ csipb.IdentityServer               = &CSI{}
	_ csipb.NodeServer                   = &CSI{}
	_ pluginreg.UnsafeRegistrationServer = &CSI{}
)
