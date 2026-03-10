package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	kjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
)

var logger = component.NewLogger("config")

type Config struct {
	Name         string
	DataDir      string
	Verbosity    int
	Certs        *certs
	FeatureGates map[string]bool
	Components   []component.Component
	CRID         *crid.CRID
}

func NewConfig(options *Options) *Config {
	logger.Info().Str("dir", options.DataDir).Msg("generating configuration")

	return &Config{
		Name:      options.Name,
		DataDir:   options.DataDir,
		Verbosity: options.Verbosity,
		FeatureGates: map[string]bool{
			"APIServerIdentity":         false,
			"RuntimeClassInImageCriApi": false,
		},
		Certs:      &certs{Name: options.Name, DataDir: options.DataDir},
		Components: []component.Component{},
	}
}

func (c *Config) SetCRID(crid *crid.CRID) error {
	host, err := crid.DefaultBackend().HostInfo()
	if err != nil {
		return fmt.Errorf("crid host probe: %w", err)
	}
	c.CRID = crid
	c.Certs.Hostname = host.Hostname
	return nil
}

func (c *Config) KubeletConfig() *kubeletconfig.KubeletConfiguration {
	return &kubeletconfig.KubeletConfiguration{
		// ContainerRuntimeEndpoint: c.CRI.Endpoint(),
		// StaticPodPath:            filepath.Join(c.DataDir, "manifests"),
		// PodLogsDir:               filepath.Join(c.DataDir, "logs"),
		// ClusterDomain:            c.CRI.Domain(),
		// ClusterDNS:               c.CRI.Nameservers(),
		// Authentication: kubeletconfig.KubeletAuthentication{
		// 	Anonymous: kubeletconfig.KubeletAnonymousAuthentication{
		// 		Enabled: true,
		// 	},
		// 	Webhook: kubeletconfig.KubeletWebhookAuthentication{
		// 		Enabled: false,
		// 	},
		// },
		// Authorization: kubeletconfig.KubeletAuthorization{
		// 	Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
		// },
		// CgroupsPerQOS:                 false,
		// CgroupDriver:                  "cgroupfs",
		// EnforceNodeAllocatable:        []string{},
		// EvictionHard:                  map[string]string{},
		// ImageGCHighThresholdPercent:   100,
		// FailSwapOn:                    false,
		// LocalStorageCapacityIsolation: false,
		// RotateCertificates:            false,
		// ServerTLSBootstrap:            false,
		// RegisterNode:                  true,
		// CPUCFSQuota:                   false,
		// CPUCFSQuotaPeriod:             metav1.Duration{Duration: 100 * time.Millisecond},
		// ContainerLogMaxFiles:          5,
		// ContainerLogMaxWorkers:        1,
		// ContainerLogMonitorInterval:   metav1.Duration{Duration: 10 * time.Second},
		// ProtectKernelDefaults:         false,
	}
}

// ApplyKubeletConfig applies nanokube-specific overrides to an already-defaulted
// KubeletConfiguration (as returned by options.NewKubeletConfiguration).
func (c *Config) ApplyKubeletConfig(cfg *kubeletconfig.KubeletConfiguration) {
	// cfg.ContainerRuntimeEndpoint = c.CRI.Endpoint()
	// cfg.StaticPodPath = filepath.Join(c.DataDir, "manifests")
	// cfg.PodLogsDir = filepath.Join(c.DataDir, "logs")
	// cfg.ClusterDomain = c.CRI.Domain()
	// cfg.ClusterDNS = c.CRI.Nameservers()
	// cfg.Authentication = kubeletconfig.KubeletAuthentication{
	// 	Anonymous: kubeletconfig.KubeletAnonymousAuthentication{Enabled: true},
	// 	Webhook:   kubeletconfig.KubeletWebhookAuthentication{Enabled: false},
	// }
	// cfg.Authorization = kubeletconfig.KubeletAuthorization{
	// 	Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
	// }
	// cfg.TLSCertFile = c.Certs.CertPath()
	// cfg.TLSPrivateKeyFile = c.Certs.KeyPath()
	// cfg.EnableServer = true
	// cfg.Port = 10250
	// cfg.ReadOnlyPort = 0
	// cfg.EnableControllerAttachDetach = false
	// cfg.HairpinMode = kubeletconfig.HairpinVeth
	// cfg.CgroupsPerQOS = false
	// cfg.CgroupDriver = "cgroupfs"
	// cfg.EnforceNodeAllocatable = []string{}
	// cfg.EvictionHard = map[string]string{}
	// cfg.ImageGCHighThresholdPercent = 100
	// cfg.FailSwapOn = false
	// cfg.LocalStorageCapacityIsolation = false
	// cfg.RotateCertificates = false
	// cfg.ServerTLSBootstrap = false
	// cfg.RegisterNode = true
	// cfg.CPUCFSQuota = false
	// cfg.CPUCFSQuotaPeriod = metav1.Duration{Duration: 100 * time.Millisecond}
	// cfg.ContainerLogMaxFiles = 5
	// cfg.ContainerLogMaxWorkers = 1
	// cfg.ContainerLogMonitorInterval = metav1.Duration{Duration: 10 * time.Second}
	// cfg.ProtectKernelDefaults = false
	// cfg.FeatureGates = c.FeatureGates
}

func (c *Config) KubeletConfigPath() string {
	config := c.KubeletConfig()
	path := filepath.Join(c.DataDir, "kubelet.yaml")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		scheme, _, _ := kubeletscheme.NewSchemeAndCodecs()
		versionedCfg := &kubeletconfigv1beta1.KubeletConfiguration{}
		scheme.Convert(config, versionedCfg, nil)
		versionedCfg.APIVersion = kubeletconfigv1beta1.SchemeGroupVersion.String()
		versionedCfg.Kind = "KubeletConfiguration"
		// Scheme defaults EnforceNodeAllocatable to ["pods"], override it
		versionedCfg.EnforceNodeAllocatable = []string{}

		serializer := kjson.NewYAMLSerializer(kjson.DefaultMetaFactory, scheme, scheme)
		f, _ := os.Create(path)
		defer f.Close()
		serializer.Encode(versionedCfg, f)
		// enforceNodeAllocatable is omitempty in v1beta1, so write it explicitly
		f.WriteString("enforceNodeAllocatable: []\n")
	}
	return path
}

func (c *Config) Kubeconfig() clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			c.Name: {
				Server:                   "https://127.0.0.1:6443",
				CertificateAuthorityData: c.Certs.Cert(),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			c.Name: {
				ClientCertificateData: c.Certs.Cert(),
				ClientKeyData:         c.Certs.Key(),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			c.Name: {
				Cluster:  c.Name,
				AuthInfo: c.Name,
			},
		},
		CurrentContext: c.Name,
	}
}

func (c *Config) KubeconfigPath() string {
	config := c.Kubeconfig()
	path := filepath.Join(c.DataDir, "kubeconfig")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		clientcmd.WriteToFile(config, path)
	}
	c.mergeKubeconfig(config)
	return path
}

// mergeKubeconfig upserts this cluster's entries into ~/.kube/config
// so that kubectl can reach the cluster without --kubeconfig.
func (c *Config) mergeKubeconfig(src clientcmdapi.Config) {
	dst, err := clientcmd.LoadFromFile(clientcmd.RecommendedHomeFile)
	if err != nil {
		dst = clientcmdapi.NewConfig()
	}

	dst.Clusters[c.Name] = src.Clusters[c.Name]
	dst.AuthInfos[c.Name] = src.AuthInfos[c.Name]
	dst.Contexts[c.Name] = src.Contexts[c.Name]
	dst.CurrentContext = c.Name

	if err := os.MkdirAll(filepath.Dir(clientcmd.RecommendedHomeFile), 0755); err != nil {
		logger.Warn().Err(err).Msg("failed to create ~/.kube directory")
		return
	}
	if err := clientcmd.WriteToFile(*dst, clientcmd.RecommendedHomeFile); err != nil {
		logger.Warn().Err(err).Msg("failed to write ~/.kube/config")
	}
}

func (c *Config) KubeArgs() []string {
	gates := make([]string, 0, len(c.FeatureGates))
	for k, v := range c.FeatureGates {
		gates = append(gates, fmt.Sprintf("%s=%t", k, v))
	}
	args := []string{
		"--feature-gates=" + strings.Join(gates, ","),
	}
	args = append(args, fmt.Sprintf("--v=%d", c.Verbosity*2))
	return args
}
