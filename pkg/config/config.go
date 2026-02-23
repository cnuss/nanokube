package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	kjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
)

type Config struct {
	Ctx          context.Context
	Name         string
	DataDir      string
	Verbose      bool
	Certs        *Certs
	Runtime      *Runtime
	FeatureGates map[string]bool
	Kubelet      kubeletconfig.KubeletConfiguration
}

func NewConfig(ctx context.Context, name, dataDir string, verbose, clean bool) *Config {
	log.Info().Str("dir", dataDir).Msg("generating configuration")

	if clean {
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0755)

	runtime := NewRuntime(ctx)

	return &Config{
		Ctx:     ctx,
		Name:    name,
		DataDir: dataDir,
		Verbose: verbose,
		Certs:   &Certs{Name: name, DataDir: dataDir, Runtime: runtime},
		Runtime: runtime,
		FeatureGates: map[string]bool{
			"APIServerIdentity":         false,
			"RuntimeClassInImageCriApi": false,
		},
		Kubelet: *newKubeletConfig(runtime, dataDir),
	}
}

func newKubeletConfig(runtime *Runtime, dataDir string) *kubeletconfig.KubeletConfiguration {
	c, err := options.NewKubeletConfiguration()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create default kubelet config")
	}

	c.ContainerRuntimeEndpoint = runtime.ContainerRuntimeEndpoint()
	c.StaticPodPath = filepath.Join(dataDir, "manifests")
	c.PodLogsDir = filepath.Join(dataDir, "logs")
	c.ClusterDomain = runtime.Domain()
	c.ClusterDNS = runtime.Nameservers()
	c.Authentication.Anonymous.Enabled = true
	c.Authentication.Webhook.Enabled = false
	c.Authorization.Mode = kubeletconfig.KubeletAuthorizationModeAlwaysAllow
	c.CgroupsPerQOS = false
	c.CgroupDriver = "cgroupfs"
	c.EnforceNodeAllocatable = []string{}
	c.EvictionHard = map[string]string{}
	c.ImageGCHighThresholdPercent = 100
	c.FailSwapOn = false
	c.LocalStorageCapacityIsolation = false
	c.RotateCertificates = false
	c.ServerTLSBootstrap = false
	c.RegisterNode = true
	c.CPUCFSQuota = false
	c.ProtectKernelDefaults = false

	return c
}

func (c *Config) KubeletConfigPath() string {
	path := filepath.Join(c.DataDir, "kubelet.yaml")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Set lazy fields before serialization
		c.Kubelet.TLSCertFile = c.Certs.CertPath()
		c.Kubelet.TLSPrivateKeyFile = c.Certs.KeyPath()
		c.Kubelet.FeatureGates = c.FeatureGates

		scheme, _, _ := kubeletscheme.NewSchemeAndCodecs()
		versionedCfg := &kubeletconfigv1beta1.KubeletConfiguration{}
		scheme.Convert(&c.Kubelet, versionedCfg, nil)
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

func (c *Config) KubeconfigPath() string {
	path := filepath.Join(c.DataDir, "kubeconfig")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		kubeconfig := clientcmdapi.Config{
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
		clientcmd.WriteToFile(kubeconfig, path)
	}
	return path
}

func (c *Config) KubeArgs() []string {
	gates := make([]string, 0, len(c.FeatureGates))
	for k, v := range c.FeatureGates {
		gates = append(gates, fmt.Sprintf("%s=%t", k, v))
	}
	args := []string{
		"--feature-gates=" + strings.Join(gates, ","),
	}
	if c.Verbose {
		args = append(args, "--v=4")
	} else {
		args = append(args, "--v=0")
	}
	return args
}
