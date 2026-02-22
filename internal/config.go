package internal

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletscheme "k8s.io/kubernetes/pkg/kubelet/apis/config/scheme"
)

type Config struct {
	ctx          context.Context
	Name         string
	DataDir      string
	Verbose      bool
	Certs        *Certs
	FeatureGates []string
	Kubelet      kubeletconfig.KubeletConfiguration
}

type Certs struct {
	Name    string
	DataDir string
	CertPEM []byte
	KeyPEM  []byte
	once    sync.Once
}

func (c *Certs) generate() {
	c.once.Do(func() {
		certPath := filepath.Join(c.DataDir, "ca.crt")
		keyPath := filepath.Join(c.DataDir, "ca.key")

		// Read existing certs if they exist
		certPEM, certErr := os.ReadFile(certPath)
		keyPEM, keyErr := os.ReadFile(keyPath)
		if certErr == nil && keyErr == nil {
			c.CertPEM = certPEM
			c.KeyPEM = keyPEM
			log.Info().Msg("certificates loaded from disk")
			return
		}

		// Generate new certs
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to generate key")
		}

		template := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				Organization: []string{"system:masters"},
				CommonName:   c.Name,
			},
			NotBefore:             time.Now(),
			NotAfter:              time.Now().AddDate(10, 0, 0),
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
			DNSNames: []string{
				"localhost",
				"kubernetes",
				"kubernetes.default",
				"kubernetes.default.svc",
				"kubernetes.default.svc.cluster.local",
			},
			IPAddresses: []net.IP{
				net.ParseIP("127.0.0.1"),
				net.ParseIP("::1"),
			},
		}

		certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create cert")
		}

		c.CertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to marshal key")
		}
		c.KeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

		log.Info().Msg("certificates generated")
	})
}

func (c *Certs) Cert() []byte {
	c.generate()
	return c.CertPEM
}

func (c *Certs) Key() []byte {
	c.generate()
	return c.KeyPEM
}

func (c *Certs) CertPath() string {
	c.generate()
	path := filepath.Join(c.DataDir, "ca.crt")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.WriteFile(path, c.CertPEM, 0644)
	}
	return path
}

func (c *Certs) KeyPath() string {
	c.generate()
	path := filepath.Join(c.DataDir, "ca.key")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.WriteFile(path, c.KeyPEM, 0600)
	}
	return path
}

func NewConfig(ctx context.Context, name, dataDir string, verbose, clean bool) *Config {
	log.Info().Str("dir", dataDir).Msg("generating configuration")

	if clean {
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0755)

	runtime := NewRuntime(ctx)

	return &Config{
		ctx:     ctx,
		Name:    name,
		DataDir: dataDir,
		Verbose: verbose,
		Certs:   &Certs{Name: name, DataDir: dataDir},
		FeatureGates: []string{
			"APIServerIdentity=false",
			"RuntimeClassInImageCriApi=false",
		},
		Kubelet: kubeletconfig.KubeletConfiguration{
			ContainerRuntimeEndpoint: runtime.ContainerRuntimeEndpoint(),
			StaticPodPath:            "/etc/kubernetes/manifests",
			Authentication: kubeletconfig.KubeletAuthentication{
				Anonymous: kubeletconfig.KubeletAnonymousAuthentication{Enabled: true},
				Webhook:   kubeletconfig.KubeletWebhookAuthentication{Enabled: false},
			},
			Authorization: kubeletconfig.KubeletAuthorization{
				Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
			},
			CgroupsPerQOS:                   false,
			CgroupDriver:                    "cgroupfs",
			EnforceNodeAllocatable:          []string{},
			EvictionHard:                    map[string]string{},
			ImageGCHighThresholdPercent:     100,
			FailSwapOn:                      false,
			LocalStorageCapacityIsolation:   false,
			RotateCertificates:              false,
			ServerTLSBootstrap:              false,
			RegisterNode:                    true,
			CPUCFSQuota:                     true,
			ProtectKernelDefaults:           false,
			ShutdownGracePeriod:             metav1.Duration{Duration: 0},
			ShutdownGracePeriodCriticalPods: metav1.Duration{Duration: 0},
		},
	}
}

func (c *Config) KubeletConfigPath() string {
	path := filepath.Join(c.DataDir, "kubelet.yaml")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		scheme, _, _ := kubeletscheme.NewSchemeAndCodecs()
		versionedCfg := &kubeletconfigv1beta1.KubeletConfiguration{}
		scheme.Convert(&c.Kubelet, versionedCfg, nil)
		versionedCfg.APIVersion = kubeletconfigv1beta1.SchemeGroupVersion.String()
		versionedCfg.Kind = "KubeletConfiguration"

		serializer := kjson.NewYAMLSerializer(kjson.DefaultMetaFactory, scheme, scheme)
		f, _ := os.Create(path)
		defer f.Close()
		serializer.Encode(versionedCfg, f)
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
	args := []string{
		"--feature-gates=" + strings.Join(c.FeatureGates, ","),
	}
	if c.Verbose {
		args = append(args, "--v=4")
	} else {
		args = append(args, "--v=0")
	}
	return args
}
