package internal

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type Config struct {
	Name         string
	DataDir      string
	Verbose      bool
	CertPath     string
	KeyPath      string
	Kubeconfig   string
	FeatureGates []string
}

func NewConfig(name, dataDir string, verbose, clean bool) *Config {
	if clean {
		os.RemoveAll(dataDir)
	}
	os.MkdirAll(dataDir, 0755)

	return &Config{
		Name:       name,
		DataDir:    dataDir,
		Verbose:    verbose,
		CertPath:   filepath.Join(dataDir, "ca.crt"),
		KeyPath:    filepath.Join(dataDir, "ca.key"),
		Kubeconfig: filepath.Join(dataDir, "kubeconfig"),
		FeatureGates: []string{
			"APIServerIdentity=false",
			"RuntimeClassInImageCriApi=false",
		},
	}
}

func (c *Config) FeatureGatesString() string {
	return strings.Join(c.FeatureGates, ",")
}

func (c *Config) KubeArgs() []string {
	args := []string{
		"--feature-gates=" + c.FeatureGatesString(),
	}
	if c.Verbose {
		args = append(args, "--v=4")
	} else {
		args = append(args, "--v=0")
	}
	return args
}

func (c *Config) Generate() error {
	log.Info().Str("dir", c.DataDir).Msg("generating certificates")

	if err := os.MkdirAll(c.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
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
		return fmt.Errorf("failed to create cert: %w", err)
	}

	if err := c.writeKeyPair(certDER, key); err != nil {
		return fmt.Errorf("failed to write cert: %w", err)
	}

	if err := c.writeKubeconfig(certDER, key); err != nil {
		return fmt.Errorf("failed to write kubeconfig: %w", err)
	}

	log.Info().Msg("certificates generated")
	return nil
}

func (c *Config) writeKeyPair(certDER []byte, key *ecdsa.PrivateKey) error {
	certFile, err := os.Create(c.CertPath)
	if err != nil {
		return err
	}
	defer certFile.Close()

	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return err
	}

	keyFile, err := os.OpenFile(c.KeyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer keyFile.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}

	return pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func (c *Config) writeKubeconfig(certDER []byte, key *ecdsa.PrivateKey) error {
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	config := clientcmdapi.NewConfig()

	config.Clusters[c.Name] = &clientcmdapi.Cluster{
		Server:                   "https://127.0.0.1:6443",
		CertificateAuthorityData: certPEM,
	}

	config.AuthInfos[c.Name] = &clientcmdapi.AuthInfo{
		ClientCertificateData: certPEM,
		ClientKeyData:         keyPEM,
	}

	config.Contexts[c.Name] = &clientcmdapi.Context{
		Cluster:  c.Name,
		AuthInfo: c.Name,
	}

	config.CurrentContext = c.Name

	return clientcmd.WriteToFile(*config, c.Kubeconfig)
}
