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

type Certs struct {
	DataDir string

	CACert     string
	CAKey      string
	ServerCert string
	ServerKey  string
	ClientCert string
	ClientKey  string
	Kubeconfig string
}

type Config struct {
	DataDir      string
	Verbose      bool
	Certs        *Certs
	FeatureGates []string
}

func NewConfig(dataDir string, verbose bool) *Config {
	certDir := filepath.Join(dataDir, "certs")
	return &Config{
		DataDir: dataDir,
		Verbose: verbose,
		Certs: &Certs{
			DataDir:    certDir,
			CACert:     filepath.Join(certDir, "ca.crt"),
			CAKey:      filepath.Join(certDir, "ca.key"),
			ServerCert: filepath.Join(certDir, "server.crt"),
			ServerKey:  filepath.Join(certDir, "server.key"),
			ClientCert: filepath.Join(certDir, "client.crt"),
			ClientKey:  filepath.Join(certDir, "client.key"),
			Kubeconfig: filepath.Join(certDir, "kubeconfig"),
		},
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
	log.Info().Str("dir", c.Certs.DataDir).Msg("generating certificates")

	if err := os.MkdirAll(c.Certs.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create cert dir: %w", err)
	}

	// Generate CA
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate CA key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"nanokube"},
			CommonName:   "nanokube-ca",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create CA cert: %w", err)
	}

	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return fmt.Errorf("failed to parse CA cert: %w", err)
	}

	if err := c.Certs.writeKeyPair(c.Certs.CACert, c.Certs.CAKey, caCertDER, caKey); err != nil {
		return fmt.Errorf("failed to write CA: %w", err)
	}

	// Generate server cert
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate server key: %w", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"system:masters"},
			CommonName:   "nanokube",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
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

	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create server cert: %w", err)
	}

	if err := c.Certs.writeKeyPair(c.Certs.ServerCert, c.Certs.ServerKey, serverCertDER, serverKey); err != nil {
		return fmt.Errorf("failed to write server cert: %w", err)
	}

	// Generate client cert
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate client key: %w", err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"system:masters"},
			CommonName:   "kubernetes-admin",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(1, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create client cert: %w", err)
	}

	if err := c.Certs.writeKeyPair(c.Certs.ClientCert, c.Certs.ClientKey, clientCertDER, clientKey); err != nil {
		return fmt.Errorf("failed to write client cert: %w", err)
	}

	// Generate kubeconfig
	if err := c.Certs.writeKubeconfig(caCertDER, clientCertDER, clientKey); err != nil {
		return fmt.Errorf("failed to write kubeconfig: %w", err)
	}

	log.Info().Msg("certificates generated")
	return nil
}

func (c *Certs) writeKeyPair(certPath, keyPath string, certDER []byte, key *ecdsa.PrivateKey) error {
	certFile, err := os.Create(certPath)
	if err != nil {
		return err
	}
	defer certFile.Close()

	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return err
	}

	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
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

func (c *Certs) writeKubeconfig(caCertDER, clientCertDER []byte, clientKey *ecdsa.PrivateKey) error {
	// Encode cert to PEM
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER})

	// Encode key to PEM
	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		return err
	}
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// Build kubeconfig
	config := clientcmdapi.NewConfig()

	config.Clusters["nanokube"] = &clientcmdapi.Cluster{
		Server:                   "https://127.0.0.1:6443",
		CertificateAuthorityData: caCertPEM,
	}

	config.AuthInfos["nanokube-admin"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: clientCertPEM,
		ClientKeyData:         clientKeyPEM,
	}

	config.Contexts["nanokube"] = &clientcmdapi.Context{
		Cluster:  "nanokube",
		AuthInfo: "nanokube-admin",
	}

	config.CurrentContext = "nanokube"

	return clientcmd.WriteToFile(*config, c.Kubeconfig)
}
