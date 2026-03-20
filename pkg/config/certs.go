package config

import (
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
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/rs/zerolog/log"
)

var logger = component.NewLogger("config")

type Certs struct {
	Name     string
	DataDir  string
	Hostname string
	CertPEM  []byte
	KeyPEM   []byte
}

func NewCerts(name, dataDir, hostname string, extraIPs ...net.IP) *Certs {
	c := &Certs{
		Name:     name,
		DataDir:  dataDir,
		Hostname: hostname,
	}

	// Read existing certs if they exist and SANs are up to date
	certPEM, certErr := os.ReadFile(filepath.Join(dataDir, "ca.crt"))
	keyPEM, keyErr := os.ReadFile(filepath.Join(dataDir, "ca.key"))
	if certErr == nil && keyErr == nil && certsHaveIPs(certPEM, extraIPs) {
		c.CertPEM = certPEM
		c.KeyPEM = keyPEM
		logger.Info().Msg("certificates loaded from disk")
		return c
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
			CommonName:   name,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames: []string{
			hostname,
			"localhost",
			"kubernetes",
			"kubernetes.default",
			"kubernetes.default.svc",
			"kubernetes.default.svc." + hostname,
		},
		IPAddresses: append([]net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		}, extraIPs...),
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

	logger.Info().Msg("certificates generated")
	return c
}

func (c *Certs) Cert() []byte {
	return c.CertPEM
}

func (c *Certs) Key() []byte {
	return c.KeyPEM
}

func (c *Certs) CertPath() string {
	caFile := filepath.Join(c.DataDir, "ca.crt")
	os.WriteFile(caFile, c.CertPEM, 0o644)
	return caFile
}

func (c *Certs) KeyPath() string {
	keyFile := filepath.Join(c.DataDir, "ca.key")
	os.WriteFile(keyFile, c.KeyPEM, 0o600)
	return keyFile
}

func certsHaveIPs(certPEM []byte, ips []net.IP) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}
	for _, want := range ips {
		found := false
		for _, have := range cert.IPAddresses {
			if have.Equal(want) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
