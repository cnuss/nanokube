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
)

type Certs struct {
	log      component.Logger
	Name     string
	DataDir  string
	Hostname string
	CertPEM  []byte
	KeyPEM   []byte
}

func NewCerts(name, dataDir, hostname string, extraIPs ...net.IP) *Certs {
	c := &Certs{
		log:      component.NewLogger("certs"),
		Name:     name,
		DataDir:  dataDir,
		Hostname: hostname,
	}

	// Reuse existing key if available, otherwise generate a new one
	var key *ecdsa.PrivateKey
	if keyPEM, err := os.ReadFile(filepath.Join(dataDir, "ca.key")); err == nil {
		block, _ := pem.Decode(keyPEM)
		if block != nil {
			key, _ = x509.ParseECPrivateKey(block.Bytes)
		}
	}
	if key == nil {
		var err error
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to generate key")
		}
		c.log.Info().Msg("generated new key")
	} else {
		c.log.Info().Msg("reusing existing key")
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
		c.log.Error().Err(err).Msg("failed to create cert")
	}

	c.CertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to marshal key")
	}
	c.KeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	c.log.Info().Msg("certificates generated")
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
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		os.WriteFile(keyFile, c.KeyPEM, 0o600)
	}
	return keyFile
}
