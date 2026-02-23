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
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type Certs struct {
	Name    string
	DataDir string
	Runtime *Runtime
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
				c.Runtime.Hostname(),
				"kubernetes",
				"kubernetes.default",
				"kubernetes.default.svc",
				"kubernetes.default.svc." + c.Runtime.Domain(),
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
