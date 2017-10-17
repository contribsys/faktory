package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTlsConfig(t *testing.T) {
	// no TLS
	cfg, err := tlsConfig("localhost:7419", false)
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// without CA bundle
	cfg, err = findTlsConfigIn(":7419", false, []string{"../test/tls/1"})
	assert.NotNil(t, cfg)
	assert.NoError(t, err)

	// with CA bundle
	cfg, err = findTlsConfigIn(":7419", false, []string{"../test/tls/1"})
	assert.NotNil(t, cfg)
	assert.NoError(t, err)

	cfg, err = findTlsConfigIn(":7419", false, []string{"../test/tls/2"})
	assert.NotNil(t, cfg)
	assert.NoError(t, err)

	cfg.BuildNameToCertificate()
	assert.Equal(t, 1, len(cfg.NameToCertificate))
	assert.NotNil(t, cfg.NameToCertificate["acme.example.com"])

	// no required certs
	cfg, err = findTlsConfigIn(":7419", false, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.Error(t, err, "not found")

	cfg, err = findTlsConfigIn("localhost:7419", false, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// disable, no certs
	cfg, err = findTlsConfigIn("localhost:7419", true, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// disable, certs
	cfg, err = findTlsConfigIn(":7419", true, []string{"../test/tls/1"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)
}
