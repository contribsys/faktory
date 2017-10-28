package server

import (
	"fmt"
	"os"
	"testing"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestTlsConfig(t *testing.T) {
	// no TLS
	cfg, err := tlsConfig("localhost:7419", false, "/etc/faktory")
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// without CA bundle
	cfg, err = findTlsConfigIn(":7419", false, []string{"../test/tls/1"})
	assert.NotNil(t, cfg)
	assert.NoError(t, err)

	// with CA bundle
	cfg, err = findTlsConfigIn(":7419", false, []string{"../test/tls/2"})
	assert.NotNil(t, cfg)
	assert.NoError(t, err)

	// the certs are self-signed for "acme.example.com"
	cfg.BuildNameToCertificate()
	assert.Equal(t, 1, len(cfg.NameToCertificate))
	assert.NotNil(t, cfg.NameToCertificate["acme.example.com"])

	// requires certs, but not there
	cfg, err = findTlsConfigIn(":7419", false, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.Error(t, err, "not found")

	// does not require certs, and not there
	cfg, err = findTlsConfigIn("localhost:7419", false, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// disable TLS, no certs
	cfg, err = findTlsConfigIn("localhost:7419", true, []string{"/tmp"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)

	// disable TLS, certs
	cfg, err = findTlsConfigIn(":7419", true, []string{"../test/tls/1"})
	assert.Nil(t, cfg)
	assert.NoError(t, err)
}

func TestPasswords(t *testing.T) {
	pwd, err := fetchPassword("../test/auth")
	assert.NoError(t, err)
	assert.Equal(t, 16, len(pwd))

	pwd, err = fetchPassword("../test/foo")
	assert.NoError(t, err)
	assert.Equal(t, "", pwd)
}

/*
 * This sets up a full server running TLS+password, connects to it with a client
 * and tries to run a few commands.  A basic integration test.
 */
func TestFullTLS(t *testing.T) {
	ok, err := util.FileExists(os.ExpandEnv("$HOME/.faktory/tls/public.crt"))
	assert.NoError(t, err)
	if !ok {
		fmt.Println("Skipping full TLS test, cert not found")
		return
	}

	os.Setenv("FAKTORY_PASSWORD", "password123")
	runServer("localhost.contribsys.com:7520", func() {
		svr := faktory.DefaultServer()
		svr.Address = "localhost.contribsys.com:7520"

		client, err := faktory.Dial(svr, "password123")
		assert.NoError(t, err)

		result, err := client.Info()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		err = client.Flush()
		assert.NoError(t, err)
		x, err := client.Beat()
		assert.NoError(t, err)
		assert.Equal(t, "", x)
	})
}
