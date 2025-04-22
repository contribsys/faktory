package server

import (
	"regexp"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestCommands(t *testing.T) {
	util.LogInfo = true
	util.LogDebug = true
	runServer("localhost:4478", func(s *Server) {
		t.Run("queue latency", func(t *testing.T) {
			c := dummyConnection()
			assert.NotNil(t, c.Context)

			queue(c, s, "QUEUE LATENCY *")
			txt := output(c)
			assert.Equal(t, "-ERR QUEUE LATENCY does not support wildcards\r\n", txt)

			queue(c, s, "QUEUE LATENCY default")
			txt = output(c)
			assert.Equal(t, "$13\r\n{\"default\":0}\r\n", txt)

			ctx := c.Context
			job := client.NewJob("jobtype", 1, 2, "mike")
			assert.NoError(t, s.Manager().Push(ctx, job))

			queue(c, s, "queue latency default foo")
			txt = output(c)
			assert.Regexp(t, regexp.MustCompile("\"default\":0.\\d{6}"), txt)
			assert.Regexp(t, regexp.MustCompile("\"foo\":0"), txt)
		})
	})
}
