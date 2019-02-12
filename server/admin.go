package server

import (
	"fmt"
	"strings"

	"github.com/contribsys/faktory/util"
)

// PAGE [structure] [offset] [size]
func adminPage(c *Connection, s *Server, cmd string) {
	util.Info(cmd)
	parts := strings.Split(cmd, " ")
	if len(parts) < 2 || len(parts) > 3 {
		c.Error(cmd, fmt.Errorf("Invalid PAGE"))
		return
	}
	structure := parts[0]
	if structure == "retries" || structure == "dead" {
		zPage(structure, parts[1], 50)
	} else {
		lPage(structure, parts[1], 50)
	}

}
func adminKill(c *Connection, s *Server, cmd string) {
}
func adminEnqueue(c *Connection, s *Server, cmd string) {
}
func adminDiscard(c *Connection, s *Server, cmd string) {
}
