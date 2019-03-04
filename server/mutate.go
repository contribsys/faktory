package server

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

// PAGE [structure] [offset] [size]
func mutatePage(c *Connection, s *Server, cmd string) {
	util.Info(cmd)
	parts := strings.Split(cmd, " ")
	if len(parts) < 2 || len(parts) > 3 {
		c.Error(cmd, fmt.Errorf("Invalid PAGE"))
		return
	}
	structure := parts[0]
	if structure == "retries" || structure == "dead" {
		//zPage(structure, parts[1], 50)
	} else {
		//lPage(structure, parts[1], 50)
	}

}
func mutateKill(store storage.Store, op client.Operation) error {
	return nil
}

func mutateRequeue(store storage.Store, op client.Operation) error {
	return nil
}

func mutateDiscard(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	if op.Filter == nil {
		return ss.Clear()
	}
	match, matchfn := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		if matchfn(string(ent.Value())) {
			return ss.RemoveEntry(ent)
		}
		return nil
	})
}

var (
	AlwaysMatch = func(value string) bool {
		return true
	}
)

func matchForFilter(filter *client.JobFilter) (string, func(value string) bool) {
	if filter == nil {
		return "*", AlwaysMatch
	}

	if filter.Regexp != "" {
		if filter.Jobtype == "" {
			return filter.Regexp, AlwaysMatch
		} else {
			// if a regexp and jobtype, pass the regexp to Redis and match jobtype
			// here
			typematch := fmt.Sprintf(`"jobtype":"%s"`, filter.Jobtype)
			return filter.Regexp, func(value string) bool {
				return strings.Index(value, typematch) > 0
			}
		}
	}

	if filter.Jobtype != "" {
		return fmt.Sprintf(`*"jobtype":"%s"*`, filter.Jobtype), AlwaysMatch
	}

	if len(filter.Jids) > 0 {
		return "*", func(value string) bool {
			for _, jid := range filter.Jids {
				if strings.Index(value, fmt.Sprintf(`"jid":"%s"`, jid)) > 0 {
					return true
				}
			}
			return false
		}
	}
	return "*", AlwaysMatch
}

func mutate(c *Connection, s *Server, cmd string) {
	util.Info(cmd)
	parts := strings.Split(cmd, " ")
	if len(parts) != 2 {
		c.Error(cmd, fmt.Errorf("Invalid format"))
		return
	}

	var err error
	var op client.Operation
	err = json.Unmarshal([]byte(parts[1]), &op)
	if err != nil {
		c.Error(cmd, err)
		return
	}

	switch op.Cmd {
	case "clear":
		err = mutateClear(s.Store(), string(op.Target))
	case "kill":
		err = mutateKill(s.Store(), op)
	case "discard":
		err = mutateDiscard(s.Store(), op)
	case "requeue":
		err = mutateRequeue(s.Store(), op)
	default:
		err = fmt.Errorf("Unknown mutate operation")
	}

	if err != nil {
		c.Error(cmd, err)
		return
	}

	c.Ok()
}

func mutateClear(store storage.Store, target string) error {
	ss := setForTarget(store, target)
	if ss != nil {
		return ss.Clear()
	} else {
		q, err := store.GetQueue(target)
		if err != nil {
			return err
		}
		_, err = q.Clear()
		return err
	}
}

func setForTarget(store storage.Store, name string) storage.SortedSet {
	switch name {
	case "retry":
		return store.Retries()
	case "dead":
		return store.Dead()
	case "scheduled":
		return store.Scheduled()
	default:
		return nil
	}

}
