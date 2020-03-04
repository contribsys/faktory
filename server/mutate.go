package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/manager"
	"github.com/contribsys/faktory/storage"
)

var (
	AlwaysMatch = func(value string) bool {
		return true
	}
)

func mutateKill(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	match, matchfn := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		if matchfn(string(ent.Value())) {
			return ss.MoveTo(store.Dead(), ent, time.Now().Add(manager.DeadTTL))
		}
		return nil
	})
}

func mutateRequeue(store storage.Store, op client.Operation) error {
	ss := setForTarget(store, string(op.Target))
	match, matchfn := matchForFilter(op.Filter)
	return ss.Find(match, func(idx int, ent storage.SortedEntry) error {
		if matchfn(string(ent.Value())) {
			j, err := ent.Job()
			if err != nil {
				return err
			}
			q, err := store.GetQueue(j.Queue)
			if err != nil {
				return err
			}
			err = q.Push(ent.Value())
			if err != nil {
				return err
			}
			return ss.RemoveEntry(ent)
		}
		return nil
	})
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
	parts := strings.Split(cmd, " ")
	if len(parts) != 2 {
		_ = c.Error(cmd, fmt.Errorf("Invalid format"))
		return
	}

	var err error
	var op client.Operation
	err = json.Unmarshal([]byte(parts[1]), &op)
	if err != nil {
		_ = c.Error(cmd, err)
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
		_ = c.Error(cmd, err)
		return
	}

	_ = c.Ok()
}

func mutateClear(store storage.Store, target string) error {
	ss := setForTarget(store, target)
	if ss != nil {
		return ss.Clear()
	}
	return fmt.Errorf("Cannot clear, invalid set: %s", target)
}

func setForTarget(store storage.Store, name string) storage.SortedSet {
	switch name {
	case "retries":
		return store.Retries()
	case "dead":
		return store.Dead()
	case "scheduled":
		return store.Scheduled()
	default:
		return nil
	}
}
