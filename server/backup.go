package server

import (
	"os"
	"strconv"
	"time"

	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type backupPolicy struct {
	*Server
	frequency time.Duration
	keep      int
	count     int
}

func newBackupPolicy(s *Server) *backupPolicy {
	freak := backupFrequency()
	kp := backupCount()
	bp := &backupPolicy{
		Server:    s,
		frequency: freak,
		keep:      kp,
		count:     0,
	}
	if bp.IsEnabled() {
		util.Debugf("Backing up storage every %v", freak)
	}
	return bp
}

func (bp *backupPolicy) IsEnabled() bool {
	return bp.Server.Options.Environment == "production" && bp.keep > 0
}

func (bp *backupPolicy) Frequency() int64 {
	return int64(bp.frequency.Seconds())
}

func (bp *backupPolicy) Name() string {
	return "backup"
}

func (bp *backupPolicy) Execute() error {
	if !bp.IsEnabled() {
		// we only back up in production mode, don't need to fill
		// developer's laptop with old junk data
		return nil
	}
	bp.count++

	util.Debug("Running automatic backup...")
	err := bp.Server.Store().Backup()
	if err != nil {
		util.Error("BACKUP FAILED", err)
		return err
	}
	err = bp.Server.Store().PurgeOldBackups(bp.keep)
	if err != nil {
		util.Error("PURGE FAILED", err)
		return err
	}
	return nil
}

func (bp *backupPolicy) Stats() map[string]interface{} {
	return map[string]interface{}{
		"count":     bp.count,
		"keep":      bp.keep,
		"frequency": bp.frequency,
	}
}

func backupFrequency() time.Duration {
	durs := os.Getenv("FAKTORY_BACKUP_FREQUENCY")
	if durs == "" {
		// by default, backup every hour
		return time.Duration(1 * time.Hour)
	}

	dur, err := time.ParseDuration(durs)
	if err != nil {
		util.Warnf("Invalid backup duration: %s", err)
		return time.Duration(1 * time.Hour)
	}

	if dur.Seconds() < 300 {
		util.Warnf("Can't take a backup more than once every 5 minutes: %v", dur)
		return time.Duration(5 * time.Minute)
	}
	return dur
}

func backupCount() int {
	durs := os.Getenv("FAKTORY_BACKUP_COUNT")
	if durs == "" {
		// by default, backup every hour
		return storage.DefaultKeepBackupsCount
	}

	dur, err := strconv.ParseInt(durs, 10, 64)
	if err != nil {
		util.Warnf("Invalid backup count: %s", err)
		return storage.DefaultKeepBackupsCount
	}

	return int(dur)
}
