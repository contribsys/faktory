package storage

import (
	"fmt"

	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

const (
	// Assume hourly backups and keep a day's worth.
	// If we take backups every 5 minutes, this will keep
	// two hours worth.
	DefaultKeepBackupsCount int = 24
)

func (store *rocksStore) PurgeOldBackups(keepCount int) error {
	be, err := gorocksdb.OpenBackupEngine(store.opts, store.db.Name())
	if err != nil {
		return err
	}
	defer be.Close()

	return be.PurgeOldBackups(keepCount)
}

func (store *rocksStore) Backup() error {
	fo := gorocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()

	err := store.db.Flush(fo)
	if err != nil {
		return err
	}

	be, err := gorocksdb.OpenBackupEngine(store.opts, store.db.Name())
	if err != nil {
		return err
	}
	defer be.Close()

	return be.CreateNewBackup(store.db)
}

func (store *rocksStore) EachBackup(fn func(BackupInfo)) error {
	be, err := gorocksdb.OpenBackupEngine(store.opts, store.db.Name())
	if err != nil {
		return err
	}
	defer be.Close()
	bei := be.GetInfo()
	defer bei.Destroy()

	for i := 0; i < bei.GetCount(); i++ {
		fn(BackupInfo{
			Id:        bei.GetBackupId(i),
			FileCount: bei.GetNumFiles(i),
			Size:      bei.GetSize(i),
			Timestamp: bei.GetTimestamp(i),
		})
	}
	return nil
}

// Restore the latest backup.
// WARNING:
// After this call, the current rocksStore is closed/invalid
// and the caller MUST open a new Store.
func (store *rocksStore) RestoreFromLatest() error {
	util.Warnf("Restoring from latest backup")

	path := store.db.Name()

	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()

	be, err := gorocksdb.OpenBackupEngine(opts, path)
	if err != nil {
		return err
	}
	defer be.Close()

	ro := gorocksdb.NewRestoreOptions()
	defer ro.Destroy()

	// point of no return
	store.Close()

	err = be.RestoreDBFromLatestBackup(path, path, ro)
	if err != nil {
		panic(fmt.Errorf("Unable to restore from latest backup, cannot continue: %s", err))
	}

	return nil
}
