package storage

import (
	"fmt"

	"github.com/mperham/faktory/util"
	"github.com/mperham/gorocksdb"
)

/*
 * I'm unclear what, if any, help this is.  Currently unused.
 */
func (store *rocksStore) Compact() error {
	fo := gorocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()

	err := store.db.Flush(fo)
	if err != nil {
		return err
	}

	store.db.CompactRange(gorocksdb.Range{[]byte{0x00}, []byte{0xFF}})
	return nil
}

/*
 * TODO Purge old backups
 */
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
