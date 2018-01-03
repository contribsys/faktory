package storage

const (
	// Assume hourly backups and keep a day's worth.
	// If we take backups every 5 minutes, this will keep
	// two hours worth.
	DefaultKeepBackupsCount int = 24
)

func (store *bStore) PurgeOldBackups(keepCount int) error {
	return nil
}

func (store *bStore) Backup() error {
	return nil
}

func (store *bStore) EachBackup(fn func(BackupInfo)) error {
	return nil
}

// Restore the latest backup.
// WARNING:
// After this call, the current rocksStore is closed/invalid
// and the caller MUST open a new Store.
func (store *bStore) RestoreFromLatest() error {
	return store.Close()
}
