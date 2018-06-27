package storage

import (
	"fmt"

	"github.com/contribsys/faktory/storage/rocksdb"
	"github.com/contribsys/faktory/storage/types"
)

func Open(dbtype string, path string) (types.Store, error) {
	if dbtype == "rocksdb" {
		return rocksdb.OpenRocks(path)
	} else {
		return nil, fmt.Errorf("Invalid dbtype: %s", dbtype)
	}
}
