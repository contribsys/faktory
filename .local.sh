ROCKSDB_HOME=/usr/local/Cellar/rocksdb/5.5.1
CGO_CFLAGS="-I${ROCKSDB_HOME}/include"
CGO_LDFLAGS="-L${ROCKSDB_HOME} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
