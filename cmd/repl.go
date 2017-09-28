package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mperham/faktory"
	"github.com/mperham/faktory/cli"
	"github.com/mperham/faktory/storage"
	"github.com/mperham/faktory/util"
	"github.com/mperham/gorocksdb"
)

func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger("warn")

	storage.DefaultPath = opts.StoragePath

	dir := fmt.Sprintf("%s.db", strings.Replace(opts.Binding, ":", "_", -1))

	store, err := storage.Open("rocksdb", dir)
	if err != nil {
		fmt.Println("Unable to open storage:", err.Error())
		os.Exit(-1)
	}
	go handleSignals(func() {
		store.Close()
		os.Exit(0)
	})

	repl(dir, store)
}

func repl(path string, store storage.Store) {
	fmt.Printf("Faktory %s, RocksDB %s\n", faktory.Version, gorocksdb.RocksDBVersion())
	prompt := fmt.Sprintf("%s/%s> ", storage.DefaultPath, path)

	rdr := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf(prompt)
		line, _, er := rdr.ReadLine()
		if er != nil {
			if io.EOF == er {
				fmt.Println("")
				store.Close()
				os.Exit(0)
			}
			fmt.Printf("Error: %s\n", er.Error())
			continue
		}
		cmd := string(line)
		var err error
		switch cmd {
		case "version":
			fmt.Printf("Faktory %s, RocksDB %s\n", faktory.Version, gorocksdb.RocksDBVersion())
		case "backup":
			err = store.Backup()
			if err == nil {
				fmt.Println("Backup created")
				store.EachBackup(func(x storage.BackupInfo) {
					fmt.Printf("%+v\n", x)
				})
			}
		case "restore":
			err = store.RestoreFromLatest()
			fmt.Println("Restoration complete, restart required")
			os.Exit(0)
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
		if err != nil {
			fmt.Println(err)
		}
	}
}

func handleSignals(fn func()) {
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGTERM)
	signal.Notify(signals, os.Interrupt)

	for {
		sig := <-signals
		util.Debugf("Received signal: %v", sig)
		fn()
	}
}
