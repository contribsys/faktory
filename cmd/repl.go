package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

/*
 The REPL provides a few admin commands outside of Faktory itself,
 noteably the backup and restore commands.
*/
func main() {
	opts := cli.ParseArguments()

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger("warn")

	store, err := storage.Open("rocksdb", opts.StorageDirectory)
	if err != nil {
		fmt.Println("Unable to open storage:", err.Error())
		fmt.Println(`Run "db repair" to attempt repair`)
	}
	go handleSignals(func() {
		if store != nil {
			store.Close()
		}
		os.Exit(0)
	})

	repl(opts.StorageDirectory, store)

	if store != nil {
		store.Close()
	}
}

func repl(path string, store storage.Store) {
	fmt.Printf("Using RocksDB %s at %s\n", gorocksdb.RocksDBVersion(), path)

	rdr := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		bytes, _, er := rdr.ReadLine()
		if er != nil {
			if io.EOF == er {
				fmt.Println("")
				if store != nil {
					store.Close()
				}
				os.Exit(0)
			}
			fmt.Printf("Error: %s\n", er.Error())
			continue
		}
		line := string(bytes)
		cmd := strings.Split(line, " ")
		first := cmd[0]
		var err error
		switch first {
		case "exit":
			return
		case "quit":
			return
		case "version":
			fmt.Printf("Faktory %s, RocksDB %s\n", faktory.Version, gorocksdb.RocksDBVersion())
		case "help":
			fmt.Println(`Valid commands:

flush
backup
restore *
repair *
version
help

* Requires an immediate restart after running command.
			`)
		default:
			err = db(store, path, line, first)
		}
		if err != nil {
			fmt.Println(err)
		}
	}
}

func db(store storage.Store, path string, line string, cmd string) error {
	switch cmd {
	case "flush":
		err := store.Flush()
		if err == nil {
			fmt.Println("OK")
		}
		return err
	case "backup":
		err := store.Backup()
		if err == nil {
			fmt.Println("Backup created")
			store.EachBackup(func(x storage.BackupInfo) {
				fmt.Printf("%+v\n", x)
			})
		}
		return err
	case "repair":
		if store != nil {
			store.Close()
		}
		opts := storage.DefaultOptions()
		err := gorocksdb.RepairDb(path, opts)
		if err == nil {
			fmt.Println("Repair complete, restart required")
			os.Exit(0)
		}
		return err
	case "restore":
		err := store.RestoreFromLatest()
		if err == nil {
			fmt.Println("Restoration complete, restart required")
			os.Exit(0)
		}
		return err
	default:
		return fmt.Errorf("Unknown command: %s", line)
	}
	return nil
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
