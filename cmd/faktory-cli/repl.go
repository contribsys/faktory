package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	"github.com/chzyer/readline"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

const helpMsg = `Valid commands:

flush			flush all job data from database, useful for testing
backup			create a new backup
purge [keep]		purge old backups, keep [N] newest backups, default 24
restore *		restore the database from the newest backup
repair *		run RocksDB's internal repair function to recover from data issues
version
help

* Requires an immediate restart after running command.`

var versionMsg = fmt.Sprintf("Faktory %s, RocksDB %s\n", client.Version, gorocksdb.RocksDBVersion())

// The REPL provides a few admin commands outside of Faktory itself,
// notably the backup and restore commands.
func main() {
	opts := cli.ParseArguments()
	args := flag.Args()
	interactive := len(args) == 0

	// This takes over the default logger in `log` and gives us
	// extra powers for adding fields, errors to log output.
	util.InitLogger("warn")

	store, err := storage.Open("rocksdb", opts.StorageDirectory)
	if err != nil {
		fmt.Println("Unable to open storage:", err.Error())
	}

	if interactive {
		repl(opts.StorageDirectory, store)
		if store != nil {
			store.Close()
		}
	} else {
		go handleSignals(func() {
			if store != nil {
				store.Close()
			}
			os.Exit(0)
		})

		err := execute(args, store, opts.StorageDirectory)
		if err != nil {
			fmt.Println(err)
		}
		if store != nil {
			store.Close()
		}
		if err != nil {
			os.Exit(-1)
		}
	}
}

func repl(path string, store storage.Store) {
	fmt.Printf("Faktory %s, using RocksDB %s at %s\n", client.Version, gorocksdb.RocksDBVersion(), path)

	var completer = readline.NewPrefixCompleter(
		readline.PcItem("version"),
		readline.PcItem("flush"),
		readline.PcItem("backup"),
		readline.PcItem("restore"),
		readline.PcItem("repair"),
		readline.PcItem("purge"),
		readline.PcItem("exit"),
		readline.PcItem("help"),
	)

	l, err := readline.NewEx(&readline.Config{
		Prompt:          "> ",
		HistoryFile:     historyFilePath(),
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	log.SetOutput(l.Stderr())
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			fmt.Println("")
			break
		}

		line = strings.TrimSpace(line)
		cmd := strings.Split(line, " ")
		if cmd[0] == "exit" || cmd[0] == "quit" {
			break
		}

		err = execute(cmd, store, path)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func execute(cmd []string, store storage.Store, path string) error {
	first := cmd[0]
	switch first {
	case "exit":
		return nil
	case "quit":
		return nil
	case "version":
		fmt.Printf(versionMsg)
	case "help":
		fmt.Println(helpMsg)
	case "flush":
		return flush(store)
	case "backup":
		return backup(store)
	case "repair":
		return repair(store, path)
	case "purge":
		return purge(store, cmd[1:])
	case "restore":
		return restore(store)
	default:
		return fmt.Errorf("Unknown command: %v", cmd)
	}
	return nil
}

func flush(store storage.Store) error {
	if err := store.Flush(); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func backup(store storage.Store) error {
	if err := store.Backup(); err != nil {
		return err
	}
	fmt.Println("Backup created")
	store.EachBackup(func(x storage.BackupInfo) {
		fmt.Printf("%+v\n", x)
	})
	return nil
}

func repair(store storage.Store, path string) error {
	if store != nil {
		store.Close()
	}
	opts := storage.DefaultOptions()
	if err := gorocksdb.RepairDb(path, opts); err != nil {
		return err
	}

	fmt.Println("Repair complete, restart required")
	os.Exit(0)
	return nil
}

func purge(store storage.Store, args []string) error {
	count := storage.DefaultKeepBackupsCount
	if len(args) == 1 {
		val, err := strconv.Atoi(args[0])
		if err != nil {
			return err
		}
		count = val
	}
	if err := store.PurgeOldBackups(count); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func restore(store storage.Store) error {
	if err := store.RestoreFromLatest(); err != nil {
		return err
	}

	fmt.Println("Restoration complete, restart required")
	os.Exit(0)
	return nil
}

func handleSignals(fn func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	signal.Notify(signals, os.Interrupt)

	for {
		sig := <-signals
		util.Debugf("Received signal: %v", sig)
		fn()
	}
}

// historyFilePath returns the path of the history file
// $HOME/.local/.faktory-cli.history
// if the .local folder does not exists, it will create it.
func historyFilePath() string {
	usr, _ := user.Current()
	dir := usr.HomeDir + "/.local"
	historyFilePath := dir + "/.faktory-cli.history"

	exists, err := util.FileExists(historyFilePath)
	if err != nil {
		return ""
	}

	if !exists {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			util.Error("Unable to create $HOME/.local dir", err)
			return ""
		}
	}

	return historyFilePath
}
