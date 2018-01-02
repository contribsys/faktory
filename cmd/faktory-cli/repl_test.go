package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/gorocksdb"
	"github.com/stretchr/testify/assert"
)

var usingRockDB = "Faktory " + client.Version + ", using RocksDB " + gorocksdb.RocksDBVersion() + " at "

func setupTests() func(t *testing.T) {
	if _, err := os.Stat("faktory-cli-test"); err != nil {
		make := exec.Command("go", "build", "-o", "faktory-cli-test", "repl.go")

		err := make.Run()
		if err != nil {
			fmt.Printf("could not make binary for %s: %v", "faktory-cli-test", err)
			os.Exit(1)
		}
	}
	return func(t *testing.T) {
		os.Remove("faktory-cli-test")
	}
}

func teardown(storageName string) {
	os.RemoveAll("./" + storageName + "-data")
}

func runFaktory(storageName string, inputChan, cmdOutputChan chan string) {
	binaryPath := "./faktory-cli-test"
	cmd := exec.Command(binaryPath, "-d", "./"+storageName+"-data")

	cmdWriter, err := cmd.StdinPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating StdinPipe for %s %v\n", "faktory-cli-test", err)
		os.Exit(1)
	}

	go func() {
		defer cmdWriter.Close()
		io.WriteString(cmdWriter, <-inputChan)
	}()

	cmdOutReader, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating StdoutPipe for %s %v\n", "faktory-cli-test", err)
		os.Exit(1)
	}

	stdOutScanner := bufio.NewScanner(cmdOutReader)
	go func() {
		cmdOutput := ""
		for stdOutScanner.Scan() {
			cmdOutput += stdOutScanner.Text()
		}
		cmdOutputChan <- cmdOutput
	}()

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "There was an error starting %v, %v: ", binaryPath, err)
		os.Exit(1)
	}

	if err := cmd.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "There was an error waiting %v, %v: ", binaryPath, err)
		os.Exit(1)
	}
}

func TestInteractiveOutputs(t *testing.T) {
	clean := setupTests()
	defer clean(t)

	tests := []struct {
		Arg    string
		Output string
	}{
		{"flush", "OK"},
		{"purge", "OK"},
		{"version", "Faktory " + client.Version + ", RocksDB " + gorocksdb.RocksDBVersion()},
		{"help", "Valid commands:flush\t\t\tflush all job data from database, useful for testingbackup\t\t\tcreate a new backuppurge [keep]\t\tpurge old backups, keep [N] newest backups, default 24restore *\t\trestore the database from the newest backuprepair *\t\trun RocksDB's internal repair function to recover from data issuesversionhelp* Requires an immediate restart after running command."},
	}

	for _, ts := range tests {
		t.Run(ts.Arg, func(t *testing.T) {
			defer teardown(ts.Arg)
			inputChan := make(chan string)
			cmdOutputChan := make(chan string)
			go runFaktory(ts.Arg, inputChan, cmdOutputChan)
			inputChan <- ts.Arg

			expected := usingRockDB + "./" + ts.Arg + "-data" + ts.Output
			assert.Equal(t, expected, <-cmdOutputChan)
		})
	}

	t.Run("backup", func(t *testing.T) {
		storageName := "backup"
		defer teardown(storageName)

		inputChan := make(chan string)
		cmdOutputChan := make(chan string)
		go runFaktory(storageName, inputChan, cmdOutputChan)
		inputChan <- "backup"
		output := <-cmdOutputChan

		db, err := storage.Open("rocksdb", "./"+storageName+"-data")
		assert.NoError(t, err)

		bkp := ""
		db.EachBackup(func(bi storage.BackupInfo) {
			bkp = fmt.Sprintf("{Id:%d FileCount:%d Size:%d Timestamp:%d}", bi.Id, bi.FileCount, bi.Size, bi.Timestamp)
		})

		expected := usingRockDB + "./" + storageName + "-data"
		expected += "Backup created" + bkp
		assert.Equal(t, expected, output)
	})

	t.Run("restore", func(t *testing.T) {
		storageName := "restore"
		defer teardown(storageName)

		db, err := storage.Open("rocksdb", "./"+storageName+"-data")
		assert.NoError(t, err)

		err = db.Backup()
		assert.NoError(t, err)

		db.Close()

		inputChan := make(chan string)
		cmdOutputChan := make(chan string)
		go runFaktory(storageName, inputChan, cmdOutputChan)
		inputChan <- "restore"
		output := <-cmdOutputChan

		assert.Contains(t, output, "Restoration complete, restart required")
	})

	t.Run("repair", func(t *testing.T) {
		storageName := "restore"
		defer teardown(storageName)

		inputChan := make(chan string)
		cmdOutputChan := make(chan string)
		go runFaktory(storageName, inputChan, cmdOutputChan)
		inputChan <- "repair"
		output := <-cmdOutputChan

		assert.Contains(t, output, "Repair complete, restart required")
	})
}
