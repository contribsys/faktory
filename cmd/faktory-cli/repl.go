package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"net"
	"math/big"

	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"crypto/x509/pkix"
	"crypto/rand"
	"encoding/pem"

	"github.com/contribsys/faktory"
	"github.com/contribsys/faktory/cli"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/contribsys/gorocksdb"
)

const helpMsg = `Valid commands:

flush
backup
restore *
repair *
generate-certificate <hostname, ..>
version
help

* Requires an immediate restart after running command.`

var versionMsg = fmt.Sprintf("Faktory %s, RocksDB %s\n", faktory.Version, gorocksdb.RocksDBVersion())

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
		fmt.Println(`Run "db repair" to attempt repair`)
	}

	if interactive {
		repl(opts, store)
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

		err := execute(args, store, opts)
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

func repl(opts cli.CmdOptions, store storage.Store) {
	fmt.Printf("Using RocksDB %s at %s\n", gorocksdb.RocksDBVersion(), opts.StorageDirectory)

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
		err := execute(cmd, store, opts)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func execute(cmd []string, store storage.Store, opts cli.CmdOptions) error {
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
		return repair(store, opts.StorageDirectory)
	case "purge":
		return purge(store)
	case "restore":
		return restore(store)
	case "generate-certificate":
		hostnames := cmd[1:]
		tlspath := opts.ConfigDirectory + "/tls"
		if len(hostnames) == 0 {
			hostnames = []string{"*"}
		}
		return generate(tlspath, hostnames)
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

func purge(store storage.Store) error {
	if err := store.PurgeOldBackups(storage.DefaultKeepBackupsCount); err != nil {
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

func generate(path string, hostnames []string) error {
	cert := path + "/public.crt"
	private := path + "/private.key"

	ok, _ := util.FileExists(cert)
	if ok {
		return fmt.Errorf("public.crt already exists in %v, will not create a new one", path)
	}

	ok, _ = util.FileExists(private)
	if ok {
		return fmt.Errorf("private.key already exists in %v, will not create a new one", path)
	}

	if len(hostnames) == 0 {
		return fmt.Errorf("no hostnames supplied")
	}

	// make sure the tls folder exists
	err := os.MkdirAll(path, os.ModeDir|0755)
	if err != nil {
		return fmt.Errorf("unable to create folder %v: %v", path, err)
	}
	fmt.Printf("Generating a certificate and key in: %v\n", path)

	// generate a private key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %v", err)
	}

	// generate a certificate
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Faktory"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA: true,
	}

	// add all the user requested hostnames to the certificate
	for _, h := range hostnames {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return fmt.Errorf("failed to create certificate: %v", err)
	}

	f, err := os.Create(cert)
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %v", err)
	}

	pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	f.Close()

	f, err = os.OpenFile(private, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open key.pem for writing: %v", err)
	}
	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("unable to marshal ECDSA private key: %v", err)
	}

	pem.Encode(f, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b})
	f.Close()

	fmt.Printf("A self signed certificate and private key have been generated in: %s\n", path)

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
