package password

import (
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"math"
	"strings"

	"github.com/contribsys/faktory/util"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"
)

type hashID string

const (
	hashIDBCrypt2  hashID = "2" // technically a major ver only
	hashIDArgon2id hashID = "argon2id"
	hashIDScrypt   hashID = "scrypt"
	hashIDPBKDF2   hashID = "pbkdf2" // technically the first part of the identifier
)

// PasswordType interface describes the common way to verify a password for a
// given particular hashing/algorithm strategy
type PasswordType interface {
	Verify(candidate string) (bool, error)
}

type basePasswordType struct {
	Hashed string
}
type plainPasswordType struct {
	basePasswordType
}

func (p plainPasswordType) Verify(candidate string) (bool, error) {
	return subtle.ConstantTimeCompare([]byte(p.Hashed), []byte(candidate)) == 1, nil
}

type bcryptPasswordType struct {
	basePasswordType
}

func (p bcryptPasswordType) Verify(candidate string) (bool, error) {
	err := bcrypt.CompareHashAndPassword([]byte(p.Hashed), []byte(candidate))
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

type argon2idPasswordType struct {
	basePasswordType
	Version int
	Memory  uint32
	Time    uint32
	Threads uint8
	Salt    []byte
	Key     []byte
	KeyLen  int32
}

func (p argon2idPasswordType) Verify(candidate string) (bool, error) {
	candidateKey := argon2.IDKey([]byte(candidate),
		p.Salt,
		p.Time,
		p.Memory,
		p.Threads,
		uint32(p.KeyLen),
	)

	if subtle.ConstantTimeCompare(p.Key, candidateKey) == 1 {
		return true, nil
	}
	return false, nil
}

type scryptPasswordType struct {
	basePasswordType
	Salt        []byte
	LogCost     int
	BlockSize   int
	Parallelism int
	Key         []byte
	KeyLen      int
}

func (p scryptPasswordType) Verify(candidate string) (bool, error) {
	candidateKey, err := scrypt.Key(
		[]byte(candidate),
		p.Salt,
		int(math.Pow(2, float64(p.LogCost))),
		p.BlockSize,
		p.Parallelism,
		p.KeyLen,
	)

	if err != nil {
		return false, err
	}
	if subtle.ConstantTimeCompare(p.Key, candidateKey) == 1 {
		return true, nil
	}
	return false, nil
}

type pbkdf2PasswordType struct {
	basePasswordType
	DigestFunc func() hash.Hash
	Salt       []byte
	Rounds     int
	Key        []byte
	KeyLen     int
}

func (p pbkdf2PasswordType) Verify(candidate string) (bool, error) {
	candidateKey, err := pbkdf2.Key(
		p.DigestFunc,
		candidate,
		p.Salt,
		p.Rounds,
		p.KeyLen,
	)

	if err != nil {
		return false, err
	}
	if subtle.ConstantTimeCompare(p.Key, candidateKey) == 1 {
		return true, nil
	}
	return false, nil
}

// Verify returns true if a `candidate` password matches the `configured` one,
// which may or may not by hashed with different standardized hashing
// algorithms. If an algorithm cannot be detected, it is assumed the
// `configured` password is in plaintext.
//
// An error is returned when unable to decode the hash correctly or if the
// underlying hashing library returns an error during verification.
func Verify(candidate string, configured string) (bool, error) {
	algo, err := detectHashAlgorithm(configured)
	if err != nil {
		return false, err
	}
	return algo.Verify(candidate)
}

func detectHashAlgorithm(pwd string) (PasswordType, error) {
	// TODO: do a fulsome parsing of PHC format
	parts := strings.Split(pwd, "$")
	ppt := plainPasswordType{}
	ppt.Hashed = pwd
	if parts[0] != "" || len(parts) < 2 || len(parts[1]) < 1 {
		util.Warn("Not a recognizable password hash format, assuming plaintext")
		return ppt, nil
	}
	if hashID(parts[1][0]) == hashIDBCrypt2 {
		pt := bcryptPasswordType{}
		pt.Hashed = pwd
		return pt, nil
	}
	if hashID(parts[1]) == hashIDArgon2id {
		pt := argon2idPasswordType{}
		pt.Hashed = pwd
		_, err := fmt.Sscanf(parts[2], "v=%d", &pt.Version)
		if pt.Version != argon2.Version {
			return nil, fmt.Errorf("Password hash uses incompatible version of Argon2id (want %d, given %d)", argon2.Version, pt.Version)
		}
		_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &pt.Memory, &pt.Time, &pt.Threads)
		if err != nil {
			return nil, err
		}
		salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
		if err != nil {
			return nil, err
		}
		pt.Salt = salt
		key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
		if err != nil {
			return nil, err
		}
		pt.Key = key
		pt.KeyLen = int32(len(pt.Key))
		return pt, nil
	}
	if hashID(parts[1]) == hashIDScrypt {
		pt := scryptPasswordType{}
		pt.Hashed = pwd
		_, err := fmt.Sscanf(parts[2], "ln=%d,r=%d,p=%d", &pt.LogCost, &pt.BlockSize, &pt.Parallelism)
		if err != nil {
			return nil, err
		}
		salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[3])
		if err != nil {
			return nil, err
		}
		pt.Salt = salt

		key, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
		if err != nil {
			return nil, err
		}
		pt.Key = key
		pt.KeyLen = len(pt.Key)
		return pt, nil
	}
	if strings.HasPrefix(parts[1], string(hashIDPBKDF2)) {
		digestIdent, found := strings.CutPrefix(parts[1], string(hashIDPBKDF2))
		if !found {
			return nil, errors.New("Could not find digest algo from PBKDF2 hash")
		}
		pt := pbkdf2PasswordType{}
		pt.Hashed = pwd
		switch digestIdent {
		case "", "-sha1":
			pt.DigestFunc = sha1.New
		case "-sha256":
			pt.DigestFunc = sha256.New
		case "-sha512":
			pt.DigestFunc = sha512.New
		default:
			return nil, fmt.Errorf("Unsupported digest ID %s for PBKDF2 hash", digestIdent)
		}
		_, err := fmt.Sscanf(parts[2], "%d", &pt.Rounds)
		if err != nil {
			return nil, err
		}
		salt, err := ab64Decode(parts[3])
		if err != nil {
			return nil, err
		}
		pt.Salt = salt
		key, err := ab64Decode(parts[4])
		if err != nil {
			return nil, err
		}
		pt.Key = key
		pt.KeyLen = len(pt.Key)
		return pt, nil
	}
	util.Warnf("Unknown password hash algorithm ID %s, assuming plaintext", parts[1])
	return ppt, nil
}

// ab64Decode from strict and raw base64 format which omits padding &
// whitespace. Supports both custom ./ and normal +/ altchars.
func ab64Decode(s string) ([]byte, error) {
	b64s := strings.ReplaceAll(s, ".", "+")
	return base64.RawStdEncoding.Strict().DecodeString(b64s)
}
