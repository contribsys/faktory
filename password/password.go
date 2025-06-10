package password

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
)

type hashID string

const (
	hashIDBCrypt2  hashID = "2" // technically a major ver only
	hashIDArgon2id hashID = "argon2id"
)

type hashAlgorithmType string

const (
	hashAlgorithmTypeBCrypt   hashAlgorithmType = "bcrypt"
	hashAlgorithmTypeArgon2id hashAlgorithmType = "argon2id"
	hashAlgorithmTypeUnknown  hashAlgorithmType = ""
)

func Verify(candidate string, configured string) (bool, error) {
	if isSupportedPasswordHash(configured) {
		return verifyAgainstHash(candidate, configured)
	} else {
		return verifyAgainstPlaintext(candidate, configured), nil
	}
}

func verifyAgainstHash(password string, hashedPassword string) (bool, error) {
	algo, err := detectHashAlgorithm(hashedPassword)
	if err != nil {
		return false, err
	}
	if algo == hashAlgorithmTypeBCrypt {
		err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
		if err != nil {
			return false, err
		} else {
			return true, nil
		}
	} else if algo == hashAlgorithmTypeArgon2id {
		var ver int
		parts := strings.Split(hashedPassword, "$")
		_, err = fmt.Sscanf(parts[2], "v=%d", &ver)
		if ver != argon2.Version {
			return false, fmt.Errorf("Password hash uses incompatible version of Argon2id (want %d, given %d)", argon2.Version, ver)
		}
		// TODO: These are technically optional. Use defaults if absent
		var mem uint32
		var iter uint32
		var para uint8
		_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &mem, &iter, &para)
		if err != nil {
			return false, err
		}
		salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
		if err != nil {
			return false, err
		}
		key, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
		if err != nil {
			return false, err
		}
		keylen := int32(len(key))
		candidateKey := argon2.IDKey([]byte(password), salt, iter, mem, para, uint32(keylen))
		candidateKeylen := int32(len(candidateKey))

		if subtle.ConstantTimeEq(keylen, candidateKeylen) == 0 {
			return false, nil
		}
		if subtle.ConstantTimeCompare(key, candidateKey) == 1 {
			return true, nil
		}
		return false, nil
	}
	panic(fmt.Sprintf("Password hash algorithm not implemented: %s", algo))
}

func verifyAgainstPlaintext(pwd1 string, pwd2 string) bool {
	return subtle.ConstantTimeCompare([]byte(pwd1), []byte(pwd2)) == 1
}

// isSupportedPasswordHash returns true if the supplied password is actually a
// hash as defined by the PHC format that Faktory supports. Per OWASP guidance,
// only Argon2id, scrypt, bcrypt, and PBKDF2 hashes are supported.
//
// https://github.com/P-H-C/phc-string-format
// https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html
func isSupportedPasswordHash(pwd string) bool {
	algo, err := detectHashAlgorithm(pwd)
	if err != nil {
		return false
	}
	return algo != hashAlgorithmTypeUnknown
}

// TODO: return a genericish struct/interface or something so we don't
// wastefully keep parsing the string over and over
func detectHashAlgorithm(pwd string) (hashAlgorithmType, error) {
	// TODO: do a fulsome parsing of PHC format
	parts := strings.Split(pwd, "$")
	if parts[0] != "" || len(parts) < 2 || len(parts[1]) < 1 {
		return hashAlgorithmTypeUnknown, errors.New("not a recognizable password hash format")
	}
	if hashID(parts[1][0]) == hashIDBCrypt2 {
		return hashAlgorithmTypeBCrypt, nil
	}
	if hashID(parts[1]) == hashIDArgon2id {
		return hashAlgorithmTypeArgon2id, nil
	}
	return hashAlgorithmTypeUnknown, fmt.Errorf("unknown password hash algorithm id %s", parts[1])
}
