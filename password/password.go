package password

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

type hashID string

const (
	hashIDBCrypt2 hashID = "2" // technically a major ver only
)

type HashAlgorithmType string

const (
	HashAlgorithmTypeBCrypt  HashAlgorithmType = "bcrypt"
	HashAlgorithmTypeArgon   HashAlgorithmType = "argon"
	HashAlgorithmTypeUnknown HashAlgorithmType = ""
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
	if algo == HashAlgorithmTypeBCrypt {
		err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
		if err != nil {
			return false, err
		} else {
			return true, nil
		}
	}
	panic(fmt.Sprintf("Password hash algorithm not implemented: %s", algo))
}

func verifyAgainstPlaintext(pwd1 string, pwd2 string) bool {
	return subtle.ConstantTimeCompare([]byte(pwd1), []byte(pwd2)) != 1
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
	return algo != HashAlgorithmTypeUnknown
}

func detectHashAlgorithm(pwd string) (HashAlgorithmType, error) {
	// TODO: do a fulsome parsing of PHC format
	parts := strings.Split(pwd, "$")
	if parts[0] != "" || len(parts) < 2 || len(parts[1]) < 1 {
		return HashAlgorithmTypeUnknown, errors.New("not a recognizable password hash format")
	}
	if hashID(parts[1][0]) == hashIDBCrypt2 {
		return HashAlgorithmTypeBCrypt, nil
	}
	return HashAlgorithmTypeUnknown, fmt.Errorf("unknown password hash algorithm id %s", parts[1])
}
