package password

import (
	"testing"
)

// PHC-formatted hashes produced using Python's `passlib` library:
//
//	from passlib import hash
//	hash.bcrypt.hash("a") # => "$2b$12$xbQjW9Gtc35jLaAnGp7iV.PMoDuu0SdfWUrv30B6NT1vTrGc4LTPW"
//	hash.scrypt.hash("a") # => "$scrypt$ln=16,r=8,p=1$AaAU4tybc06JUcoZo1RK6Q$xsBBX09sq48k30ngJqoUepwYM3T/HJn9K7eYzgIyNbw"
func TestPasswordVerify(t *testing.T) {
	tests :=
		[]struct {
			name       string
			candidate  string
			configured string
			verified   bool
			expectErr  bool
			errMessage string
		}{
			{
				name:       "bcrypt correct",
				candidate:  "a",
				configured: "$2b$12$xbQjW9Gtc35jLaAnGp7iV.PMoDuu0SdfWUrv30B6NT1vTrGc4LTPW",
				verified:   true,
			},
			{
				name:       "bcrypt incorrect",
				candidate:  "wrong",
				configured: "$2b$12$xbQjW9Gtc35jLaAnGp7iV.PMoDuu0SdfWUrv30B6NT1vTrGc4LTPW",
				verified:   false,
				expectErr:  true,
				errMessage: "crypto/bcrypt: hashedPassword is not the hash of the given password",
			},
			{
				name:       "argon2id correct",
				candidate:  "a",
				configured: "$argon2id$v=19$m=65536,t=3,p=4$VSrlfE9pLcW4977XGiOklA$GIIN05JoObiRMLBpP+iPBHeemyovXJvM4Zi2JU82XFg",
				verified:   true,
			},
			{
				name:       "argon2id incorrect",
				candidate:  "wrong",
				configured: "$argon2id$v=19$m=65536,t=3,p=4$VSrlfE9pLcW4977XGiOklA$GIIN05JoObiRMLBpP+iPBHeemyovXJvM4Zi2JU82XFg",
				verified:   false,
			},
			{
				name:       "scrypt correct",
				candidate:  "a",
				configured: "$scrypt$ln=16,r=8,p=1$AaAU4tybc06JUcoZo1RK6Q$xsBBX09sq48k30ngJqoUepwYM3T/HJn9K7eYzgIyNbw",
				verified:   true,
			},
			{
				name:       "scrypt incorrect",
				candidate:  "wrong",
				configured: "$scrypt$ln=16,r=8,p=1$AaAU4tybc06JUcoZo1RK6Q$xsBBX09sq48k30ngJqoUepwYM3T/HJn9K7eYzgIyNbw",
				verified:   false,
			},
			{
				name:       "pbkdf2-hmac-sha1 correct",
				candidate:  "a",
				configured: "$pbkdf2$131000$j3FurVUqxbiXUuqdc865Fw$khjQ9RJHk0901AZmqtUnudHQmDg",
				verified:   true,
			},
			{
				name:       "pbkdf2-hmac-sha1 incorrect",
				candidate:  "wrong",
				configured: "$pbkdf2$131000$j3FurVUqxbiXUuqdc865Fw$khjQ9RJHk0901AZmqtUnudHQmDg",
				verified:   false,
			},
			{
				name:       "pbkdf2-hmac-sha256 correct",
				candidate:  "a",
				configured: "$pbkdf2-sha256$29000$EqJUqlXqfa/13hsDYGyNsQ$mySn2pP1vbxyIA2/ExJqoHDc0ywnwf4SSJPavT6n3oA",
				verified:   true,
			},
			{
				name:       "pbkdf2-hmac-sha256 incorrect",
				candidate:  "wrong",
				configured: "$pbkdf2-sha256$29000$EqJUqlXqfa/13hsDYGyNsQ$mySn2pP1vbxyIA2/ExJqoHDc0ywnwf4SSJPavT6n3oA",
				verified:   false,
			},
			{
				name:       "pbkdf2-hmac-sha512 correct",
				candidate:  "a",
				configured: "$pbkdf2-sha512$25000$QmhtjZEyJuR8r3UOoVRKaQ$EiqzPjoOZkEt3SKVZv9g31/kaj8WXIaey5pNWWVczZrJXXeuA9CU.vlJ3AgYS6CqojXtpgC1P0kJwkevKDMqMw",
				verified:   true,
			},
			{
				name:       "pbkdf2-hmac-sha512 incorrect",
				candidate:  "wrong",
				configured: "$pbkdf2-sha512$25000$QmhtjZEyJuR8r3UOoVRKaQ$EiqzPjoOZkEt3SKVZv9g31/kaj8WXIaey5pNWWVczZrJXXeuA9CU.vlJ3AgYS6CqojXtpgC1P0kJwkevKDMqMw",
				verified:   false,
			},
			{
				name:       "plaintext correct",
				candidate:  "a",
				configured: "a",
				verified:   true,
			},
			{
				name:       "plaintext incorrect",
				candidate:  "wrong",
				configured: "a",
				verified:   false,
			},
			{
				name:       "PHC-ish looking plaintext",
				candidate:  "$bcrypt$tography",
				configured: "$bcrypt$tography",
				verified:   true,
			},
		}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := Verify(tt.candidate, tt.configured)

			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if err.Error() != tt.errMessage {
					t.Errorf("expected error %q, got %q", tt.errMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("did not expect error, got %q", err.Error())
				}
				if v != tt.verified {
					t.Errorf("Verify(\"%s\", \"%s\") = %v; want %v", tt.candidate, tt.configured, v, tt.verified)
				}
			}
		})
	}
}
