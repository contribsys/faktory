package util

import (
	"bytes"
	"encoding/json"
)

var (
	// If true, activates encoding/json's Decoder and its UseNumber()
	// option to preserve number precision.
	// Defaults to false in Faktory 1.x.
	// Will default to true in Faktory 2.x
	JsonUseNumber bool = false
)

func JsonUnmarshal(data []byte, target any) error {
	if !JsonUseNumber {
		return json.Unmarshal(data, target)
	}

	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	return dec.Decode(target)
}
