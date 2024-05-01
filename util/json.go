package util

import (
	"encoding/json"
)

func JsonUnmarshal(data []byte, target any) error {
	return json.Unmarshal(data, target)
}
