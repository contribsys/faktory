package faktory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientOperations(t *testing.T) {
	cl, err := Open()
	assert.Error(t, err)
	assert.Nil(t, cl)
}
