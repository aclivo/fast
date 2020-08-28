package fast_test

import (
	"testing"

	"github.com/aclivo/fast"
	"github.com/aclivo/olap"
	"github.com/aclivo/tests"
)

func TestStorage(t *testing.T) {
	factory := func() olap.Storage {
		return fast.NewStorage()
	}
	tests.StorageTestSuit(factory, t)
}
