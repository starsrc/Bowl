package db_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

func TestNewStore(t *testing.T) {
	store := db.NewStore(nil)
	assert.NotNil(t, store)

	var _ db.Store = store
}

func TestModule(t *testing.T) {
	assert.NotNil(t, db.Module)
}
