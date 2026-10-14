package domain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-ledger/internal/domain"
)

func TestReserveStatusValid(t *testing.T) {
	assert.True(t, domain.ReserveStatusActive.Valid())
	assert.True(t, domain.ReserveStatusCommitted.Valid())
	assert.True(t, domain.ReserveStatusReleased.Valid())
	assert.False(t, domain.ReserveStatus("unknown").Valid())
}

func TestReserveStatusTerminal(t *testing.T) {
	assert.False(t, domain.ReserveStatusActive.Terminal())
	assert.True(t, domain.ReserveStatusCommitted.Terminal())
	assert.True(t, domain.ReserveStatusReleased.Terminal())
}

func TestValidateReserveAmount(t *testing.T) {
	require.NoError(t, domain.ValidateReserveAmount(1))
	require.NoError(t, domain.ValidateReserveAmount(1_000_000))

	err := domain.ValidateReserveAmount(0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidReserve))

	err = domain.ValidateReserveAmount(-1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, domain.ErrInvalidReserve))
}
