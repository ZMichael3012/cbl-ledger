package domain_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-ledger/internal/domain"
)

func TestValidateEntries(t *testing.T) {
	t.Parallel()

	valid := []domain.EntryInput{
		{AccountID: "a1", Direction: domain.DirectionDebit, Amount: 10000},
		{AccountID: "a2", Direction: domain.DirectionCredit, Amount: 10000},
	}
	require.NoError(t, domain.ValidateEntries(valid))

	tests := []struct {
		name    string
		entries []domain.EntryInput
		wantErr error
	}{
		{
			name:    "single entry",
			entries: []domain.EntryInput{{AccountID: "a1", Direction: domain.DirectionDebit, Amount: 1}},
			wantErr: domain.ErrInvalidTransaction,
		},
		{
			name: "empty account",
			entries: []domain.EntryInput{
				{AccountID: " ", Direction: domain.DirectionDebit, Amount: 1},
				{AccountID: "a2", Direction: domain.DirectionCredit, Amount: 1},
			},
			wantErr: domain.ErrInvalidEntry,
		},
		{
			name: "unknown direction",
			entries: []domain.EntryInput{
				{AccountID: "a1", Direction: domain.Direction("hold"), Amount: 1},
				{AccountID: "a2", Direction: domain.DirectionCredit, Amount: 1},
			},
			wantErr: domain.ErrInvalidEntry,
		},
		{
			name: "zero amount",
			entries: []domain.EntryInput{
				{AccountID: "a1", Direction: domain.DirectionDebit, Amount: 0},
				{AccountID: "a2", Direction: domain.DirectionCredit, Amount: 0},
			},
			wantErr: domain.ErrInvalidEntry,
		},
		{
			name: "negative amount",
			entries: []domain.EntryInput{
				{AccountID: "a1", Direction: domain.DirectionDebit, Amount: -5},
				{AccountID: "a2", Direction: domain.DirectionCredit, Amount: -5},
			},
			wantErr: domain.ErrInvalidEntry,
		},
		{
			name: "unbalanced",
			entries: []domain.EntryInput{
				{AccountID: "a1", Direction: domain.DirectionDebit, Amount: 100},
				{AccountID: "a2", Direction: domain.DirectionCredit, Amount: 99},
			},
			wantErr: domain.ErrUnbalanced,
		},
		{
			name: "debit overflow",
			entries: []domain.EntryInput{
				{AccountID: "a1", Direction: domain.DirectionDebit, Amount: math.MaxInt64},
				{AccountID: "a2", Direction: domain.DirectionDebit, Amount: 1},
				{AccountID: "a3", Direction: domain.DirectionCredit, Amount: 1},
				{AccountID: "a4", Direction: domain.DirectionCredit, Amount: 1},
			},
			wantErr: domain.ErrInvalidEntry,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, domain.ValidateEntries(tt.entries), tt.wantErr)
		})
	}
}

func TestValidateEntriesLimit(t *testing.T) {
	t.Parallel()

	many := make([]domain.EntryInput, domain.MaxEntries+1)
	for i := range many {
		many[i] = domain.EntryInput{AccountID: "a", Direction: domain.DirectionDebit, Amount: 1}
	}
	require.ErrorIs(t, domain.ValidateEntries(many), domain.ErrInvalidTransaction)
}

func TestDirectionValid(t *testing.T) {
	t.Parallel()

	require.True(t, domain.DirectionDebit.Valid())
	require.True(t, domain.DirectionCredit.Valid())
	require.False(t, domain.Direction("x").Valid())
}

func TestAccountTypeValid(t *testing.T) {
	t.Parallel()

	require.True(t, domain.AccountTypeMerchant.Valid())
	require.True(t, domain.AccountTypeSuspense.Valid())
	require.False(t, domain.AccountType("cash").Valid())
}
