package grpcserver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-ledger/internal/domain"
)

func TestMapError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want grpcmw.Kind
	}{
		{name: "idempotency reuse", err: domain.ErrIdempotencyReuse, want: grpcmw.KindAlreadyExists},
		{name: "unknown account", err: domain.ErrUnknownAccount, want: grpcmw.KindNotFound},
		{name: "insufficient funds", err: domain.ErrInsufficientFunds, want: grpcmw.KindFailedPrecondition},
		{name: "unbalanced", err: domain.ErrUnbalanced, want: grpcmw.KindInvalidArgument},
		{name: "invalid entry", err: domain.ErrInvalidEntry, want: grpcmw.KindInvalidArgument},
		{name: "invalid transaction", err: domain.ErrInvalidTransaction, want: grpcmw.KindInvalidArgument},
		{name: "asset mismatch", err: domain.ErrAssetMismatch, want: grpcmw.KindInvalidArgument},
		{name: "invalid reserve", err: domain.ErrInvalidReserve, want: grpcmw.KindInvalidArgument},
		{name: "unknown reserve", err: domain.ErrUnknownReserve, want: grpcmw.KindNotFound},
		{name: "reserve state", err: domain.ErrReserveState, want: grpcmw.KindFailedPrecondition},
		{name: "reserve not allowed", err: domain.ErrReserveNotAllowed, want: grpcmw.KindFailedPrecondition},
		{name: "conflict", err: domain.ErrConflict, want: grpcmw.KindResourceExhausted},
		{name: "unknown", err: errors.New("boom"), want: grpcmw.KindInternal},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mapped := mapError(tt.err)
			assert.Equal(t, tt.want, grpcmw.KindOf(mapped))
		})
	}
}

func TestMapDirection(t *testing.T) {
	t.Parallel()

	debit, err := mapDirection(commonv1.Direction_DIRECTION_DEBIT)
	require.NoError(t, err)
	assert.Equal(t, domain.DirectionDebit, debit)

	credit, err := mapDirection(commonv1.Direction_DIRECTION_CREDIT)
	require.NoError(t, err)
	assert.Equal(t, domain.DirectionCredit, credit)

	_, err = mapDirection(commonv1.Direction_DIRECTION_UNSPECIFIED)
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
}

func validEntry() *ledgerv1.EntryInput {
	return &ledgerv1.EntryInput{
		AccountId: uuid.Must(uuid.NewV7()).String(),
		Direction: commonv1.Direction_DIRECTION_DEBIT,
		Amount:    &commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 2},
	}
}

func TestReserveValidation(t *testing.T) {
	t.Parallel()

	server := NewLedger(nil)
	ctx := context.Background()
	accountID := uuid.Must(uuid.NewV7()).String()
	money := &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2}

	t.Run("missing idempotency key", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{AccountId: accountID, Amount: money, RefId: "r-1"})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("invalid account id", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{IdempotencyKey: "k", AccountId: "nope", Amount: money, RefId: "r-1"})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("missing amount", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{IdempotencyKey: "k", AccountId: accountID, RefId: "r-1"})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("non-positive amount", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{
			IdempotencyKey: "k", AccountId: accountID, RefId: "r-1",
			Amount: &commonv1.Money{MinorUnits: 0, Asset: "USDT", Decimals: 2},
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("missing ref id", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{IdempotencyKey: "k", AccountId: accountID, Amount: money})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("ref too long", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{
			IdempotencyKey: "k", AccountId: accountID, Amount: money,
			RefId: strings.Repeat("r", maxRefFieldLen+1),
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("asset too long", func(t *testing.T) {
		_, err := server.Reserve(ctx, &ledgerv1.ReserveRequest{
			IdempotencyKey: "k", AccountId: accountID, RefId: "r-1",
			Amount: &commonv1.Money{MinorUnits: 100, Asset: strings.Repeat("A", maxAssetLen+1), Decimals: 2},
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})
}

func TestCommitReserveValidation(t *testing.T) {
	t.Parallel()

	server := NewLedger(nil)
	ctx := context.Background()

	t.Run("missing idempotency key", func(t *testing.T) {
		_, err := server.CommitReserve(ctx, &ledgerv1.CommitReserveRequest{ReserveId: uuid.Must(uuid.NewV7()).String()})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("invalid reserve id", func(t *testing.T) {
		_, err := server.CommitReserve(ctx, &ledgerv1.CommitReserveRequest{IdempotencyKey: "k", ReserveId: "nope"})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})
}

func TestReleaseReserveValidation(t *testing.T) {
	t.Parallel()

	server := NewLedger(nil)
	ctx := context.Background()

	t.Run("reason too long", func(t *testing.T) {
		_, err := server.ReleaseReserve(ctx, &ledgerv1.ReleaseReserveRequest{
			IdempotencyKey: "k",
			ReserveId:      uuid.Must(uuid.NewV7()).String(),
			Reason:         strings.Repeat("r", maxReasonLen+1),
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("invalid reserve id", func(t *testing.T) {
		_, err := server.ReleaseReserve(ctx, &ledgerv1.ReleaseReserveRequest{IdempotencyKey: "k", ReserveId: "nope"})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})
}

func TestMapReserveStatus(t *testing.T) {
	t.Parallel()

	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_ACTIVE, mapReserveStatus(domain.ReserveStatusActive))
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_COMMITTED, mapReserveStatus(domain.ReserveStatusCommitted))
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_RELEASED, mapReserveStatus(domain.ReserveStatusReleased))
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_UNSPECIFIED, mapReserveStatus(domain.ReserveStatus("unknown")))
}

func TestPostTransactionValidation(t *testing.T) {
	t.Parallel()

	server := NewLedger(nil)
	ctx := context.Background()

	t.Run("missing idempotency key", func(t *testing.T) {
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("idempotency key too long", func(t *testing.T) {
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: strings.Repeat("k", maxIdempotencyKeyLen+1),
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("ref too long", func(t *testing.T) {
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: "key",
			RefType:        strings.Repeat("r", maxRefFieldLen+1),
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("missing amount", func(t *testing.T) {
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: "key",
			Entries:        []*ledgerv1.EntryInput{{AccountId: uuid.Must(uuid.NewV7()).String()}},
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("invalid account id", func(t *testing.T) {
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: "key",
			Entries: []*ledgerv1.EntryInput{{
				AccountId: "not-a-uuid",
				Direction: commonv1.Direction_DIRECTION_DEBIT,
				Amount:    &commonv1.Money{MinorUnits: 1},
			}},
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("unknown direction", func(t *testing.T) {
		entry := validEntry()
		entry.Direction = commonv1.Direction_DIRECTION_UNSPECIFIED
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: "key",
			Entries:        []*ledgerv1.EntryInput{entry},
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})

	t.Run("too many entries", func(t *testing.T) {
		entries := make([]*ledgerv1.EntryInput, domain.MaxEntries+1)
		for i := range entries {
			entries[i] = validEntry()
		}
		_, err := server.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
			IdempotencyKey: "key",
			Entries:        entries,
		})
		require.Error(t, err)
		assert.Equal(t, grpcmw.KindInvalidArgument, grpcmw.KindOf(err))
	})
}
