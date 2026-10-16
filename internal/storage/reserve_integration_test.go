//go:build integration

package storage_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func reserveInput(accountID string, amount int64, key, refID string) storage.ReserveInput {
	return storage.ReserveInput{
		IdempotencyKey: key,
		AccountID:      accountID,
		Asset:          "USDT",
		Decimals:       2,
		Amount:         amount,
		RefID:          refID,
	}
}

func TestIntegrationReserveCreatesHold(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)

	result, err := repo.Reserve(context.Background(), reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	assert.NotEmpty(t, result.ReserveID)
	assert.Equal(t, domain.ReserveStatusActive, result.Status)
	assert.False(t, result.Replayed)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(60_000), balance.Available)
	assert.Equal(t, int64(40_000), balance.Reserved)

	var reserve storage.Reserve
	require.NoError(t, db.Where("id = ?", result.ReserveID).First(&reserve).Error)
	assert.Equal(t, merchant.ID, reserve.AccountID)
	assert.Equal(t, int64(40_000), reserve.Amount)
	assert.Equal(t, "payout-1", reserve.RefID)
	assert.Equal(t, "active", reserve.Status)
	assert.Nil(t, reserve.TransactionID)

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Zero(t, entries, "a hold creates no entries")
}

func TestIntegrationReserveReplayAndReuse(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	first, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	replay, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, first.ReserveID, replay.ReserveID)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(60_000), balance.Available)
	assert.Equal(t, int64(40_000), balance.Reserved, "a key repeat does not hold twice")

	_, err = repo.Reserve(ctx, reserveInput(merchant.ID, 30_000, "payout-1:reserve", "payout-1"))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrIdempotencyReuse)

	second, err := repo.Reserve(ctx, reserveInput(merchant.ID, 10_000, "payout-2:reserve", "payout-2"))
	require.NoError(t, err)
	assert.NotEqual(t, first.ReserveID, second.ReserveID)
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(50_000), balance.Available)
	assert.Equal(t, int64(50_000), balance.Reserved)
}

func TestIntegrationReserveRejections(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 10_000)
	clearing := seedAccount(t, db, "clearing", "USDT", 2, "", 10_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 10_000)
	suspense := seedAccount(t, db, "suspense", "USDT", 2, "", 10_000)
	payout := seedAccount(t, db, "payout", "USDT", 2, "m-1", 10_000)
	nameless := seedAccount(t, db, "merchant", "USDT", 2, "", 10_000)
	ctx := context.Background()

	tests := []struct {
		name  string
		input storage.ReserveInput
		want  error
	}{
		{"unknown account", reserveInput("018f0000-0000-7000-8000-000000000000", 100, "k-1", "r-1"), domain.ErrUnknownAccount},
		{"clearing not allowed", reserveInput(clearing.ID, 100, "k-2", "r-1"), domain.ErrReserveNotAllowed},
		{"fee not allowed", reserveInput(fee.ID, 100, "k-2a", "r-1"), domain.ErrReserveNotAllowed},
		{"suspense not allowed", reserveInput(suspense.ID, 100, "k-2b", "r-1"), domain.ErrReserveNotAllowed},
		{"payout not allowed", reserveInput(payout.ID, 100, "k-2c", "r-1"), domain.ErrReserveNotAllowed},
		{"merchant without ref", reserveInput(nameless.ID, 100, "k-2d", "r-1"), domain.ErrReserveNotAllowed},
		{"asset mismatch", storage.ReserveInput{IdempotencyKey: "k-3", AccountID: merchant.ID, Asset: "BTC", Decimals: 8, Amount: 100, RefID: "r-1"}, domain.ErrAssetMismatch},
		{"insufficient funds", reserveInput(merchant.ID, 10_001, "k-4", "r-1"), domain.ErrInsufficientFunds},
		{"zero amount", reserveInput(merchant.ID, 0, "k-5", "r-1"), domain.ErrInvalidReserve},
		{"negative amount", reserveInput(merchant.ID, -1, "k-5a", "r-1"), domain.ErrInvalidReserve},
		{"empty key", reserveInput(merchant.ID, 100, " ", "r-1"), domain.ErrInvalidReserve},
		{"empty ref", reserveInput(merchant.ID, 100, "k-6", " "), domain.ErrInvalidReserve},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.Reserve(ctx, tt.input)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.want)
		})
	}

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(10_000), balance.Available, "rejections do not change the balance")
	assert.Zero(t, balance.Reserved)
}

func TestIntegrationReserveConcurrentOversubscribe(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)

	const workers = 10
	const amount = 30_000
	var wg sync.WaitGroup
	successes := make([]bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := repo.Reserve(context.Background(),
				reserveInput(merchant.ID, amount, "concurrent-reserve-"+string(rune('a'+idx)), "payout-c"))
			successes[idx] = err == nil
		}(i)
	}
	wg.Wait()

	okCount := 0
	for _, ok := range successes {
		if ok {
			okCount++
		}
	}
	assert.GreaterOrEqual(t, okCount, 1, "with available balance at least one hold succeeds")
	assert.LessOrEqual(t, okCount, 3, "cannot reserve more than available")

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.GreaterOrEqual(t, balance.Available, int64(0))
	assert.Equal(t, int64(100_000)-int64(okCount)*amount, balance.Available)
	assert.Equal(t, int64(okCount)*amount, balance.Reserved)
}

func TestIntegrationCommitReserve(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	result, err := repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.NoError(t, err)
	assert.Equal(t, reserve.ReserveID, result.ReserveID)
	assert.Equal(t, domain.ReserveStatusCommitted, result.Status)
	assert.False(t, result.Replayed)

	var merchantBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&merchantBalance).Error)
	assert.Equal(t, int64(60_000), merchantBalance.Available, "a commit does not touch the source available")
	assert.Zero(t, merchantBalance.Reserved, "the hold is removed")

	var payout storage.Account
	require.NoError(t, db.Where("type = ? AND asset = ?", "payout", "USDT").First(&payout).Error)
	assert.Equal(t, "m-1", *payout.MerchantRef)
	derivedPayout, err := domain.DeriveAccountID(domain.PayoutAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, derivedPayout.String(), payout.ID, "the payout account is created per the account plan (ADR-2.6)")
	var payoutBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", payout.ID).First(&payoutBalance).Error)
	assert.Equal(t, int64(40_000), payoutBalance.Available)

	var stored storage.Reserve
	require.NoError(t, db.Where("id = ?", reserve.ReserveID).First(&stored).Error)
	assert.Equal(t, "committed", stored.Status)
	require.NotNil(t, stored.TransactionID)
	assert.Equal(t, payout.ID, *stored.CounterpartyAccountID)

	var transaction storage.Transaction
	require.NoError(t, db.Where("id = ?", *stored.TransactionID).First(&transaction).Error)
	assert.Equal(t, "reserve", transaction.RefType)
	assert.Equal(t, "payout-1", transaction.RefID)

	var entries []storage.Entry
	require.NoError(t, db.Where("transaction_id = ?", transaction.ID).Order("direction").Find(&entries).Error)
	require.Len(t, entries, 2)
	assert.Equal(t, "credit", entries[0].Direction)
	assert.Equal(t, payout.ID, entries[0].AccountID)
	assert.Equal(t, "debit", entries[1].Direction)
	assert.Equal(t, merchant.ID, entries[1].AccountID)
	assert.Equal(t, int64(40_000), entries[0].Amount)
	assert.Equal(t, entries[1].Amount, entries[0].Amount)

	var transactionCount int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactionCount).Error)
	assert.Equal(t, int64(1), transactionCount)
}

func TestIntegrationCommitReserveIdempotency(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	first, err := repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.NoError(t, err)
	require.Equal(t, domain.ReserveStatusCommitted, first.Status)

	replay, err := repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, domain.ReserveStatusCommitted, replay.Status)

	stateRepeat, err := repo.CommitReserve(ctx, "payout-1:commit-again", reserve.ReserveID)
	require.NoError(t, err)
	assert.Equal(t, domain.ReserveStatusCommitted, stateRepeat.Status)
	assert.False(t, stateRepeat.Replayed)

	var transactionCount int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactionCount).Error)
	assert.Equal(t, int64(1), transactionCount, "repeats create no transactions")

	var entriesCount int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entriesCount).Error)
	assert.Equal(t, int64(2), entriesCount)

	_, err = repo.CommitReserve(ctx, "payout-1:commit", "018f0000-0000-7000-8000-000000000001")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrIdempotencyReuse)

	_, err = repo.CommitReserve(ctx, "unknown-key", "018f0000-0000-7000-8000-000000000002")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownReserve)
}

func TestIntegrationCommitReleasedReserveRejected(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	_, err = repo.ReleaseReserve(ctx, "payout-1:release", reserve.ReserveID, "cancelled")
	require.NoError(t, err)

	_, err = repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrReserveState)
}

func TestIntegrationReleaseReserve(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	result, err := repo.ReleaseReserve(ctx, "payout-1:release", reserve.ReserveID, "gateway timeout")
	require.NoError(t, err)
	assert.Equal(t, domain.ReserveStatusReleased, result.Status)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(100_000), balance.Available, "funds are returned")
	assert.Zero(t, balance.Reserved)

	var stored storage.Reserve
	require.NoError(t, db.Where("id = ?", reserve.ReserveID).First(&stored).Error)
	assert.Equal(t, "released", stored.Status)
	assert.Equal(t, "gateway timeout", stored.Reason)

	replay, err := repo.ReleaseReserve(ctx, "payout-1:release", reserve.ReserveID, "another reason")
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, domain.ReserveStatusReleased, replay.Status)

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Zero(t, entries, "a hold release creates no entries")

	stateRepeat, err := repo.ReleaseReserve(ctx, "payout-1:release-again", reserve.ReserveID, "")
	require.NoError(t, err)
	assert.Equal(t, domain.ReserveStatusReleased, stateRepeat.Status)
}

func TestIntegrationReleaseCommittedReserveRejected(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.NoError(t, err)

	_, err = repo.ReleaseReserve(ctx, "payout-1:release", reserve.ReserveID, "late cancel")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrReserveState)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Zero(t, balance.Reserved)
}

func TestIntegrationCommitReserveConcurrent(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	const workers = 8
	var wg sync.WaitGroup
	results := make([]storage.ReserveResult, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = repo.CommitReserve(context.Background(),
				"concurrent-commit-"+string(rune('a'+idx)), reserve.ReserveID)
		}(i)
	}
	wg.Wait()

	for i := 0; i < workers; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, domain.ReserveStatusCommitted, results[i].Status)
	}

	var transactionCount int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactionCount).Error)
	assert.Equal(t, int64(1), transactionCount, "exactly one transaction per concurrent commit")

	var entriesCount int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entriesCount).Error)
	assert.Equal(t, int64(2), entriesCount)

	var payout storage.Account
	require.NoError(t, db.Where("type = ? AND asset = ?", "payout", "USDT").First(&payout).Error)
	var payoutBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", payout.ID).First(&payoutBalance).Error)
	assert.Equal(t, int64(40_000), payoutBalance.Available, "funds are credited exactly once")
}

func TestIntegrationCommitReusesExistingPayoutAccount(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	payout := seedAccount(t, db, "payout", "USDT", 2, "m-1", 5_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.NoError(t, err)

	var accounts int64
	require.NoError(t, db.Model(&storage.Account{}).Where("type = ?", "payout").Count(&accounts).Error)
	assert.Equal(t, int64(1), accounts, "the existing payout account is reused")

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", payout.ID).First(&balance).Error)
	assert.Equal(t, int64(45_000), balance.Available)
}

func TestIntegrationReleaseUnknownReserve(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	_, err := repo.ReleaseReserve(context.Background(), "key",
		"018f0000-0000-7000-8000-000000000003", "reason")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownReserve)
}

func TestIntegrationReleaseAvailableOverflow(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 100, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	// Shift available to the int64 ceiling: the hold release must hit the overflow.
	require.NoError(t, db.Model(&storage.Balance{}).Where("account_id = ?", merchant.ID).
		Updates(map[string]any{"available": int64(math.MaxInt64 - 50), "reserved": int64(100)}).Error)

	_, err = repo.ReleaseReserve(ctx, "payout-1:release", reserve.ReserveID, "overflow")
	require.Error(t, err, "available overflow is rejected")
	assert.NotErrorIs(t, err, domain.ErrInvalidReserve)
	assert.NotErrorIs(t, err, domain.ErrReserveState)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(math.MaxInt64-50), balance.Available, "the rollback restored the balance")
	assert.Equal(t, int64(100), balance.Reserved)
}

func TestIntegrationCommitKeyCollisionWithTransaction(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	_, err := repo.PostTransaction(ctx, "shared-key", "payment", "pay-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 100},
	})
	require.NoError(t, err)
	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)

	_, err = repo.CommitReserve(ctx, "shared-key", reserve.ReserveID)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrIdempotencyReuse, "the key is taken by a PostTransaction")

	var reserveAfter storage.Reserve
	require.NoError(t, db.Where("id = ?", reserve.ReserveID).First(&reserveAfter).Error)
	assert.Equal(t, "active", reserveAfter.Status, "a failed commit did not change the reserve")
}

func TestIntegrationCommitStateRepeatKeyBinding(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	first, err := repo.Reserve(ctx, reserveInput(merchant.ID, 10_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-1:commit", first.ReserveID)
	require.NoError(t, err)

	// A repeat in a terminal state with a new key must persist the key.
	stateRepeat, err := repo.CommitReserve(ctx, "payout-1:commit2", first.ReserveID)
	require.NoError(t, err)
	assert.Equal(t, domain.ReserveStatusCommitted, stateRepeat.Status)

	// The same key on another reserve is a key body conflict, not a silent commit.
	second, err := repo.Reserve(ctx, reserveInput(merchant.ID, 10_000, "payout-2:reserve", "payout-2"))
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-1:commit2", second.ReserveID)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrIdempotencyReuse)

	// A repeat of the same key on the original reserve is an idempotent replay.
	replay, err := repo.CommitReserve(ctx, "payout-1:commit2", first.ReserveID)
	require.NoError(t, err)
	assert.True(t, replay.Replayed)
	assert.Equal(t, domain.ReserveStatusCommitted, replay.Status)

	var reserveAfter storage.Reserve
	require.NoError(t, db.Where("id = ?", second.ReserveID).First(&reserveAfter).Error)
	assert.Equal(t, "active", reserveAfter.Status, "the foreign key did not commit the second reserve")
}

func TestIntegrationCommitDestinationDecimalsMismatch(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	seedAccount(t, db, "payout", "USDT", 6, "m-1", 0)
	ctx := context.Background()

	reserve, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-1:commit", reserve.ReserveID)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrAssetMismatch, "the scales of the source and the payout account must match")
}

func TestIntegrationReserveReservedOverflow(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", math.MaxInt64)
	ctx := context.Background()

	_, err := repo.Reserve(ctx, reserveInput(merchant.ID, math.MaxInt64, "overflow-1", "r-1"))
	require.NoError(t, err)

	_, err = repo.Reserve(ctx, reserveInput(merchant.ID, 1, "overflow-2", "r-2"))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidReserve, "reserved overflow is rejected")

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(math.MaxInt64), balance.Reserved)
	assert.Zero(t, balance.Available)
}

func TestIntegrationReserveContextCancelled(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop(), storage.WithRetryPolicy(storage.RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   time.Millisecond,
		MaxDelay:    2 * time.Millisecond,
	}))
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "cancelled", "r-1"))
	require.Error(t, err)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(100_000), balance.Available)
}
