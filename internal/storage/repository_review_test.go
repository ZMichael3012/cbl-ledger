//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func TestIntegrationIdempotencyKeyReuseRejected(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	ctx := context.Background()

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 1_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	_, err := repo.PostTransaction(ctx, "same-key", "payment", "p-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.NoError(t, err)

	_, err = repo.PostTransaction(ctx, "same-key", "payment", "p-2", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 20},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 20},
	})
	require.ErrorIs(t, err, domain.ErrIdempotencyReuse)

	var transactions int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	assert.Equal(t, int64(1), transactions)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(990), balance.Available)
}

func TestIntegrationEmptyIdempotencyKeyRejected(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	_, err := repo.PostTransaction(context.Background(), "  ", "payment", "p-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.ErrorIs(t, err, domain.ErrInvalidTransaction)
}

func TestIntegrationConcurrentSameKeySingleEffect(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 1_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	entries := []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 100},
	}

	const workers = 2
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.PostTransaction(context.Background(), "race-key", "payment", "p-race", entries)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var transactions int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	assert.Equal(t, int64(1), transactions)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(900), balance.Available)
}

func TestIntegrationClearingMayGoNegative(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	clearing := seedAccount(t, db, "clearing", "USDT", 2, "", 0)
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 0)

	result, err := repo.PostTransaction(context.Background(), "pay-1:completed", "payment", "p-1", []domain.EntryInput{
		{AccountID: clearing.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: merchant.ID, Direction: domain.DirectionCredit, Amount: 100},
	})
	require.NoError(t, err)
	for _, snapshot := range result.Balances {
		if snapshot.AccountID == clearing.ID {
			assert.Equal(t, int64(-100), snapshot.Available)
		}
	}
}

func TestIntegrationBalanceOverflowRejected(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", math.MaxInt64-5)

	_, err := repo.PostTransaction(context.Background(), "overflow-key", "payment", "p-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.ErrorIs(t, err, domain.ErrInvalidTransaction)

	var transactions int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	assert.Equal(t, int64(0), transactions)
}

func TestIntegrationMissingBalanceRow(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	orphan := storage.Account{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "fee",
		Asset:     "USDT",
		Decimals:  2,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(&orphan).Error)

	_, err := repo.PostTransaction(context.Background(), "orphan-key", "payment", "p-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: orphan.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.ErrorIs(t, err, domain.ErrUnknownAccount)
}

func TestIntegrationReservedConstraint(t *testing.T) {
	db := setupLedgerDB(t)
	account := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)

	err := db.Exec("UPDATE balances SET reserved = -1 WHERE account_id = ?", account.ID).Error
	require.Error(t, err)
}

func TestIntegrationEntriesForeignKey(t *testing.T) {
	db := setupLedgerDB(t)
	account := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)

	err := db.Exec(fmt.Sprintf(
		"INSERT INTO entries (transaction_id, account_id, direction, amount) VALUES ('%s', '%s', 'debit', 1)",
		uuid.Must(uuid.NewV7()).String(), account.ID)).Error
	require.Error(t, err)
}

func TestIntegrationBalancesNonNegativeTrigger(t *testing.T) {
	db := setupLedgerDB(t)
	account := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)

	err := db.Exec("UPDATE balances SET available = -10 WHERE account_id = ?", account.ID).Error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be negative")

	clearing := seedAccount(t, db, "clearing", "USDT", 2, "", 0)
	require.NoError(t, db.Exec("UPDATE balances SET available = -10 WHERE account_id = ?", clearing.ID).Error)
}
