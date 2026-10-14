//go:build integration

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func setupLedgerDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("ledger"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func seedAccount(t *testing.T, db *gorm.DB, accountType, asset string, decimals int32, merchantRef string, initial int64) storage.Account {
	t.Helper()
	account := storage.Account{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      accountType,
		Asset:     asset,
		Decimals:  decimals,
		CreatedAt: time.Now().UTC(),
	}
	if merchantRef != "" {
		account.MerchantRef = &merchantRef
	}
	require.NoError(t, db.Create(&account).Error)
	require.NoError(t, db.Create(&storage.Balance{
		AccountID: account.ID,
		Available: initial,
		UpdatedAt: time.Now().UTC(),
	}).Error)
	return account
}

func TestIntegrationPostTransactionBalances(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 50_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	result, err := repo.PostTransaction(context.Background(), "pay-1:completed", "payment", "pay-1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10_000},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10_000},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.TransactionID)
	require.False(t, result.Replayed)
	require.Len(t, result.Balances, 2)

	byAccount := map[string]int64{}
	for _, snapshot := range result.Balances {
		byAccount[snapshot.AccountID] = snapshot.Available
	}
	assert.Equal(t, int64(40_000), byAccount[merchant.ID])
	assert.Equal(t, int64(10_000), byAccount[fee.ID])

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Equal(t, int64(2), entries)
}

func TestIntegrationIdempotentRepeat(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 50_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	entries := []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10_000},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10_000},
	}

	first, err := repo.PostTransaction(context.Background(), "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)

	second, err := repo.PostTransaction(context.Background(), "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)
	assert.Equal(t, first.TransactionID, second.TransactionID)
	assert.True(t, second.Replayed)

	var transactions, entryCount int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entryCount).Error)
	assert.Equal(t, int64(1), transactions)
	assert.Equal(t, int64(2), entryCount)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(40_000), balance.Available)
}

func TestIntegrationRejections(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	btc := seedAccount(t, db, "fee", "BTC", 8, "", 0)

	_, err := repo.PostTransaction(context.Background(), "k1", "payment", "p1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 99},
	})
	require.ErrorIs(t, err, domain.ErrUnbalanced)

	_, err = repo.PostTransaction(context.Background(), "k2", "payment", "p1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 200},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 200},
	})
	require.ErrorIs(t, err, domain.ErrInsufficientFunds)

	_, err = repo.PostTransaction(context.Background(), "k3", "payment", "p1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: uuid.Must(uuid.NewV7()).String(), Direction: domain.DirectionCredit, Amount: 10},
	})
	require.ErrorIs(t, err, domain.ErrUnknownAccount)

	_, err = repo.PostTransaction(context.Background(), "k4", "payment", "p1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: btc.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.ErrorIs(t, err, domain.ErrAssetMismatch)

	var transactions int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	assert.Equal(t, int64(0), transactions)
}

func TestIntegrationEntriesAppendOnly(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	_, err := repo.PostTransaction(context.Background(), "k1", "payment", "p1", []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10},
	})
	require.NoError(t, err)

	err = db.Exec("UPDATE entries SET amount = 1").Error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "append-only")

	err = db.Exec("DELETE FROM entries").Error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "append-only")

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Equal(t, int64(2), entries)
}

func TestIntegrationConcurrentDebits(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 500)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	const attempts = 10
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-%d", n)
			_, err := repo.PostTransaction(context.Background(), key, "test", key, []domain.EntryInput{
				{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 100},
				{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 100},
			})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case isExpectedConcurrencyError(err):
		default:
			require.NoError(t, err)
		}
	}
	require.LessOrEqual(t, successes, 5)
	require.GreaterOrEqual(t, successes, 1)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(500-100*successes), balance.Available)
	assert.GreaterOrEqual(t, balance.Available, int64(0))

	var feeBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", fee.ID).First(&feeBalance).Error)
	assert.Equal(t, int64(100*successes), feeBalance.Available)
}

// isExpectedConcurrencyError сообщает, что ошибка ожидаема при конкурентных спорах за баланс.
func isExpectedConcurrencyError(err error) bool {
	return errors.Is(err, domain.ErrInsufficientFunds) || errors.Is(err, domain.ErrConflict)
}
