//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// hotAmount sets the amount of each entry in the hot-account tests.
const hotAmount int64 = 7

// retryCollector collects retry events from the observer across goroutines.
type retryCollector struct {
	mu     sync.Mutex
	events []storage.RetryEvent
}

// observe appends an event to the collection; it is safe for concurrent calls.
func (c *retryCollector) observe(event storage.RetryEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

// snapshot returns a copy of the collected events.
func (c *retryCollector) snapshot() []storage.RetryEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]storage.RetryEvent(nil), c.events...)
}

func TestIntegrationHotAccountContention(t *testing.T) {
	db := setupLedgerDB(t)
	collector := &retryCollector{}
	repo := storage.NewRepository(db, zap.NewNop(), storage.WithRetryObserver(collector.observe))

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 1_000_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	const workers = 24
	const perWorker = 5
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("hot-%d-%d", worker, i)
				_, err := repo.PostTransaction(context.Background(), key, "test", key, []domain.EntryInput{
					{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: hotAmount},
					{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: hotAmount},
				})
				results <- err
			}
		}(w)
	}
	close(start)
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
	require.GreaterOrEqual(t, successes, 1)

	applied := int64(successes) * hotAmount
	var merchantBalance, feeBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&merchantBalance).Error)
	require.NoError(t, db.Where("account_id = ?", fee.ID).First(&feeBalance).Error)
	assert.Equal(t, int64(1_000_000)-applied, merchantBalance.Available, "no lost updates")
	assert.Equal(t, applied, feeBalance.Available, "money is conserved between accounts")

	var transactions, entries int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Equal(t, int64(successes), transactions)
	assert.Equal(t, int64(2*successes), entries)

	events := collector.snapshot()
	require.NotEmpty(t, events, "contention on a hot account must lead to retries")
	for _, event := range events {
		assert.GreaterOrEqual(t, event.Attempt, 1)
		assert.NotEmpty(t, event.Cause)
		assert.GreaterOrEqual(t, event.Delay, time.Duration(0))
		assert.LessOrEqual(t, event.Delay, storage.DefaultRetryPolicy().MaxDelay)
	}
}

func TestIntegrationRetryExhaustionDeterministic(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop(),
		storage.WithRetryPolicy(storage.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}))

	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 1_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	locker := db.Begin()
	require.NoError(t, locker.Error)
	committed := false
	defer func() {
		if !committed {
			_ = locker.Rollback().Error
		}
	}()
	require.NoError(t, locker.Exec(
		"UPDATE balances SET lock_version = lock_version + 1, updated_at = now() WHERE account_id = ?", merchant.ID).Error)

	results := make(chan error, 1)
	go func() {
		_, err := repo.PostTransaction(context.Background(), "locked-key", "test", "locked", []domain.EntryInput{
			{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: hotAmount},
			{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: hotAmount},
		})
		results <- err
	}()

	waitForBlockedUpdate(t, db)
	require.NoError(t, locker.Commit().Error)
	committed = true

	select {
	case err := <-results:
		require.ErrorIs(t, err, domain.ErrConflict)
	case <-time.After(10 * time.Second):
		t.Fatal("the transaction did not finish after the lock was released")
	}

	var transactions, entries int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Zero(t, transactions, "retry exhaustion leaves no transactions")
	assert.Zero(t, entries)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(1_000), balance.Available, "funds are not debited")
}

// waitForBlockedUpdate waits for an UPDATE that is waiting on the balance row lock:
// this way the test deterministically captures the moment when the transaction hit
// a foreign lock.
func waitForBlockedUpdate(t testing.TB, db *gorm.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int64
		require.NoError(t, db.Raw(
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'UPDATE%'").Scan(&waiting).Error)
		if waiting > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the transaction did not start waiting for the balance row lock")
}

func BenchmarkPostTransactionHotAccount(b *testing.B) {
	db := setupLedgerDB(b)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(b, db, "merchant", "USDT", 2, "m-1", 1<<60)
	fee := seedAccount(b, db, "fee", "USDT", 2, "", 0)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-%d", i)
		if _, err := repo.PostTransaction(ctx, key, "bench", key, []domain.EntryInput{
			{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 1},
			{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 1},
		}); err != nil {
			b.Fatal(err)
		}
	}
}
