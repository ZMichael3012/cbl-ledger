//go:build integration

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// entriesPair builds a balanced pair of entries between accounts.
func entriesPair(debitAccountID, creditAccountID string, amount int64) []domain.EntryInput {
	return []domain.EntryInput{
		{AccountID: debitAccountID, Direction: domain.DirectionDebit, Amount: amount},
		{AccountID: creditAccountID, Direction: domain.DirectionCredit, Amount: amount},
	}
}

// BenchmarkPostTransactionHotAccountParallel measures parallel writes to a single pair
// of accounts: all transactions contend for one balance row, and conflicts are resolved
// by the retries of 2.3. Metrics: retries_per_op is the number of retry events per
// operation, conflicts_% is the share of transactions that exhausted the retry budget
// (ErrConflict).
func BenchmarkPostTransactionHotAccountParallel(b *testing.B) {
	db := setupLedgerDB(b)
	var retries, exhausted atomic.Int64
	repo := storage.NewRepository(db, zap.NewNop(), storage.WithRetryObserver(func(storage.RetryEvent) {
		retries.Add(1)
	}))
	merchant := seedAccount(b, db, "merchant", "USDT", 2, "bench-hot-d", 1<<60)
	credit := seedAccount(b, db, "merchant", "USDT", 2, "bench-hot-c", 0)

	var seq atomic.Int64
	ctx := context.Background()
	b.SetParallelism(2)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := fmt.Sprintf("bench-hot-par-%d", seq.Add(1))
			_, err := repo.PostTransaction(ctx, key, "bench", key, entriesPair(merchant.ID, credit.ID, 1))
			switch {
			case err == nil:
			case errors.Is(err, domain.ErrConflict):
				exhausted.Add(1)
			default:
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(retries.Load())/float64(b.N), "retries_per_op")
	b.ReportMetric(float64(exhausted.Load())/float64(b.N)*100, "conflicts_%")
}

// BenchmarkSingleRowUpdateHotBalance measures the serialization ceiling on a row lock:
// a single UPDATE without CAS and retries. An upper bound for a pessimistic hot-account
// strategy (comparison in the CBL-2.9 report).
func BenchmarkSingleRowUpdateHotBalance(b *testing.B) {
	db := setupLedgerDB(b)
	merchant := seedAccount(b, db, "merchant", "USDT", 2, "bench-lock", 1<<60)

	b.SetParallelism(2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := db.Exec(
				"UPDATE balances SET available = available + 1, lock_version = lock_version + 1 WHERE account_id = ?",
				merchant.ID).Error
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// setupManyEntries prepares the database, the repository and a transaction of 100 entries
// (the limit of 2.2): 50 accounts are debited and 50 are credited with equal amounts.
func setupManyEntries(b *testing.B) (*gorm.DB, *storage.Repository, []domain.EntryInput) {
	b.Helper()
	db := setupLedgerDB(b)
	repo := storage.NewRepository(db, zap.NewNop())
	entries := make([]domain.EntryInput, 0, 100)
	for i := 0; i < 50; i++ {
		debit := seedAccount(b, db, "merchant", "USDT", 2, fmt.Sprintf("many-debit-%d", i), 1<<60)
		entries = append(entries, domain.EntryInput{AccountID: debit.ID, Direction: domain.DirectionDebit, Amount: 1})
	}
	for i := 0; i < 50; i++ {
		credit := seedAccount(b, db, "merchant", "USDT", 2, fmt.Sprintf("many-credit-%d", i), 0)
		entries = append(entries, domain.EntryInput{AccountID: credit.ID, Direction: domain.DirectionCredit, Amount: 1})
	}
	return db, repo, entries
}

// runManyEntries posts transactions of 100 entries.
func runManyEntries(b *testing.B, repo *storage.Repository, entries []domain.EntryInput) {
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-many-%d", i)
		if _, err := repo.PostTransaction(ctx, key, "bench", key, entries); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPostTransactionManyEntries measures a transaction of 100 entries with the
// deferred trigger 00006 enabled: it closes the open question of 2.3 about the trigger
// cost on many-entry transactions.
func BenchmarkPostTransactionManyEntries(b *testing.B) {
	_, repo, entries := setupManyEntries(b)
	runManyEntries(b, repo, entries)
}

// BenchmarkPostTransactionManyEntriesNoTrigger measures the same transaction with the
// trigger 00006 disabled: the difference from BenchmarkPostTransactionManyEntries is
// the barrier cost.
func BenchmarkPostTransactionManyEntriesNoTrigger(b *testing.B) {
	db, repo, entries := setupManyEntries(b)
	if err := db.Exec("ALTER TABLE entries DISABLE TRIGGER entries_transaction_balanced_trigger").Error; err != nil {
		b.Fatal(err)
	}
	runManyEntries(b, repo, entries)
}

// BenchmarkPostTransactionDistributedAccounts measures parallel writes across 64 pairs
// of accounts without a shared hot account: the ceiling of the storage layer and
// PostgreSQL without contention (the gRPC transport is measured by the k6 scenario).
func BenchmarkPostTransactionDistributedAccounts(b *testing.B) {
	db := setupLedgerDB(b)
	repo := storage.NewRepository(db, zap.NewNop())

	const pairs = 64
	debitAccounts := make([]string, pairs)
	creditAccounts := make([]string, pairs)
	for i := 0; i < pairs; i++ {
		debit := seedAccount(b, db, "merchant", "USDT", 2, fmt.Sprintf("bench-debit-%d", i), 1<<60)
		credit := seedAccount(b, db, "merchant", "USDT", 2, fmt.Sprintf("bench-credit-%d", i), 0)
		debitAccounts[i] = debit.ID
		creditAccounts[i] = credit.ID
	}

	var seq atomic.Int64
	ctx := context.Background()
	b.SetParallelism(2)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1)
			pair := int(n % pairs)
			key := fmt.Sprintf("bench-dist-%d", n)
			if _, err := repo.PostTransaction(ctx, key, "bench", key, entriesPair(debitAccounts[pair], creditAccounts[pair], 1)); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
