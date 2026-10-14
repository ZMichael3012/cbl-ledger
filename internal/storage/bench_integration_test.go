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

// entriesPair собирает сбалансированную пару проводок между счетами.
func entriesPair(debitAccountID, creditAccountID string, amount int64) []domain.EntryInput {
	return []domain.EntryInput{
		{AccountID: debitAccountID, Direction: domain.DirectionDebit, Amount: amount},
		{AccountID: creditAccountID, Direction: domain.DirectionCredit, Amount: amount},
	}
}

// BenchmarkPostTransactionHotAccountParallel измеряет параллельную запись в одну пару
// счетов: все транзакции конкурируют за одну строку баланса, конфликты разрешаются
// повторами 2.3. Метрики: retries_per_op - число событий повторов на операцию,
// conflicts_% - доля проводок, исчерпавших бюджет повторов (ErrConflict).
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

// BenchmarkSingleRowUpdateHotBalance измеряет потолок сериализации на блокировке строки:
// одиночный UPDATE без CAS и повторов. Верхняя граница для пессимистичной стратегии
// горячего счета (сравнение в отчете CBL-2.9).
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

// setupManyEntries готовит базу, репозиторий и проводку из 100 входов (лимит 2.2):
// 50 счетов дебетуются, 50 кредитуются на равные суммы.
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

// runManyEntries выполняет проводки из 100 входов.
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

// BenchmarkPostTransactionManyEntries измеряет проводку из 100 входов с работающим
// deferred-триггером 00006: закрывает открытый вопрос 2.3 о стоимости триггера
// на многоentry-транзакциях.
func BenchmarkPostTransactionManyEntries(b *testing.B) {
	_, repo, entries := setupManyEntries(b)
	runManyEntries(b, repo, entries)
}

// BenchmarkPostTransactionManyEntriesNoTrigger измеряет ту же проводку с отключенным
// триггером 00006: разница с BenchmarkPostTransactionManyEntries - стоимость барьера.
func BenchmarkPostTransactionManyEntriesNoTrigger(b *testing.B) {
	db, repo, entries := setupManyEntries(b)
	if err := db.Exec("ALTER TABLE entries DISABLE TRIGGER entries_transaction_balanced_trigger").Error; err != nil {
		b.Fatal(err)
	}
	runManyEntries(b, repo, entries)
}

// BenchmarkPostTransactionDistributedAccounts измеряет параллельную запись по 64 парам
// счетов без общего горячего счета: потолок слоя storage и PostgreSQL без contention
// (транспорт gRPC измеряется k6-сценарием).
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
