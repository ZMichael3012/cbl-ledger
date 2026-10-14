//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func TestIntegrationGetBalance(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	ctx := context.Background()

	view, err := repo.GetBalance(ctx, merchant.ID)
	require.NoError(t, err)
	assert.Equal(t, merchant.ID, view.AccountID)
	assert.Equal(t, "USDT", view.Asset)
	assert.Equal(t, int32(2), view.Decimals)
	assert.Equal(t, int64(100_000), view.Available)
	assert.Zero(t, view.Reserved)

	_, err = repo.Reserve(ctx, reserveInput(merchant.ID, 40_000, "payout-1:reserve", "payout-1"))
	require.NoError(t, err)
	view, err = repo.GetBalance(ctx, merchant.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(60_000), view.Available)
	assert.Equal(t, int64(40_000), view.Reserved)

	_, err = repo.GetBalance(ctx, "018f0000-0000-7000-8000-000000000010")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownAccount)

	_, err = repo.GetBalance(ctx, "not-a-uuid")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownAccount)

	empty := seedAccount(t, db, "merchant", "USDT", 2, "m-empty", 0)
	view, err = repo.GetBalance(ctx, empty.ID)
	require.NoError(t, err)
	assert.Zero(t, view.Available, "нулевой доступный баланс возвращается явно")
	assert.Zero(t, view.Reserved)

	// Счет без балансового ряда - испорченный провижининг: fail-closed.
	broken := storage.Account{
		ID:        "018f0000-0000-7000-8000-0000000000aa",
		Type:      "merchant",
		Asset:     "USDT",
		Decimals:  2,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(&broken).Error)
	_, err = repo.GetBalance(ctx, broken.ID)
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownAccount)
}

func postPayment(t *testing.T, repo *storage.Repository, key, refID, merchantID, feeID string, amount int64) {
	t.Helper()
	_, err := repo.PostTransaction(context.Background(), key, "payment", refID, []domain.EntryInput{
		{AccountID: merchantID, Direction: domain.DirectionDebit, Amount: amount},
		{AccountID: feeID, Direction: domain.DirectionCredit, Amount: amount},
	})
	require.NoError(t, err)
}

func TestIntegrationListEntriesPagination(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		postPayment(t, repo, "pay-"+string(rune('0'+i))+":completed", "pay-"+string(rune('0'+i)), merchant.ID, fee.ID, int64(i*100))
	}

	page, cursor, err := repo.ListEntries(ctx, merchant.ID, 2, "")
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.NotEmpty(t, cursor)
	assert.Equal(t, int64(100), page[0].Amount)
	assert.Equal(t, domain.DirectionDebit, page[0].Direction)
	assert.Equal(t, "payment", page[0].RefType)
	assert.Equal(t, "pay-1", page[0].RefID)
	assert.Equal(t, "USDT", page[0].Asset)
	assert.Equal(t, int32(2), page[0].Decimals)
	assert.NotEmpty(t, page[0].TransactionID)
	assert.False(t, page[0].CreatedAt.IsZero())
	assert.Less(t, page[0].EntryID, page[1].EntryID, "выписка по возрастанию entry_id")

	seen := append([]storage.EntryView{}, page...)
	for cursor != "" {
		page, cursor, err = repo.ListEntries(ctx, merchant.ID, 2, cursor)
		require.NoError(t, err)
		seen = append(seen, page...)
	}
	require.Len(t, seen, 5)
	for i := 1; i < len(seen); i++ {
		assert.Less(t, seen[i-1].EntryID, seen[i].EntryID, "страницы не пересекаются и не пропускают записи")
	}
	assert.Equal(t, int64(500), seen[4].Amount)

	emptyAccount := seedAccount(t, db, "clearing", "USDT", 2, "", 0)
	empty, cursor, err := repo.ListEntries(ctx, emptyAccount.ID, 0, "")
	require.NoError(t, err)
	assert.Empty(t, empty, "счет без проводок дает пустую страницу")
	assert.Empty(t, cursor)

	_, cursor, err = repo.ListEntries(ctx, merchant.ID, 500, "")
	require.NoError(t, err, "limit больше потолка клампится")
	assert.Empty(t, cursor)
}

func TestIntegrationListEntriesRejections(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 1_000)
	ctx := context.Background()

	_, _, err := repo.ListEntries(ctx, "018f0000-0000-7000-8000-000000000011", 10, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownAccount)

	for _, cursor := range []string{"abc", "-1", "1.5", "999999999999999999999999"} {
		_, _, err = repo.ListEntries(ctx, merchant.ID, 10, cursor)
		require.Error(t, err, "курсор %q", cursor)
		assert.ErrorIs(t, err, domain.ErrInvalidCursor, "курсор %q", cursor)
	}
}

func TestIntegrationReplayReturnsSnapshot(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	entries := []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10_000},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10_000},
	}
	first, err := repo.PostTransaction(ctx, "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)
	require.NotEmpty(t, first.TransactionID)

	postPayment(t, repo, "pay-2:completed", "pay-2", merchant.ID, fee.ID, 20_000)

	replay, err := repo.PostTransaction(ctx, "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Len(t, replay.Balances, 2)

	byAccount := map[string]storage.BalanceSnapshot{}
	for _, snapshot := range replay.Balances {
		byAccount[snapshot.AccountID] = snapshot
	}
	assert.Equal(t, int64(90_000), byAccount[merchant.ID].Available, "снимок на момент первой проводки")
	assert.Equal(t, int64(10_000), byAccount[fee.ID].Available)

	current, err := repo.GetBalance(ctx, merchant.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(70_000), current.Available, "текущий баланс отличается от снимка")

	var snapshotRows int64
	require.NoError(t, db.Model(&storage.TransactionBalance{}).Count(&snapshotRows).Error)
	assert.Equal(t, int64(4), snapshotRows, "по два снимка на каждую транзакцию")
}

func TestIntegrationReplayLegacyWithoutSnapshot(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	entries := []domain.EntryInput{
		{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: 10_000},
		{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: 10_000},
	}
	first, err := repo.PostTransaction(ctx, "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)
	postPayment(t, repo, "pay-2:completed", "pay-2", merchant.ID, fee.ID, 20_000)

	// Транзакции до миграции 00008 не имеют снимков: повтор читает текущие балансы.
	require.NoError(t, db.Where("transaction_id = ?", first.TransactionID).
		Delete(&storage.TransactionBalance{}).Error)

	replay, err := repo.PostTransaction(ctx, "pay-1:completed", "payment", "pay-1", entries)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	for _, snapshot := range replay.Balances {
		if snapshot.AccountID == merchant.ID {
			assert.Equal(t, int64(70_000), snapshot.Available, "legacy fallback возвращает текущий баланс")
		}
	}
}

func TestIntegrationCursorFromAnotherStatement(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchantA := seedAccount(t, db, "merchant", "USDT", 2, "m-a", 100_000)
	merchantB := seedAccount(t, db, "merchant", "USDT", 2, "m-b", 100_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		postPayment(t, repo, fmt.Sprintf("a-%d:completed", i), fmt.Sprintf("a-%d", i), merchantA.ID, fee.ID, 100)
		postPayment(t, repo, fmt.Sprintf("b-%d:completed", i), fmt.Sprintf("b-%d", i), merchantB.ID, fee.ID, 100)
	}

	pageA, _, err := repo.ListEntries(ctx, merchantA.ID, 1, "")
	require.NoError(t, err)
	require.Len(t, pageA, 1)

	// Курсор из выписки другого счета принимается: фильтр по entry_id > cursor.
	pageB, _, err := repo.ListEntries(ctx, merchantB.ID, 10, fmt.Sprint(pageA[0].EntryID))
	require.NoError(t, err)
	for _, entry := range pageB {
		assert.Greater(t, entry.EntryID, pageA[0].EntryID)
	}
}

func TestIntegrationListEntriesConcurrentWritesCompleteness(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop(),
		storage.WithRetryPolicy(storage.RetryPolicy{MaxAttempts: 32, BaseDelay: 2 * time.Millisecond, MaxDelay: 100 * time.Millisecond}))
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 10_000_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	ctx := context.Background()

	const writers = 3
	const perWriter = 15
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Разносим попытки писателей по времени: цель теста - чередование
				// коммитов, а не предельная нагрузка на CAS.
				time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond) //nolint:gosec // G404: разнесение попыток, не криптография
				key := fmt.Sprintf("pag-%d-%d:completed", w, i)
				_, err := repo.PostTransaction(context.Background(), key, "payment", key, []domain.EntryInput{
					{AccountID: merchant.ID, Direction: domain.DirectionDebit, Amount: int64(100 + i)},
					{AccountID: fee.ID, Direction: domain.DirectionCredit, Amount: int64(100 + i)},
				})
				if err != nil {
					errCh <- fmt.Errorf("%s: %w", key, err)
					return
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Читатель следует цепочке курсоров, пока идут записи: пропуск любой
	// закоммиченной проводки означал бы разрыв порядка id и коммитов.
	seen := map[int64]struct{}{}
	cursor := ""
	finished := false
	for !finished {
		page, next, err := repo.ListEntries(ctx, merchant.ID, 3, cursor)
		require.NoError(t, err)
		for _, entry := range page {
			seen[entry.EntryID] = struct{}{}
		}
		cursor = next
		if cursor == "" {
			select {
			case <-done:
				finished = true
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	close(errCh)
	for err := range errCh {
		require.NoError(t, err, "проводка конкурентного писателя")
	}

	var full []storage.EntryView
	cursor = ""
	for {
		page, next, err := repo.ListEntries(ctx, merchant.ID, 50, cursor)
		require.NoError(t, err)
		full = append(full, page...)
		cursor = next
		if cursor == "" {
			break
		}
	}
	require.Len(t, full, writers*perWriter, "все конкурентные проводки закоммичены")
	for _, entry := range full {
		assert.Contains(t, seen, entry.EntryID,
			"курсорная пагинация не должна пропускать закоммиченную проводку %d", entry.EntryID)
	}
}
