//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// testEventSink пишет конверты ledger.events в outbox в транзакции бизнес-изменения
// (как продюсерский путь сервиса): нужен для проверки атомарности outbox.
type testEventSink struct{}

// AppendTxnEvent добавляет конверт события в переданной транзакции.
func (testEventSink) AppendTxnEvent(ctx context.Context, tx *gorm.DB, event storage.TxnEvent) error {
	envelope, err := outbox.NewEnvelope(event.Type, "ledger.events", event.Key,
		map[string]string{"producer": "cbl-ledger"}, []byte(event.Type))
	if err != nil {
		return err
	}
	return outbox.NewStore().Append(ctx, tx, envelope)
}

// rowCounts описывает счетчики строк для проверки атомарности операций.
type rowCounts struct {
	Transactions int64
	Entries      int64
	Reserves     int64
	ReserveOps   int64
	Snapshots    int64
	Outbox       int64
}

// snapshotRowCounts собирает счетчики строк таблиц леджера.
func snapshotRowCounts(t *testing.T, db *gorm.DB) rowCounts {
	t.Helper()
	var counts rowCounts
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&counts.Transactions).Error)
	require.NoError(t, db.Model(&storage.Entry{}).Count(&counts.Entries).Error)
	require.NoError(t, db.Model(&storage.Reserve{}).Count(&counts.Reserves).Error)
	require.NoError(t, db.Model(&storage.ReserveOperation{}).Count(&counts.ReserveOps).Error)
	require.NoError(t, db.Model(&storage.TransactionBalance{}).Count(&counts.Snapshots).Error)
	require.NoError(t, db.Model(&outbox.Record{}).Count(&counts.Outbox).Error)
	return counts
}

// balanceInvariantRow описывает строку агрегата инварианта балансов.
type balanceInvariantRow struct {
	AccountID string `gorm:"column:account_id"`
	Type      string `gorm:"column:type"`
	Asset     string `gorm:"column:asset"`
	Available int64  `gorm:"column:available"`
	Reserved  int64  `gorm:"column:reserved"`
	Credits   int64  `gorm:"column:credits"`
	Debits    int64  `gorm:"column:debits"`
}

// checkPropertyInvariants независимо (SQL-агрегаты) проверяет инварианты леджера:
// баланс = начальный + кредиты - дебеты; reserved равен сумме активных удержаний;
// сохранность суммы по активу; сбалансированность транзакций; целостность резервов;
// снимки только у проводок PostTransaction.
func checkPropertyInvariants(t *testing.T, db *gorm.DB, initial map[string]int64, assetTotals map[string]int64, stage string) {
	t.Helper()

	var rows []balanceInvariantRow
	require.NoError(t, db.Raw(`
		SELECT b.account_id, a.type, a.asset, b.available, b.reserved,
		       COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount END), 0) AS credits,
		       COALESCE(SUM(CASE WHEN e.direction = 'debit' THEN e.amount END), 0) AS debits
		FROM balances AS b
		JOIN accounts AS a ON a.id = b.account_id
		LEFT JOIN entries AS e ON e.account_id = b.account_id
		GROUP BY b.account_id, a.type, a.asset, b.available, b.reserved`).Scan(&rows).Error)
	require.NotEmpty(t, rows)

	globalByAsset := map[string]int64{}
	for _, row := range rows {
		expected := initial[row.AccountID] + row.Credits - row.Debits
		assert.Equalf(t, expected, row.Available+row.Reserved,
			"баланс = начальный + кредиты - дебеты (%s, %s)", row.AccountID, stage)
		assert.GreaterOrEqualf(t, row.Reserved, int64(0), "reserved >= 0 (%s, %s)", row.AccountID, stage)
		if row.Available < 0 {
			assert.Containsf(t, []string{"clearing", "fee", "suspense"}, row.Type,
				"отрицательный available только у транзитных счетов (%s, %s)", row.AccountID, stage)
		}
		globalByAsset[row.Asset] += row.Available + row.Reserved
	}
	assert.Equalf(t, assetTotals, globalByAsset, "глобальные суммы по активам сохраняются (%s)", stage)

	// reserved счета равен сумме его активных удержаний: ловит перенос средств
	// при release/commit с неверным направлением или без смены статуса.
	var reservedRows []struct {
		AccountID string `gorm:"column:account_id"`
		Reserved  int64  `gorm:"column:reserved"`
		ActiveSum int64  `gorm:"column:active_sum"`
	}
	require.NoError(t, db.Raw(`
		SELECT b.account_id, b.reserved,
		       COALESCE((SELECT SUM(r.amount) FROM reserves AS r
		                 WHERE r.account_id = b.account_id AND r.status = 'active'), 0) AS active_sum
		FROM balances AS b`).Scan(&reservedRows).Error)
	for _, row := range reservedRows {
		assert.Equalf(t, row.ActiveSum, row.Reserved,
			"reserved = сумма активных удержаний (%s, %s)", row.AccountID, stage)
	}

	var unbalanced int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT transaction_id FROM entries
			GROUP BY transaction_id
			HAVING SUM(CASE WHEN direction = 'credit' THEN amount ELSE 0 END)
			    <> SUM(CASE WHEN direction = 'debit' THEN amount ELSE 0 END)
		) AS unbalanced`).Scan(&unbalanced).Error)
	assert.Zerof(t, unbalanced, "каждая транзакция сбалансирована (%s)", stage)

	// Целостность резервов: committed ссылается на существующую проводку,
	// захват пишет ровно пару проводок.
	var brokenCommitted int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM reserves AS r
		WHERE r.status = 'committed'
		  AND NOT EXISTS (SELECT 1 FROM transactions AS t WHERE t.id = r.transaction_id)`).
		Scan(&brokenCommitted).Error)
	assert.Zerof(t, brokenCommitted, "committed-резерв ссылается на проводку (%s)", stage)

	var brokenCommitEntries int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT e.transaction_id FROM entries AS e
			JOIN transactions AS t ON t.id = e.transaction_id
			WHERE t.ref_type = 'reserve'
			GROUP BY e.transaction_id
			HAVING COUNT(*) <> 2
		) AS broken`).Scan(&brokenCommitEntries).Error)
	assert.Zerof(t, brokenCommitEntries, "захват резерва пишет ровно пару проводок (%s)", stage)

	// Снимки balances_after пишут только проводки PostTransaction (не резервы).
	var expectedSnapshots int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT DISTINCT e.transaction_id, e.account_id FROM entries AS e
			JOIN transactions AS t ON t.id = e.transaction_id
			WHERE t.ref_type <> 'reserve'
		) AS pairs`).Scan(&expectedSnapshots).Error)
	var snapshots int64
	require.NoError(t, db.Model(&storage.TransactionBalance{}).Count(&snapshots).Error)
	assert.Equalf(t, expectedSnapshots, snapshots, "снимки соответствуют проводкам (%s)", stage)

	var reserveSnapshots int64
	require.NoError(t, db.Raw(`
		SELECT COUNT(*) FROM transaction_balances AS tb
		JOIN transactions AS t ON t.id = tb.transaction_id
		WHERE t.ref_type = 'reserve'`).Scan(&reserveSnapshots).Error)
	assert.Zerof(t, reserveSnapshots, "захваты резервов не пишут снимки (%s)", stage)
}

// replayCase описывает успешную проводку для проверки идемпотентного повтора.
type replayCase struct {
	key     string
	refID   string
	entries []domain.EntryInput
}

// propertyRand возвращает генератор сценария: фиксированный сид, переопределяемый
// PROPERTY_SEED для локализации редких сбоев.
func propertyRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := int64(20261014)
	if raw := os.Getenv("PROPERTY_SEED"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err, "PROPERTY_SEED")
		seed = parsed
	}
	t.Logf("property storage seed: %d", seed)
	return rand.New(rand.NewPCG(uint64(seed), 0x9e3779b9))
}

func TestIntegrationPropertyInvariants(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop(),
		storage.WithRetryPolicy(storage.RetryPolicy{MaxAttempts: 16, BaseDelay: time.Millisecond, MaxDelay: 50 * time.Millisecond}),
		storage.WithEventSink(testEventSink{}))
	rng := propertyRand(t)

	u1 := seedAccount(t, db, "merchant", "USDT", 2, "m-prop-1", 1_000_000)
	u2 := seedAccount(t, db, "merchant", "USDT", 2, "m-prop-2", 500_000)
	uFee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	uClearing := seedAccount(t, db, "clearing", "USDT", 2, "", 0)
	b1 := seedAccount(t, db, "merchant", "BTC", 8, "m-prop-btc", 2_000_000)
	bFee := seedAccount(t, db, "fee", "BTC", 8, "", 0)

	initial := map[string]int64{
		u1.ID: 1_000_000, u2.ID: 500_000, uFee.ID: 0, uClearing.ID: 0,
		b1.ID: 2_000_000, bFee.ID: 0,
	}
	assetTotals := map[string]int64{"USDT": 1_500_000, "BTC": 2_000_000}
	usdtPool := []storage.Account{u1, u2, uFee, uClearing}
	btcPool := []storage.Account{b1, bFee}
	merchants := []storage.Account{u1, u2, b1}
	ctx := context.Background()
	check := func(stage string) { checkPropertyInvariants(t, db, initial, assetTotals, stage) }
	check("seed")

	// Счетчики успешных операций: проверяют живучесть сценария и точные дельты строк.
	var posts, reserves, commits, releases, expectedEntries int
	var rejectedUnbalanced, rejectedInsufficient, rejectedState, rejectedAsset, rejectedReuse int
	var activeReserves, finalizedReserves []string
	var replays []replayCase

	const operations = 30
	for i := 0; i < operations; i++ {
		stage := fmt.Sprintf("op-%d", i)
		pick := rng.IntN(100)
		if pick < 40 || (pick >= 60 && len(activeReserves) == 0) {
			pool := usdtPool
			if rng.IntN(2) == 0 {
				pool = btcPool
			}
			from := pool[rng.IntN(len(pool))]
			to := pool[rng.IntN(len(pool))]
			for to.ID == from.ID {
				to = pool[rng.IntN(len(pool))]
			}
			amount := int64(1 + rng.Int64N(50_000))
			key := fmt.Sprintf("prop-%d-post", i)
			refID := fmt.Sprintf("prop-%d", i)
			entries := []domain.EntryInput{
				{AccountID: from.ID, Direction: domain.DirectionDebit, Amount: amount},
				{AccountID: to.ID, Direction: domain.DirectionCredit, Amount: amount},
			}
			unbalanced := rng.IntN(7) == 0
			if unbalanced {
				entries[1].Amount = amount + 1 + rng.Int64N(1_000)
			}
			before := snapshotRowCounts(t, db)
			_, err := repo.PostTransaction(ctx, key, "property", refID, entries)
			switch {
			case unbalanced:
				require.ErrorIs(t, err, domain.ErrUnbalanced)
				rejectedUnbalanced++
				assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутая проводка не меняет состояние (%s)", stage)
			case err != nil:
				require.ErrorIs(t, err, domain.ErrInsufficientFunds, "неожиданная ошибка (%s): %v", stage, err)
				rejectedInsufficient++
				assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутая проводка не меняет состояние (%s)", stage)
			default:
				posts++
				expectedEntries += 2
				replays = append(replays, replayCase{key: key, refID: refID, entries: entries})
			}
		} else if pick < 60 {
			target := merchants[rng.IntN(len(merchants))]
			amount := int64(1 + rng.Int64N(150_000))
			key := fmt.Sprintf("prop-%d-reserve", i)
			before := snapshotRowCounts(t, db)
			result, err := repo.Reserve(ctx, storage.ReserveInput{
				IdempotencyKey: key,
				AccountID:      target.ID,
				Asset:          target.Asset,
				Decimals:       target.Decimals,
				Amount:         amount,
				RefID:          key,
			})
			if err != nil {
				require.ErrorIs(t, err, domain.ErrInsufficientFunds, "неожиданная ошибка (%s): %v", stage, err)
				rejectedInsufficient++
				assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутое удержание не меняет состояние (%s)", stage)
			} else {
				reserves++
				activeReserves = append(activeReserves, result.ReserveID)
			}
		} else if pick < 80 {
			index := rng.IntN(len(activeReserves))
			reserveID := activeReserves[index]
			key := fmt.Sprintf("prop-%d-commit", i)
			before := snapshotRowCounts(t, db)
			_, err := repo.CommitReserve(ctx, key, reserveID)
			if err == nil {
				commits++
				expectedEntries += 2
				activeReserves = append(activeReserves[:index], activeReserves[index+1:]...)
				finalizedReserves = append(finalizedReserves, reserveID)
			} else {
				require.ErrorIs(t, err, domain.ErrReserveState, "неожиданная ошибка (%s): %v", stage, err)
				rejectedState++
				assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутый захват не меняет состояние (%s)", stage)
			}
		} else {
			index := rng.IntN(len(activeReserves))
			reserveID := activeReserves[index]
			key := fmt.Sprintf("prop-%d-release", i)
			before := snapshotRowCounts(t, db)
			_, err := repo.ReleaseReserve(ctx, key, reserveID, "property release")
			if err == nil {
				releases++
				activeReserves = append(activeReserves[:index], activeReserves[index+1:]...)
				finalizedReserves = append(finalizedReserves, reserveID)
			} else {
				require.ErrorIs(t, err, domain.ErrReserveState, "неожиданная ошибка (%s): %v", stage, err)
				rejectedState++
				assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутый возврат не меняет состояние (%s)", stage)
			}
		}
		check(stage)
	}

	// Детерминированные отказные кейсы: FR-3 не должен зависеть от везения сида.
	assertPositive := func(t *testing.T, counter int, name string) {
		t.Helper()
		assert.Greaterf(t, counter, 0, "класс отказов %s не воспроизведен сценарием", name)
	}
	require.Greater(t, posts, 0, "сценарий обязан содержать успешные проводки")

	// Дисбаланс: детерминированный кейс, FR-3 не зависит от везения сида.
	before := snapshotRowCounts(t, db)
	_, err := repo.PostTransaction(ctx, "prop-unbalanced", "property", "prop-unbalanced", []domain.EntryInput{
		{AccountID: u1.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: uFee.ID, Direction: domain.DirectionCredit, Amount: 101},
	})
	require.ErrorIs(t, err, domain.ErrUnbalanced)
	assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутый дисбаланс не меняет состояние")
	rejectedUnbalanced++
	assertPositive(t, rejectedUnbalanced, "unbalanced")

	// Нехватка средств на проводке: дебет больше доступного.
	available := balanceOf(t, db, u1.ID)
	nsfAmount := available + 1
	before = snapshotRowCounts(t, db)
	_, err = repo.PostTransaction(ctx, "prop-nsf-post", "property", "prop-nsf-post", []domain.EntryInput{
		{AccountID: u1.ID, Direction: domain.DirectionDebit, Amount: nsfAmount},
		{AccountID: uFee.ID, Direction: domain.DirectionCredit, Amount: nsfAmount},
	})
	require.ErrorIs(t, err, domain.ErrInsufficientFunds)
	assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутая проводка NSF не меняет состояние")
	rejectedInsufficient++

	// Нехватка средств на удержании: сумма больше доступного.
	before = snapshotRowCounts(t, db)
	_, err = repo.Reserve(ctx, storage.ReserveInput{
		IdempotencyKey: "prop-nsf-reserve", AccountID: u1.ID, Asset: "USDT", Decimals: 2,
		Amount: balanceOf(t, db, u1.ID) + 1, RefID: "prop-nsf-reserve",
	})
	require.ErrorIs(t, err, domain.ErrInsufficientFunds)
	assert.Equal(t, before, snapshotRowCounts(t, db), "отвергнутое удержание NSF не меняет состояние")
	rejectedInsufficient++

	// Смешивание активов в одной транзакции отклоняется.
	before = snapshotRowCounts(t, db)
	_, err = repo.PostTransaction(ctx, "prop-mixed", "property", "prop-mixed", []domain.EntryInput{
		{AccountID: u1.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: b1.ID, Direction: domain.DirectionCredit, Amount: 100},
	})
	require.ErrorIs(t, err, domain.ErrAssetMismatch)
	assert.Equal(t, before, snapshotRowCounts(t, db), "смешивание активов не меняет состояние")
	rejectedAsset++

	// Прямые проверки встречных переходов резервов (спека, раздел 6).
	ensureActiveReserve := func(key string) string {
		if len(activeReserves) > 0 {
			return activeReserves[len(activeReserves)-1]
		}
		result, reserveErr := repo.Reserve(ctx, storage.ReserveInput{
			IdempotencyKey: key, AccountID: u2.ID, Asset: "USDT", Decimals: 2, Amount: 1_000, RefID: key,
		})
		require.NoError(t, reserveErr)
		reserves++
		activeReserves = append(activeReserves, result.ReserveID)
		return result.ReserveID
	}
	// commit после release
	releaseID := ensureActiveReserve("prop-state-release")
	_, err = repo.ReleaseReserve(ctx, "prop-state-release-op", releaseID, "state check")
	require.NoError(t, err)
	releases++
	activeReserves = activeReserves[:len(activeReserves)-1]
	before = snapshotRowCounts(t, db)
	_, err = repo.CommitReserve(ctx, "prop-state-commit-after-release", releaseID)
	require.ErrorIs(t, err, domain.ErrReserveState)
	assert.Equal(t, before, snapshotRowCounts(t, db), "commit released резерва не меняет состояние")
	rejectedState++
	// release после commit
	commitID := ensureActiveReserve("prop-state-commit")
	_, err = repo.CommitReserve(ctx, "prop-state-commit-op", commitID)
	require.NoError(t, err)
	commits++
	expectedEntries += 2
	activeReserves = activeReserves[:len(activeReserves)-1]
	before = snapshotRowCounts(t, db)
	_, err = repo.ReleaseReserve(ctx, "prop-state-release-after-commit", commitID, "state check")
	require.ErrorIs(t, err, domain.ErrReserveState)
	assert.Equal(t, before, snapshotRowCounts(t, db), "release committed резерва не меняет состояние")
	rejectedState++
	assertPositive(t, rejectedState, "reserve state")
	require.Greater(t, reserves, 0, "сценарий обязан создать удержания")
	require.Greater(t, commits, 0, "сценарий обязан выполнить захват")
	require.Greater(t, releases, 0, "сценарий обязан выполнить возврат")

	// Проводка с повторяющимся счетом: снимок пишется на счет, а не на проводку.
	before = snapshotRowCounts(t, db)
	_, err = repo.PostTransaction(ctx, "prop-multi", "property", "prop-multi", []domain.EntryInput{
		{AccountID: u1.ID, Direction: domain.DirectionDebit, Amount: 100},
		{AccountID: uFee.ID, Direction: domain.DirectionCredit, Amount: 100},
		{AccountID: u1.ID, Direction: domain.DirectionDebit, Amount: 200},
		{AccountID: uFee.ID, Direction: domain.DirectionCredit, Amount: 200},
	})
	require.NoError(t, err)
	after := snapshotRowCounts(t, db)
	posts++
	expectedEntries += 4
	assert.Equal(t, before.Transactions+1, after.Transactions)
	assert.Equal(t, before.Entries+4, after.Entries)
	assert.Equal(t, before.Snapshots+2, after.Snapshots, "снимок на счет, а не на проводку")
	assert.Equal(t, before.Outbox+1, after.Outbox)

	// Идемпотентность: повтор успешных проводок не создает эффектов.
	require.NotEmpty(t, replays, "сценарий обязан содержать успешные проводки для replay")
	before = snapshotRowCounts(t, db)
	for _, replay := range replays {
		result, replayErr := repo.PostTransaction(ctx, replay.key, "property", replay.refID, replay.entries)
		require.NoError(t, replayErr)
		assert.True(t, result.Replayed)
	}
	assert.Equal(t, before, snapshotRowCounts(t, db), "replay не создает эффектов")
	check("replay")

	// Повтор ключа с другим телом отклоняется и не меняет состояние.
	mutated := append([]domain.EntryInput(nil), replays[0].entries...)
	mutated[0].Amount++
	mutated[1].Amount++
	_, err = repo.PostTransaction(ctx, replays[0].key, "property", replays[0].refID, mutated)
	require.ErrorIs(t, err, domain.ErrIdempotencyReuse)
	rejectedReuse++
	assert.Equal(t, before, snapshotRowCounts(t, db), "конфликт ключа не меняет состояние")
	assertPositive(t, rejectedReuse, "key reuse")
	assertPositive(t, rejectedInsufficient, "insufficient funds")
	assertPositive(t, rejectedAsset, "asset mismatch")

	// Точные дельты строк: ловит «пустые» операции и лишние записи.
	final := snapshotRowCounts(t, db)
	assert.Equal(t, int64(posts+commits), final.Transactions)
	assert.Equal(t, int64(expectedEntries), final.Entries)
	assert.Equal(t, int64(reserves), final.Reserves)
	assert.Equal(t, int64(commits+releases), final.ReserveOps)
	assert.Equal(t, int64(2*posts), final.Snapshots, "снимок на каждую проводку PostTransaction")
	assert.Equal(t, int64(posts+reserves+commits+releases), final.Outbox, "событие на каждую мутацию")
	check("final")
}

// balanceOf читает доступный баланс счета.
func balanceOf(t *testing.T, db *gorm.DB, accountID string) int64 {
	t.Helper()
	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", accountID).First(&balance).Error)
	return balance.Available
}
