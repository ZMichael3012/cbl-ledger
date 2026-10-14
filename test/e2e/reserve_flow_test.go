//go:build e2e

// Пакет e2e содержит сквозные сценарии леджера на compose-стенде (KI-ledger-03):
// Reserve -> Commit / Release и PostTransaction -> GetBalance -> ListEntries
// через реальный gRPC и PostgreSQL.
// Запуск: powershell -NoProfile -File test/e2e/reserve_flow.ps1
package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-ledger/internal/storage"
)

// envOr возвращает значение переменной окружения или значение по умолчанию.
func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// openLedgerDB подключается к PostgreSQL compose-стенда.
func openLedgerDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := envOr("LEDGER_PG_DSN", "postgres://cbl:cbl@localhost:5432/ledger?sslmode=disable")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "compose-стенд ledger должен быть запущен (make up)")
	return db
}

// openLedgerClient подключается к gRPC compose-стенда.
func openLedgerClient(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()
	address := envOr("LEDGER_GRPC_ADDR", "localhost:9040")
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

// seedE2EAccount создает счет с балансом для сценария.
func seedE2EAccount(t *testing.T, db *gorm.DB, accountType string, initial int64) storage.Account {
	t.Helper()
	merchantRef := "e2e-" + uuid.Must(uuid.NewV7()).String()
	account := storage.Account{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        accountType,
		Asset:       "USDT",
		Decimals:    2,
		MerchantRef: &merchantRef,
		CreatedAt:   time.Now().UTC(),
	}
	require.NoError(t, db.Create(&account).Error)
	require.NoError(t, db.Create(&storage.Balance{
		AccountID: account.ID,
		Available: initial,
		UpdatedAt: time.Now().UTC(),
	}).Error)
	return account
}

// money собирает сумму актива сценария.
func money(units int64) *commonv1.Money {
	return &commonv1.Money{MinorUnits: units, Asset: "USDT", Decimals: 2}
}

func TestReserveCommitAndReleaseFlow(t *testing.T) {
	db := openLedgerDB(t)
	client := openLedgerClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	merchant := seedE2EAccount(t, db, "merchant", 100_000)
	refID := "payout-" + uuid.Must(uuid.NewV7()).String()

	// Удержание: available -> reserved.
	reserved, err := client.Reserve(ctx, &ledgerv1.ReserveRequest{
		IdempotencyKey: refID + ":reserve",
		AccountId:      merchant.ID,
		Amount:         money(40_000),
		RefId:          refID,
	})
	require.NoError(t, err)
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_ACTIVE, reserved.GetStatus())
	require.NotEmpty(t, reserved.GetReserveId())

	replay, err := client.Reserve(ctx, &ledgerv1.ReserveRequest{
		IdempotencyKey: refID + ":reserve",
		AccountId:      merchant.ID,
		Amount:         money(40_000),
		RefId:          refID,
	})
	require.NoError(t, err)
	assert.Equal(t, reserved.GetReserveId(), replay.GetReserveId(), "повтор ключа идемпотентен")

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(60_000), balance.Available)
	assert.Equal(t, int64(40_000), balance.Reserved)

	// Захват: проводка Dr merchant / Cr payout, удержание снято.
	committed, err := client.CommitReserve(ctx, &ledgerv1.CommitReserveRequest{
		IdempotencyKey: refID + ":commit",
		ReserveId:      reserved.GetReserveId(),
	})
	require.NoError(t, err)
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_COMMITTED, committed.GetStatus())

	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(60_000), balance.Available)
	assert.Zero(t, balance.Reserved)

	var payout storage.Account
	require.NoError(t, db.Where("type = ? AND asset = ?", "payout", "USDT").
		Where("merchant_ref = ?", *merchant.MerchantRef).First(&payout).Error)
	var payoutBalance storage.Balance
	require.NoError(t, db.Where("account_id = ?", payout.ID).First(&payoutBalance).Error)
	assert.Equal(t, int64(40_000), payoutBalance.Available)

	var stored storage.Reserve
	require.NoError(t, db.Where("id = ?", reserved.GetReserveId()).First(&stored).Error)
	require.NotNil(t, stored.TransactionID)
	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).
		Where("transaction_id = ?", *stored.TransactionID).Count(&entries).Error)
	assert.Equal(t, int64(2), entries, "захват создает сбалансированную пару проводок")

	// Отмена второй выплаты: компенсация возвращает средства.
	cancelRef := "payout-" + uuid.Must(uuid.NewV7()).String()
	second, err := client.Reserve(ctx, &ledgerv1.ReserveRequest{
		IdempotencyKey: cancelRef + ":reserve",
		AccountId:      merchant.ID,
		Amount:         money(10_000),
		RefId:          cancelRef,
	})
	require.NoError(t, err)
	released, err := client.ReleaseReserve(ctx, &ledgerv1.ReleaseReserveRequest{
		IdempotencyKey: cancelRef + ":release",
		ReserveId:      second.GetReserveId(),
		Reason:         "gateway timeout",
	})
	require.NoError(t, err)
	assert.Equal(t, ledgerv1.ReserveStatus_RESERVE_STATUS_RELEASED, released.GetStatus())

	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Equal(t, int64(60_000), balance.Available)
	assert.Zero(t, balance.Reserved)

	// Захват освобожденного резерва отклоняется.
	_, err = client.CommitReserve(ctx, &ledgerv1.CommitReserveRequest{
		IdempotencyKey: cancelRef + ":commit",
		ReserveId:      second.GetReserveId(),
	})
	require.Error(t, err, "захват released резерва должен быть отклонен")
}

func TestBalanceAndEntriesFlow(t *testing.T) {
	db := openLedgerDB(t)
	client := openLedgerClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	merchant := seedE2EAccount(t, db, "merchant", 50_000)
	fee := seedE2EAccount(t, db, "fee", 0)
	refID := "pay-" + uuid.Must(uuid.NewV7()).String()

	posted, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: refID + ":completed",
		RefType:        "payment",
		RefId:          refID,
		Entries: []*ledgerv1.EntryInput{
			{AccountId: merchant.ID, Direction: commonv1.Direction_DIRECTION_DEBIT, Amount: money(10_000)},
			{AccountId: fee.ID, Direction: commonv1.Direction_DIRECTION_CREDIT, Amount: money(10_000)},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, posted.GetTransactionId())

	balance, err := client.GetBalance(ctx, &ledgerv1.GetBalanceRequest{AccountId: merchant.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(40_000), balance.GetBalance().GetAvailable().GetMinorUnits())
	assert.Equal(t, "USDT", balance.GetBalance().GetAvailable().GetAsset())

	// Вторая проводка меняет текущий баланс; повтор первой обязан вернуть снимок.
	secondRef := "pay-" + uuid.Must(uuid.NewV7()).String()
	_, err = client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: secondRef + ":completed",
		RefType:        "payment",
		RefId:          secondRef,
		Entries: []*ledgerv1.EntryInput{
			{AccountId: merchant.ID, Direction: commonv1.Direction_DIRECTION_DEBIT, Amount: money(5_000)},
			{AccountId: fee.ID, Direction: commonv1.Direction_DIRECTION_CREDIT, Amount: money(5_000)},
		},
	})
	require.NoError(t, err)

	replay, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: refID + ":completed",
		RefType:        "payment",
		RefId:          refID,
		Entries: []*ledgerv1.EntryInput{
			{AccountId: merchant.ID, Direction: commonv1.Direction_DIRECTION_DEBIT, Amount: money(10_000)},
			{AccountId: fee.ID, Direction: commonv1.Direction_DIRECTION_CREDIT, Amount: money(10_000)},
		},
	})
	require.NoError(t, err)
	for _, snapshot := range replay.GetBalancesAfter() {
		if snapshot.GetAccountId() == merchant.ID {
			assert.Equal(t, int64(40_000), snapshot.GetAvailable().GetMinorUnits(),
				"повтор возвращает снимок balances_after")
		}
	}

	// Выписка: две проводки по счету, курсорная пагинация по одной записи.
	page, err := client.ListEntries(ctx, &ledgerv1.ListEntriesRequest{AccountId: merchant.ID, Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.GetEntries(), 1)
	assert.NotEmpty(t, page.GetNextCursor())
	assert.Equal(t, commonv1.Direction_DIRECTION_DEBIT, page.GetEntries()[0].GetDirection())
	assert.Equal(t, "payment", page.GetEntries()[0].GetRefType())

	tail, err := client.ListEntries(ctx, &ledgerv1.ListEntriesRequest{
		AccountId: merchant.ID,
		Limit:     1,
		Cursor:    page.GetNextCursor(),
	})
	require.NoError(t, err)
	require.Len(t, tail.GetEntries(), 1)
	assert.Empty(t, tail.GetNextCursor())
	assert.Greater(t, tail.GetEntries()[0].GetEntryId(), page.GetEntries()[0].GetEntryId())
}
