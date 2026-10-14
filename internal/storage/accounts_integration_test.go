//go:build integration

package storage_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func TestIntegrationEnsureAccountCreatesDerived(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	ctx := context.Background()

	merchant, err := repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, "58686062-c133-5d4e-b08b-0d33a0db924f", merchant.ID, "golden-вектор плана счетов")
	assert.Equal(t, "USDT", merchant.Asset)
	assert.Equal(t, int32(2), merchant.Decimals)
	require.NotNil(t, merchant.MerchantRef)
	assert.Equal(t, "m-1", *merchant.MerchantRef)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", merchant.ID).First(&balance).Error)
	assert.Zero(t, balance.Available)
	assert.Zero(t, balance.Reserved)

	replay, err := repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, merchant.ID, replay.ID, "повторный провижининг идемпотентен")

	fee, err := repo.EnsureAccount(ctx, domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, "b138b712-5940-59bc-82bd-19f653642227", fee.ID)
	assert.Nil(t, fee.MerchantRef, "платформенный счет без ссылки на мерчанта")

	var accounts int64
	require.NoError(t, db.Model(&storage.Account{}).Count(&accounts).Error)
	assert.Equal(t, int64(2), accounts, "повтор не создает дублей")
}

func TestIntegrationEnsureAccountConcurrent(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	const workers = 10
	var wg sync.WaitGroup
	ids := make([]string, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			account, err := repo.EnsureAccount(context.Background(), domain.MerchantAccountKey("m-concurrent", "USDT", 2))
			errs[idx] = err
			ids[idx] = account.ID
		}(i)
	}
	wg.Wait()

	for i := 0; i < workers; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, ids[0], ids[i], "все конкурентные вызовы возвращают один счет")
	}
	var accounts int64
	require.NoError(t, db.Model(&storage.Account{}).
		Where("merchant_ref = ?", "m-concurrent").Count(&accounts).Error)
	assert.Equal(t, int64(1), accounts)
}

func TestIntegrationEnsureAccountConcurrentMixedDecimals(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())

	// Гонка с разными decimals: победившая вставка занимает бизнес-ключ,
	// проигравшие обязаны получить account_conflict, а не сырую ошибку БД.
	const workers = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			decimals := int32(2)
			if idx%2 == 1 {
				decimals = 6
			}
			_, errs[idx] = repo.EnsureAccount(context.Background(), domain.MerchantAccountKey("m-mixed", "USDT", decimals))
		}(i)
	}
	wg.Wait()

	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			assert.ErrorIs(t, errs[i], domain.ErrAccountConflict, "только конфликт плана, worker %d: %v", i, errs[i])
		}
	}
	var accounts int64
	require.NoError(t, db.Model(&storage.Account{}).
		Where("merchant_ref = ?", "m-mixed").Count(&accounts).Error)
	assert.Equal(t, int64(1), accounts)

	var balances int64
	require.NoError(t, db.Model(&storage.Balance{}).
		Where("account_id IN (?)", db.Model(&storage.Account{}).
			Select("id").Where("merchant_ref = ?", "m-mixed")).Count(&balances).Error)
	assert.Equal(t, int64(1), balances, "балансовый ряд только у канонического счета")
}

func TestIntegrationEnsureAccountConflicts(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	ctx := context.Background()

	_, err := repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)

	_, err = repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-1", "USDT", 6))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrAccountConflict, "другая шкала того же ключа - конфликт")

	_, err = repo.EnsureAccount(ctx, domain.AccountKey{
		Type: domain.AccountTypeFee, Asset: "USDT", Decimals: 2, MerchantRef: "m-1",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidAccountPlan, "платформенный счет не принимает ref")

	_, err = repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-1", "usdt", 2))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidAccountPlan)

	var accounts int64
	require.NoError(t, db.Model(&storage.Account{}).Count(&accounts).Error)
	assert.Equal(t, int64(1), accounts, "отказы не создают счетов")
}

func TestIntegrationEnsureAccountReusesLegacyRow(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	legacy := seedAccount(t, db, "merchant", "USDT", 2, "m-legacy", 500)
	ctx := context.Background()

	account, err := repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-legacy", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, legacy.ID, account.ID, "legacy-строка со случайным id переиспользуется")

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", legacy.ID).First(&balance).Error)
	assert.Equal(t, int64(500), balance.Available, "баланс legacy-счета не обнуляется")
}

func TestIntegrationEnsureAccountHealsMissingBalance(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchantRef := "m-broken"
	account := storage.Account{
		ID:          "018f0000-0000-7000-8000-0000000000bb",
		Type:        "merchant",
		Asset:       "USDT",
		Decimals:    2,
		MerchantRef: &merchantRef,
		CreatedAt:   time.Now().UTC(),
	}
	require.NoError(t, db.Create(&account).Error)

	ensured, err := repo.EnsureAccount(context.Background(), domain.MerchantAccountKey("m-broken", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, account.ID, ensured.ID)

	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", account.ID).First(&balance).Error)
	assert.Zero(t, balance.Available, "балансовый ряд досоздан провижинингом")
}

func TestIntegrationEnsureAccountRefusesHealingWithEntries(t *testing.T) {
	db := setupLedgerDB(t)
	repo := storage.NewRepository(db, zap.NewNop())
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-dirty", 1_000)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)
	postPayment(t, repo, "pay-1:completed", "pay-1", merchant.ID, fee.ID, 100)

	// Имитируем порчу: балансовый ряд счета с проводками потерян.
	require.NoError(t, db.Where("account_id = ?", merchant.ID).Delete(&storage.Balance{}).Error)

	_, err := repo.EnsureAccount(context.Background(), domain.MerchantAccountKey("m-dirty", "USDT", 2))
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrUnknownAccount, "нулевой баланс не маскирует порчу данных")

	var balanceCount int64
	require.NoError(t, db.Model(&storage.Balance{}).Where("account_id = ?", merchant.ID).Count(&balanceCount).Error)
	assert.Zero(t, balanceCount, "балансовый ряд не досоздан")
}
