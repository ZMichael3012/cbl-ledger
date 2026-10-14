package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// EnsureAccount возвращает счет по бизнес-ключу плана счетов, создавая его с
// детерминированным идентификатором (ADR-2.6) и нулевым балансом при отсутствии.
// Повторный и конкурентный вызовы идемпотентны; расхождение decimals с существующим
// счетом отклоняется ErrAccountConflict.
func (r *Repository) EnsureAccount(ctx context.Context, key domain.AccountKey) (Account, error) {
	if err := key.Validate(); err != nil {
		return Account{}, err
	}
	var account Account
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		stored, err := ensureAccountTx(tx, key)
		if err != nil {
			return err
		}
		account = stored
		return nil
	})
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

// ensureAccountTx возвращает счет по бизнес-ключу в текущей транзакции, создавая его
// идемпотентно; ON CONFLICT закрывает гонку параллельных создателей, после чего строка
// перечитывается по ключу - победившая вставка (в том числе legacy со случайным id)
// определяет канонический счет и балансовый ряд. Баланс лечится только у счетов без
// проводок: нулевой ряд рядом с существующими entries маскировал бы порчу данных.
func ensureAccountTx(tx *gorm.DB, key domain.AccountKey) (Account, error) {
	account, err := findAccountByKey(tx, key)
	if err == nil {
		if account.Decimals != key.Decimals {
			return Account{}, fmt.Errorf("%w: account %s decimals %d vs %d",
				domain.ErrAccountConflict, account.ID, account.Decimals, key.Decimals)
		}
		if err := ensureBalanceRow(tx, account.ID); err != nil {
			return Account{}, err
		}
		return account, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return Account{}, fmt.Errorf("storage: load %s account by key: %w", key.Type, err)
	}

	accountID, err := domain.DeriveAccountID(key)
	if err != nil {
		return Account{}, err
	}
	account = Account{
		ID:        accountID.String(),
		Type:      string(key.Type),
		Asset:     key.Asset,
		Decimals:  key.Decimals,
		CreatedAt: time.Now().UTC(),
	}
	if key.MerchantRef != "" {
		merchantRef := key.MerchantRef
		account.MerchantRef = &merchantRef
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&account).Error; err != nil {
		return Account{}, fmt.Errorf("storage: insert %s account: %w", key.Type, err)
	}

	// Строку мог создать конкурент (или конфликт погасил вставку legacy-ключом):
	// каноническим считается то, что лежит в БД, а не выведенный идентификатор.
	stored, err := findAccountByKey(tx, key)
	if err != nil {
		return Account{}, fmt.Errorf("storage: load %s account after insert: %w", key.Type, err)
	}
	if stored.Decimals != key.Decimals {
		return Account{}, fmt.Errorf("%w: account %s decimals %d vs %d",
			domain.ErrAccountConflict, stored.ID, stored.Decimals, key.Decimals)
	}
	if err := ensureBalanceRow(tx, stored.ID); err != nil {
		return Account{}, err
	}
	return stored, nil
}

// findAccountByKey ищет счет по бизнес-ключу плана счетов.
func findAccountByKey(tx *gorm.DB, key domain.AccountKey) (Account, error) {
	var account Account
	err := tx.Where("type = ? AND asset = ? AND COALESCE(merchant_ref, '') = ?",
		key.Type, key.Asset, key.MerchantRef).First(&account).Error
	return account, err
}

// ensureBalanceRow создает балансовый ряд счета с нулем, если его нет. Счет с
// проводками, но без балансового ряда - порча данных: провижининг не маскирует
// ее нулем, а отдает ErrUnknownAccount (как loadBalance).
func ensureBalanceRow(tx *gorm.DB, accountID string) error {
	var balance Balance
	err := tx.Where("account_id = ?", accountID).First(&balance).Error
	if err == nil {
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("storage: load balance %s: %w", accountID, err)
	}
	var entries int64
	if err := tx.Model(&Entry{}).Where("account_id = ?", accountID).Count(&entries).Error; err != nil {
		return fmt.Errorf("storage: count entries %s: %w", accountID, err)
	}
	if entries > 0 {
		return fmt.Errorf("%w: account %s has entries but no balance row", domain.ErrUnknownAccount, accountID)
	}
	created := Balance{AccountID: accountID, UpdatedAt: time.Now().UTC()}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&created).Error; err != nil {
		return fmt.Errorf("storage: insert balance for account %s: %w", accountID, err)
	}
	return nil
}
