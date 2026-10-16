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

// EnsureAccount returns an account by the account plan business key, creating
// it with a deterministic identifier (ADR-2.6) and a zero balance when missing.
// Repeated and concurrent calls are idempotent; a decimals mismatch with an
// existing account is rejected with ErrAccountConflict.
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

// ensureAccountTx returns an account by business key in the current transaction,
// creating it idempotently; ON CONFLICT closes the race between parallel creators,
// after which the row is re-read by key - the winning insert (including a legacy
// one with a random id) determines the canonical account and balance row. The
// balance is healed only for accounts without entries: a zero row next to
// existing entries would mask data corruption.
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

	// The row could have been created by a concurrent creator (or the conflict
	// suppressed the insert by a legacy key): the canonical account is the one
	// stored in the DB, not the derived identifier.
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

// findAccountByKey looks up an account by the account plan business key.
func findAccountByKey(tx *gorm.DB, key domain.AccountKey) (Account, error) {
	var account Account
	err := tx.Where("type = ? AND asset = ? AND COALESCE(merchant_ref, '') = ?",
		key.Type, key.Asset, key.MerchantRef).First(&account).Error
	return account, err
}

// ensureBalanceRow creates a zero balance row for an account if missing. An
// account with entries but without a balance row is data corruption: provisioning
// does not mask it with a zero row but returns ErrUnknownAccount (like loadBalance).
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
