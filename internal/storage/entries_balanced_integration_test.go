//go:build integration

package storage_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/storage"
)

// insertRawTransaction inserts a transaction bypassing the application to verify the database barrier.
func insertRawTransaction(tx *gorm.DB, key string) (string, error) {
	transactionID := uuid.Must(uuid.NewV7()).String()
	err := tx.Exec(
		"INSERT INTO transactions (id, idempotency_key, ref_type, ref_id, request_hash) VALUES (?, ?, 'test', 't-1', 'h')",
		transactionID, key).Error
	return transactionID, err
}

func TestIntegrationUnbalancedEntriesRejectedByTrigger(t *testing.T) {
	db := setupLedgerDB(t)
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	err := db.Transaction(func(tx *gorm.DB) error {
		transactionID, insertErr := insertRawTransaction(tx, "trigger-unbalanced")
		if insertErr != nil {
			return insertErr
		}
		return tx.Exec(
			"INSERT INTO entries (transaction_id, account_id, direction, amount) VALUES (?, ?, 'debit', 10), (?, ?, 'credit', 9)",
			transactionID, merchant.ID, transactionID, fee.ID).Error
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unbalanced")

	var transactions, entries int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Zero(t, transactions)
	assert.Zero(t, entries)
}

func TestIntegrationSingleEntryRejectedByTrigger(t *testing.T) {
	db := setupLedgerDB(t)
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)

	err := db.Transaction(func(tx *gorm.DB) error {
		transactionID, insertErr := insertRawTransaction(tx, "trigger-single")
		if insertErr != nil {
			return insertErr
		}
		return tx.Exec(
			"INSERT INTO entries (transaction_id, account_id, direction, amount) VALUES (?, ?, 'debit', 10)",
			transactionID, merchant.ID).Error
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least two")

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Count(&entries).Error)
	assert.Zero(t, entries)
}

func TestIntegrationBalancedDirectInsertAllowed(t *testing.T) {
	db := setupLedgerDB(t)
	merchant := seedAccount(t, db, "merchant", "USDT", 2, "m-1", 100)
	fee := seedAccount(t, db, "fee", "USDT", 2, "", 0)

	var transactionID string
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		var insertErr error
		transactionID, insertErr = insertRawTransaction(tx, "trigger-balanced")
		if insertErr != nil {
			return insertErr
		}
		return tx.Exec(
			"INSERT INTO entries (transaction_id, account_id, direction, amount) VALUES (?, ?, 'debit', 10), (?, ?, 'credit', 10)",
			transactionID, merchant.ID, transactionID, fee.ID).Error
	}))

	var entries int64
	require.NoError(t, db.Model(&storage.Entry{}).Where("transaction_id = ?", transactionID).Count(&entries).Error)
	assert.Equal(t, int64(2), entries)
}
