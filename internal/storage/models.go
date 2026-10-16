// Package storage provides access to the ledger service storages:
// GORM models, migrations, and the posting repository.
package storage

import "time"

// Account describes a row of the accounts table.
type Account struct {
	// ID sets the account identifier: UUIDv7 or the deterministic UUIDv5 of the account plan (2.6).
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// Type sets the account type of the account plan.
	Type string `gorm:"column:type"`
	// Asset sets the account asset code.
	Asset string `gorm:"column:asset"`
	// Decimals sets the number of decimal places.
	Decimals int32 `gorm:"column:decimals"`
	// MerchantRef sets the merchant reference for merchant accounts.
	MerchantRef *string `gorm:"column:merchant_ref"`
	// CreatedAt sets the account creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the accounts table name.
func (Account) TableName() string { return "accounts" }

// Transaction describes a row of the transactions table.
type Transaction struct {
	// ID sets the transaction UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// IdempotencyKey sets the transaction idempotency key.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RefType sets the reference type of the business operation.
	RefType string `gorm:"column:ref_type"`
	// RefID sets the identifier of the business operation.
	RefID string `gorm:"column:ref_id"`
	// RequestHash sets the canonical hash of the request body for key reuse control.
	RequestHash string `gorm:"column:request_hash"`
	// CreatedAt sets the posting time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the transactions table name.
func (Transaction) TableName() string { return "transactions" }

// Entry describes a row of the entries table; the table is append-only.
type Entry struct {
	// ID sets the monotonic entry identifier.
	ID int64 `gorm:"column:id;primaryKey"`
	// TransactionID sets the transaction of the entry.
	TransactionID string `gorm:"column:transaction_id;type:uuid"`
	// AccountID sets the account of the entry.
	AccountID string `gorm:"column:account_id;type:uuid"`
	// Direction sets the direction of the entry.
	Direction string `gorm:"column:direction"`
	// Amount sets the amount in asset minor units.
	Amount int64 `gorm:"column:amount"`
	// CreatedAt sets the entry time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the entries table name.
func (Entry) TableName() string { return "entries" }

// Balance describes a row of the balances table with optimistic locking.
type Balance struct {
	// AccountID sets the account of the balance.
	AccountID string `gorm:"column:account_id;type:uuid;primaryKey"`
	// Available sets the available balance in minor units.
	Available int64 `gorm:"column:available"`
	// Reserved sets the reserved balance in minor units.
	Reserved int64 `gorm:"column:reserved"`
	// LockVersion sets the optimistic lock version.
	LockVersion int64 `gorm:"column:lock_version"`
	// UpdatedAt sets the last modification time.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName sets the balances table name.
func (Balance) TableName() string { return "balances" }

// TransactionBalance describes a snapshot of the account balance after an entry.
type TransactionBalance struct {
	// TransactionID sets the transaction of the snapshot.
	TransactionID string `gorm:"column:transaction_id;type:uuid;primaryKey"`
	// AccountID sets the account of the snapshot.
	AccountID string `gorm:"column:account_id;type:uuid;primaryKey"`
	// Available sets the available balance after the entry.
	Available int64 `gorm:"column:available"`
	// Reserved sets the reserved balance after the entry.
	Reserved int64 `gorm:"column:reserved"`
	// CreatedAt sets the snapshot time (for future partitioning and retention).
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the transaction_balances table name.
func (TransactionBalance) TableName() string { return "transaction_balances" }

// Reserve describes a row of the reserves table: a hold of funds for an external operation.
type Reserve struct {
	// ID sets the reserve UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// AccountID sets the hold account.
	AccountID string `gorm:"column:account_id;type:uuid"`
	// Amount sets the hold amount in asset minor units.
	Amount int64 `gorm:"column:amount"`
	// RefID sets the hold business reference (for example, payout_id).
	RefID string `gorm:"column:ref_id"`
	// Status sets the reserve state: active, committed, or released.
	Status string `gorm:"column:status"`
	// IdempotencyKey sets the idempotency key of the reserve creation.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash sets the canonical hash of the reserve request body.
	RequestHash string `gorm:"column:request_hash"`
	// Reason sets the reason for the reserve release.
	Reason string `gorm:"column:reason"`
	// TransactionID sets the posting of the reserve capture (for committed).
	TransactionID *string `gorm:"column:transaction_id;type:uuid"`
	// CounterpartyAccountID sets the payout account that received the funds on capture.
	CounterpartyAccountID *string `gorm:"column:counterparty_account_id;type:uuid"`
	// CreatedAt sets the reserve creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt sets the last modification time of the reserve.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName sets the reserves table name.
func (Reserve) TableName() string { return "reserves" }

// ReserveOperation describes a row of the reserve_operations table: a capture or release key.
type ReserveOperation struct {
	// ID sets the monotonic operation identifier.
	ID int64 `gorm:"column:id;primaryKey"`
	// ReserveID sets the reserve of the operation.
	ReserveID string `gorm:"column:reserve_id;type:uuid"`
	// Op sets the operation kind: commit or release.
	Op string `gorm:"column:op"`
	// IdempotencyKey sets the operation idempotency key.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash sets the canonical hash of the operation body.
	RequestHash string `gorm:"column:request_hash"`
	// CreatedAt sets the operation time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the reserve_operations table name.
func (ReserveOperation) TableName() string { return "reserve_operations" }
