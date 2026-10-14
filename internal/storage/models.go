// Пакет storage содержит доступ к хранилищам сервиса ledger:
// GORM-модели, миграции, репозиторий проводок.
package storage

import "time"

// Account описывает строку таблицы accounts.
type Account struct {
	// ID задает UUIDv7 счета.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// Type задает тип счета плана счетов.
	Type string `gorm:"column:type"`
	// Asset задает код актива счета.
	Asset string `gorm:"column:asset"`
	// Decimals задает количество знаков после точки.
	Decimals int32 `gorm:"column:decimals"`
	// MerchantRef задает ссылку на мерчанта для счетов мерчанта.
	MerchantRef *string `gorm:"column:merchant_ref"`
	// CreatedAt задает время создания счета.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы accounts.
func (Account) TableName() string { return "accounts" }

// Transaction описывает строку таблицы transactions.
type Transaction struct {
	// ID задает UUIDv7 транзакции.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// IdempotencyKey задает ключ идемпотентности транзакции.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RefType задает тип ссылки бизнес-операции.
	RefType string `gorm:"column:ref_type"`
	// RefID задает идентификатор бизнес-операции.
	RefID string `gorm:"column:ref_id"`
	// RequestHash задает канонический хэш тела запроса для контроля повтора ключа.
	RequestHash string `gorm:"column:request_hash"`
	// CreatedAt задает время проводки.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы transactions.
func (Transaction) TableName() string { return "transactions" }

// Entry описывает строку таблицы entries; таблица append-only.
type Entry struct {
	// ID задает монотонный идентификатор проводки.
	ID int64 `gorm:"column:id;primaryKey"`
	// TransactionID задает транзакцию проводки.
	TransactionID string `gorm:"column:transaction_id;type:uuid"`
	// AccountID задает счет проводки.
	AccountID string `gorm:"column:account_id;type:uuid"`
	// Direction задает направление проводки.
	Direction string `gorm:"column:direction"`
	// Amount задает сумму в минимальных единицах актива.
	Amount int64 `gorm:"column:amount"`
	// CreatedAt задает время проводки.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы entries.
func (Entry) TableName() string { return "entries" }

// Balance описывает строку таблицы balances с оптимистичной блокировкой.
type Balance struct {
	// AccountID задает счет баланса.
	AccountID string `gorm:"column:account_id;type:uuid;primaryKey"`
	// Available задает доступный остаток в минимальных единицах.
	Available int64 `gorm:"column:available"`
	// Reserved задает зарезервированный остаток в минимальных единицах.
	Reserved int64 `gorm:"column:reserved"`
	// LockVersion задает версию оптимистичной блокировки.
	LockVersion int64 `gorm:"column:lock_version"`
	// UpdatedAt задает время последнего изменения.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName задает имя таблицы balances.
func (Balance) TableName() string { return "balances" }
