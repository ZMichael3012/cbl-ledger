// Пакет storage содержит доступ к хранилищам сервиса ledger:
// GORM-модели, миграции, репозиторий проводок.
package storage

import "time"

// Account описывает строку таблицы accounts.
type Account struct {
	// ID задает идентификатор счета: UUIDv7 или детерминированный UUIDv5 плана счетов (2.6).
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

// TransactionBalance описывает снимок баланса счета после проводки.
type TransactionBalance struct {
	// TransactionID задает транзакцию снимка.
	TransactionID string `gorm:"column:transaction_id;type:uuid;primaryKey"`
	// AccountID задает счет снимка.
	AccountID string `gorm:"column:account_id;type:uuid;primaryKey"`
	// Available задает доступный остаток после проводки.
	Available int64 `gorm:"column:available"`
	// Reserved задает зарезервированный остаток после проводки.
	Reserved int64 `gorm:"column:reserved"`
	// CreatedAt задает время снимка (для будущего партиционирования и ретеншена).
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы transaction_balances.
func (TransactionBalance) TableName() string { return "transaction_balances" }

// Reserve описывает строку таблицы reserves: удержание средств под внешнюю операцию.
type Reserve struct {
	// ID задает UUIDv7 резерва.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// AccountID задает счет удержания.
	AccountID string `gorm:"column:account_id;type:uuid"`
	// Amount задает сумму удержания в минимальных единицах актива.
	Amount int64 `gorm:"column:amount"`
	// RefID задает бизнес-ссылку удержания (например, payout_id).
	RefID string `gorm:"column:ref_id"`
	// Status задает состояние резерва: active, committed или released.
	Status string `gorm:"column:status"`
	// IdempotencyKey задает ключ идемпотентности создания резерва.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash задает канонический хэш тела запроса резерва.
	RequestHash string `gorm:"column:request_hash"`
	// Reason задает причину возврата резерва.
	Reason string `gorm:"column:reason"`
	// TransactionID задает проводку захвата резерва (для committed).
	TransactionID *string `gorm:"column:transaction_id;type:uuid"`
	// CounterpartyAccountID задает счет выплат, получивший средства при захвате.
	CounterpartyAccountID *string `gorm:"column:counterparty_account_id;type:uuid"`
	// CreatedAt задает время создания резерва.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt задает время последнего изменения резерва.
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

// TableName задает имя таблицы reserves.
func (Reserve) TableName() string { return "reserves" }

// ReserveOperation описывает строку таблицы reserve_operations: ключ захвата или возврата.
type ReserveOperation struct {
	// ID задает монотонный идентификатор операции.
	ID int64 `gorm:"column:id;primaryKey"`
	// ReserveID задает резерв операции.
	ReserveID string `gorm:"column:reserve_id;type:uuid"`
	// Op задает вид операции: commit или release.
	Op string `gorm:"column:op"`
	// IdempotencyKey задает ключ идемпотентности операции.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash задает канонический хэш тела операции.
	RequestHash string `gorm:"column:request_hash"`
	// CreatedAt задает время операции.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы reserve_operations.
func (ReserveOperation) TableName() string { return "reserve_operations" }
