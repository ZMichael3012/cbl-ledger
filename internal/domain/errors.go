package domain

import "errors"

// Ошибки домена леджера; транспорт маппит их в gRPC-статусы.
var (
	// ErrInvalidTransaction сообщает о некорректной транзакции в целом.
	ErrInvalidTransaction = errors.New("ledger: invalid transaction")
	// ErrInvalidEntry сообщает о некорректной проводке.
	ErrInvalidEntry = errors.New("ledger: invalid entry")
	// ErrUnbalanced сообщает о несбалансированной транзакции.
	ErrUnbalanced = errors.New("ledger: debits and credits are not balanced")
	// ErrUnknownAccount сообщает о несуществующем счете.
	ErrUnknownAccount = errors.New("ledger: unknown account")
	// ErrAssetMismatch сообщает о разных активах или decimals в одной транзакции.
	ErrAssetMismatch = errors.New("ledger: account asset mismatch")
	// ErrInsufficientFunds сообщает о недостатке средств на счете.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	// ErrConflict сообщает об исчерпании попыток оптимистичной блокировки.
	ErrConflict = errors.New("ledger: concurrent update conflict")
	// ErrIdempotencyReuse сообщает о повторе ключа идемпотентности с другим телом.
	ErrIdempotencyReuse = errors.New("ledger: idempotency key reuse with different payload")
)
