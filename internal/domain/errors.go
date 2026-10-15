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
	// ErrInvalidReserve сообщает о некорректном запросе резерва.
	ErrInvalidReserve = errors.New("ledger: invalid reserve request")
	// ErrUnknownReserve сообщает о несуществующем резерве.
	ErrUnknownReserve = errors.New("ledger: unknown reserve")
	// ErrReserveState сообщает о недопустимом переходе состояния резерва.
	ErrReserveState = errors.New("ledger: reserve in invalid state")
	// ErrReserveNotAllowed сообщает, что тип счета не поддерживает резервирование.
	ErrReserveNotAllowed = errors.New("ledger: reserve is not allowed for account")
	// ErrInvalidCursor сообщает о некорректном курсоре выписки.
	ErrInvalidCursor = errors.New("ledger: invalid cursor")
	// ErrInvalidAccountPlan сообщает о нарушении правил плана счетов.
	ErrInvalidAccountPlan = errors.New("ledger: invalid account plan key")
	// ErrAccountConflict сообщает о существующем счете с другими характеристиками.
	ErrAccountConflict = errors.New("ledger: account already exists with different parameters")
	// ErrInvalidQuery сообщает о некорректных параметрах выписки или списка счетов.
	ErrInvalidQuery = errors.New("ledger: invalid query")
	// ErrTooManyResults сообщает о превышении серверного предела выдачи.
	ErrTooManyResults = errors.New("ledger: too many results")
)
