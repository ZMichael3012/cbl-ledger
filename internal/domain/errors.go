package domain

import "errors"

// Ledger domain errors; the transport maps them to gRPC statuses.
var (
	// ErrInvalidTransaction reports an invalid transaction as a whole.
	ErrInvalidTransaction = errors.New("ledger: invalid transaction")
	// ErrInvalidEntry reports an invalid entry.
	ErrInvalidEntry = errors.New("ledger: invalid entry")
	// ErrUnbalanced reports an unbalanced transaction.
	ErrUnbalanced = errors.New("ledger: debits and credits are not balanced")
	// ErrUnknownAccount reports a nonexistent account.
	ErrUnknownAccount = errors.New("ledger: unknown account")
	// ErrAssetMismatch reports different assets or decimals within one transaction.
	ErrAssetMismatch = errors.New("ledger: account asset mismatch")
	// ErrInsufficientFunds reports insufficient funds on the account.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	// ErrConflict reports exhausted optimistic lock attempts.
	ErrConflict = errors.New("ledger: concurrent update conflict")
	// ErrIdempotencyReuse reports a repeated idempotency key with a different payload.
	ErrIdempotencyReuse = errors.New("ledger: idempotency key reuse with different payload")
	// ErrInvalidReserve reports an invalid reserve request.
	ErrInvalidReserve = errors.New("ledger: invalid reserve request")
	// ErrUnknownReserve reports a nonexistent reserve.
	ErrUnknownReserve = errors.New("ledger: unknown reserve")
	// ErrReserveState reports an invalid reserve state transition.
	ErrReserveState = errors.New("ledger: reserve in invalid state")
	// ErrReserveNotAllowed reports that the account type does not support reservation.
	ErrReserveNotAllowed = errors.New("ledger: reserve is not allowed for account")
	// ErrInvalidCursor reports an invalid statement cursor.
	ErrInvalidCursor = errors.New("ledger: invalid cursor")
	// ErrInvalidAccountPlan reports a violation of the account plan rules.
	ErrInvalidAccountPlan = errors.New("ledger: invalid account plan key")
	// ErrAccountConflict reports an existing account with different characteristics.
	ErrAccountConflict = errors.New("ledger: account already exists with different parameters")
	// ErrInvalidQuery reports invalid statement or account list parameters.
	ErrInvalidQuery = errors.New("ledger: invalid query")
	// ErrTooManyResults reports exceeding the server result limit.
	ErrTooManyResults = errors.New("ledger: too many results")
)
