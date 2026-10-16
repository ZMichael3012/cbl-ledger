// Package usecase contains the use cases of the ledger service:
// orchestration of domain rules, transactions and event publication.
package usecase

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// PostTransactionRequest describes a double-entry transaction request.
type PostTransactionRequest struct {
	// IdempotencyKey sets the idempotency key.
	IdempotencyKey string
	// RefType sets the type of the business reference.
	RefType string
	// RefID sets the identifier of the business reference.
	RefID string
	// Entries sets the entries of the transaction.
	Entries []domain.EntryInput
}

// Ledger describes the ledger use cases.
type Ledger struct {
	repo *storage.Repository
	log  *zap.Logger
}

// NewLedger creates a Ledger.
func NewLedger(repo *storage.Repository, log *zap.Logger) *Ledger {
	if log == nil {
		log = zap.NewNop()
	}
	return &Ledger{repo: repo, log: log}
}

// PostTransaction performs a balanced double-entry transaction
// with key-based idempotency.
func (l *Ledger) PostTransaction(ctx context.Context, req PostTransactionRequest) (storage.PostTransactionResult, error) {
	return l.repo.PostTransaction(ctx, req.IdempotencyKey, req.RefType, req.RefID, req.Entries)
}

// ReserveRequest describes a request to hold funds on a merchant account.
type ReserveRequest struct {
	// IdempotencyKey sets the idempotency key.
	IdempotencyKey string
	// AccountID sets the hold account.
	AccountID string
	// Asset sets the amount asset to be checked against the account.
	Asset string
	// Decimals sets the number of decimal places to be checked against the account.
	Decimals int32
	// Amount sets the hold amount in minor units of the asset.
	Amount int64
	// RefID sets the business reference of the hold.
	RefID string
}

// CommitReserveRequest describes a reserve commit request.
type CommitReserveRequest struct {
	// IdempotencyKey sets the idempotency key.
	IdempotencyKey string
	// ReserveID sets the reserve to commit.
	ReserveID string
}

// ReleaseReserveRequest describes a reserve release request.
type ReleaseReserveRequest struct {
	// IdempotencyKey sets the idempotency key.
	IdempotencyKey string
	// ReserveID sets the reserve to release.
	ReserveID string
	// Reason sets the release reason.
	Reason string
}

// Reserve creates a hold on funds on a merchant account.
func (l *Ledger) Reserve(ctx context.Context, req ReserveRequest) (storage.ReserveResult, error) {
	return l.repo.Reserve(ctx, storage.ReserveInput{
		IdempotencyKey: req.IdempotencyKey,
		AccountID:      req.AccountID,
		Asset:          req.Asset,
		Decimals:       req.Decimals,
		Amount:         req.Amount,
		RefID:          req.RefID,
	})
}

// CommitReserve converts a hold into an entry to the payout account.
func (l *Ledger) CommitReserve(ctx context.Context, req CommitReserveRequest) (storage.ReserveResult, error) {
	return l.repo.CommitReserve(ctx, req.IdempotencyKey, req.ReserveID)
}

// ReleaseReserve returns held funds to the available balance.
func (l *Ledger) ReleaseReserve(ctx context.Context, req ReleaseReserveRequest) (storage.ReserveResult, error) {
	return l.repo.ReleaseReserve(ctx, req.IdempotencyKey, req.ReserveID, req.Reason)
}

// GetBalance returns the account balance.
func (l *Ledger) GetBalance(ctx context.Context, accountID string) (storage.AccountBalanceView, error) {
	return l.repo.GetBalance(ctx, accountID)
}

// ListEntriesFilter describes the BFF statement filter (2.10): either AccountID
// or MerchantRef is set (at least one); the period applies to the transaction created_at.
type ListEntriesFilter struct {
	// AccountID sets the statement account; empty for a statement by MerchantRef.
	AccountID string
	// MerchantRef sets the filter of merchant accounts (merchant and payout).
	MerchantRef string
	// PeriodFrom sets the inclusive lower bound of the period.
	PeriodFrom *time.Time
	// PeriodTo sets the exclusive upper bound of the period.
	PeriodTo *time.Time
	// Limit sets the page size; 0 means the server default.
	Limit uint32
	// Cursor sets the continuation cursor.
	Cursor string
}

// ListEntries returns a statement page by filter (account, merchant, period)
// with a continuation cursor.
func (l *Ledger) ListEntries(ctx context.Context, filter ListEntriesFilter) ([]storage.EntryView, string, error) {
	return l.repo.ListEntries(ctx, storage.EntryFilter{
		AccountID:   filter.AccountID,
		MerchantRef: filter.MerchantRef,
		PeriodFrom:  filter.PeriodFrom,
		PeriodTo:    filter.PeriodTo,
		Limit:       filter.Limit,
		Cursor:      filter.Cursor,
	})
}

// ListMerchantAccounts returns merchant accounts with balances for BFF statements.
func (l *Ledger) ListMerchantAccounts(ctx context.Context, merchantRef string) ([]storage.MerchantAccountView, error) {
	return l.repo.ListMerchantAccounts(ctx, merchantRef)
}

// EnsureAccount returns the account plan account by business key, creating it
// deterministically when absent (ADR-2.6); used by internal scenarios
// and by consumer 2.7.
func (l *Ledger) EnsureAccount(ctx context.Context, key domain.AccountKey) (storage.Account, error) {
	return l.repo.EnsureAccount(ctx, key)
}
