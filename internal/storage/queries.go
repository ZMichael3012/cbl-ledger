package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// Statement limits: limit 0 is replaced by the default value, and a larger
// value is clamped to the ceiling. The merchant account list is bounded by a
// server limit: the number of plan accounts is assets per merchant, and
// exceeding the limit is an account plan anomaly (ErrTooManyResults).
const (
	defaultEntriesLimit = 50
	maxEntriesLimit     = 200
	maxMerchantAccounts = 1000
)

// AccountBalanceView describes an account balance for reading.
type AccountBalanceView struct {
	// AccountID sets the account.
	AccountID string
	// Asset sets the account asset code.
	Asset string
	// Decimals sets the number of decimal places.
	Decimals int32
	// Available sets the available balance.
	Available int64
	// Reserved sets the reserved balance.
	Reserved int64
}

// EntryView describes a statement entry with the transaction business reference.
type EntryView struct {
	// EntryID sets the monotonic identifier of the entry.
	EntryID int64 `gorm:"column:entry_id"`
	// TransactionID sets the transaction of the entry.
	TransactionID string `gorm:"column:transaction_id"`
	// AccountID sets the account of the entry.
	AccountID string `gorm:"column:account_id"`
	// Direction sets the direction of the entry.
	Direction domain.Direction `gorm:"column:direction"`
	// Amount sets the amount in asset minor units.
	Amount int64 `gorm:"column:amount"`
	// Asset sets the account asset code.
	Asset string `gorm:"column:asset"`
	// Decimals sets the number of decimal places.
	Decimals int32 `gorm:"column:decimals"`
	// RefType sets the transaction business reference type.
	RefType string `gorm:"column:ref_type"`
	// RefID sets the transaction business reference identifier.
	RefID string `gorm:"column:ref_id"`
	// CreatedAt sets the entry time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// MerchantAccountView describes a merchant account with balance for BFF statements (2.10).
type MerchantAccountView struct {
	// AccountID sets the account.
	AccountID string
	// Type sets the account type of the account plan: merchant or payout.
	Type string
	// Asset sets the account asset code.
	Asset string
	// Decimals sets the number of decimal places.
	Decimals int32
	// Available sets the available balance.
	Available int64
	// Reserved sets the reserved balance.
	Reserved int64
}

// EntryFilter describes the statement filter (2.10): account_id or merchant_ref
// is filled in; the period applies to the transaction created_at.
type EntryFilter struct {
	// AccountID sets the statement account; empty for a statement by merchant_ref.
	AccountID string
	// MerchantRef sets the merchant account filter (merchant and payout).
	MerchantRef string
	// PeriodFrom sets the inclusive lower period bound (nil means no bound).
	PeriodFrom *time.Time
	// PeriodTo sets the exclusive upper period bound (nil means no bound).
	PeriodTo *time.Time
	// Limit sets the page size; 0 means the default value.
	Limit uint32
	// Cursor sets the continuation cursor (the entry_id of the last record).
	Cursor string
}

// merchantAccountTypes sets the account types visible to a merchant in statements and balances.
var merchantAccountTypes = [2]string{string(domain.AccountTypeMerchant), string(domain.AccountTypePayout)}

// GetBalance returns an account balance; an unknown account or a missing
// balance row is rejected with ErrUnknownAccount.
func (r *Repository) GetBalance(ctx context.Context, accountID string) (AccountBalanceView, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(accountID))
	if err != nil {
		return AccountBalanceView{}, fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, accountID)
	}
	db := r.db.WithContext(ctx)
	account, err := loadAccountByID(db, parsed.String())
	if err != nil {
		return AccountBalanceView{}, err
	}
	balance, err := loadBalance(db, account.ID)
	if err != nil {
		return AccountBalanceView{}, err
	}
	return AccountBalanceView{
		AccountID: account.ID,
		Asset:     account.Asset,
		Decimals:  account.Decimals,
		Available: balance.Available,
		Reserved:  balance.Reserved,
	}, nil
}

// ListMerchantAccounts returns merchant accounts with balances in ascending
// (asset, type) order; accounts without a balance row are returned with zeros
// (the provisioning anomaly is not masked only by the point-wise GetBalance,
// which answers with not found). An empty or invalid reference is rejected with
// ErrInvalidQuery: a list without a merchant is forbidden (fail-closed);
// exceeding the server limit of accounts yields ErrTooManyResults.
func (r *Repository) ListMerchantAccounts(ctx context.Context, merchantRef string) ([]MerchantAccountView, error) {
	trimmed := strings.TrimSpace(merchantRef)
	if !domain.ValidMerchantRef(trimmed) {
		return nil, fmt.Errorf("%w: merchant_ref %q", domain.ErrInvalidQuery, merchantRef)
	}
	db := r.db.WithContext(ctx)
	rows := make([]MerchantAccountView, 0)
	err := db.Table("accounts AS a").
		Select("a.id AS account_id, a.type, a.asset, a.decimals, "+
			"COALESCE(b.available, 0) AS available, COALESCE(b.reserved, 0) AS reserved").
		Joins("LEFT JOIN balances AS b ON b.account_id = a.id").
		Where("a.merchant_ref = ?", trimmed).
		Where("a.type IN ?", merchantAccountTypes[:]).
		Order("a.asset, a.type").
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("storage: list merchant accounts: %w", err)
	}
	if len(rows) > maxMerchantAccounts {
		return nil, fmt.Errorf("%w: %d accounts for merchant", domain.ErrTooManyResults, len(rows))
	}
	return rows, nil
}

// ListEntries returns a statement page in ascending entry_id order. account_id
// or merchant_ref is filled in (at least one): merchant_ref limits the statement
// to merchant accounts (merchant and payout), while an account_id outside the set
// is rejected with ErrUnknownAccount without revealing existence (fail-closed).
// The period filters the transaction created_at: from inclusive, to exclusive.
// The cursor is the decimal entry_id of the last record of the previous page;
// nextCursor is non-empty only if there are newer records beyond the page.
func (r *Repository) ListEntries(ctx context.Context, filter EntryFilter) ([]EntryView, string, error) {
	accountID := strings.TrimSpace(filter.AccountID)
	merchantRef := strings.TrimSpace(filter.MerchantRef)
	if accountID == "" && merchantRef == "" {
		return nil, "", fmt.Errorf("%w: account_id or merchant_ref required", domain.ErrInvalidQuery)
	}
	if merchantRef != "" && !domain.ValidMerchantRef(merchantRef) {
		return nil, "", fmt.Errorf("%w: merchant_ref %q", domain.ErrInvalidQuery, filter.MerchantRef)
	}
	if filter.PeriodFrom != nil && filter.PeriodTo != nil && !filter.PeriodTo.After(*filter.PeriodFrom) {
		return nil, "", fmt.Errorf("%w: period_to must be after period_from", domain.ErrInvalidQuery)
	}
	afterID, err := parseCursor(filter.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := filter.Limit
	if limit == 0 {
		limit = defaultEntriesLimit
	}
	if limit > maxEntriesLimit {
		limit = maxEntriesLimit
	}

	db := r.db.WithContext(ctx)
	query := db.Table("entries AS e").
		Select("e.id AS entry_id, e.transaction_id, e.account_id, e.direction, e.amount, " +
			"t.created_at, t.ref_type, t.ref_id, a.asset, a.decimals").
		Joins("JOIN transactions AS t ON t.id = e.transaction_id").
		Joins("JOIN accounts AS a ON a.id = e.account_id")
	if accountID != "" {
		parsed, parseErr := uuid.Parse(accountID)
		if parseErr != nil {
			return nil, "", fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, filter.AccountID)
		}
		account, loadErr := loadAccountByID(db, parsed.String())
		if loadErr != nil {
			return nil, "", loadErr
		}
		if merchantRef != "" && (account.MerchantRef == nil || *account.MerchantRef != merchantRef ||
			(account.Type != string(domain.AccountTypeMerchant) && account.Type != string(domain.AccountTypePayout))) {
			// A foreign or platform account answers as not found: the existence
			// of accounts of other merchants is not revealed.
			return nil, "", fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, filter.AccountID)
		}
		query = query.Where("e.account_id = ?", parsed.String())
	}
	if merchantRef != "" {
		// A merchant statement covers only its account plan accounts (merchant
		// and payout); platform accounts (clearing/fee/suspense) are unavailable.
		query = query.Where("a.merchant_ref = ?", merchantRef).
			Where("a.type IN ?", merchantAccountTypes[:])
	}
	if filter.PeriodFrom != nil {
		query = query.Where("t.created_at >= ?", filter.PeriodFrom.UTC())
	}
	if filter.PeriodTo != nil {
		query = query.Where("t.created_at < ?", filter.PeriodTo.UTC())
	}
	if afterID > 0 {
		query = query.Where("e.id > ?", afterID)
	}
	// limit+1 distinguishes "the page is over" from "there is a continuation";
	// the ceiling is clamped above, so an int overflow is impossible.
	rows := make([]EntryView, 0, limit+1)
	if err := query.Order("e.id").Limit(int(limit) + 1).Scan(&rows).Error; err != nil {
		return nil, "", fmt.Errorf("storage: list entries: %w", err)
	}
	var nextCursor string
	if len(rows) > int(limit) {
		rows = rows[:limit]
		nextCursor = strconv.FormatInt(rows[len(rows)-1].EntryID, 10)
	}
	for i := range rows {
		rows[i].Direction = domain.Direction(rows[i].Direction)
	}
	return rows, nextCursor, nil
}

// parseCursor converts the opaque cursor into an entry_id; 0 means the start of the statement.
func parseCursor(cursor string) (int64, error) {
	trimmed := strings.TrimSpace(cursor)
	if trimmed == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%w: %q", domain.ErrInvalidCursor, cursor)
	}
	return value, nil
}
