package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// RetryPolicy sets the retry policy for concurrent conflicts.
type RetryPolicy struct {
	// MaxAttempts sets the maximum number of posting attempts.
	MaxAttempts int
	// BaseDelay sets the base delay between attempts.
	BaseDelay time.Duration
	// MaxDelay sets the delay ceiling between attempts.
	MaxDelay time.Duration
}

// DefaultRetryPolicy returns the default retry policy:
// 8 attempts, a base of 5 ms, a ceiling of 250 ms, at most ~1 s of total waiting.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 8,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    250 * time.Millisecond,
	}
}

// maxRetryAttemptsCeiling bounds a custom policy from above so that long-lived
// calls (for example, the 2.7 consumer) cannot loop on conflicts.
const maxRetryAttemptsCeiling = 32

// normalizeRetryPolicy replaces invalid policy fields with default values so
// that a zero-value Repository works without explicit configuration. A ceiling
// below the base delay is raised to it, and the attempt count is bounded above.
func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	defaults := DefaultRetryPolicy()
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = defaults.MaxAttempts
	}
	if policy.MaxAttempts > maxRetryAttemptsCeiling {
		policy.MaxAttempts = maxRetryAttemptsCeiling
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = defaults.BaseDelay
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = defaults.MaxDelay
	}
	if policy.MaxDelay < policy.BaseDelay {
		policy.MaxDelay = policy.BaseDelay
	}
	return policy
}

// nextDelay computes the delay before attempt `attempt` with full jitter:
// a uniform value from [0, min(MaxDelay, BaseDelay << attempt)].
// randN is injected for deterministic tests.
func (policy RetryPolicy) nextDelay(attempt int, randN func(int64) int64) time.Duration {
	limit := policy.BaseDelay
	for i := 0; i < attempt && limit < policy.MaxDelay; i++ {
		// Doubling only for a definitely safe value: otherwise int64 overflow.
		if limit > policy.MaxDelay/2 {
			limit = policy.MaxDelay
			break
		}
		limit *= 2
	}
	if limit > policy.MaxDelay {
		limit = policy.MaxDelay
	}
	if limit <= 0 {
		return 0
	}
	bound := int64(limit)
	if bound < math.MaxInt64 {
		bound++
	}
	return time.Duration(randN(bound))
}

// RetryEvent describes a single posting retry for observers.
type RetryEvent struct {
	// Attempt sets the number of the retried attempt, starting from 1.
	Attempt int
	// Cause sets the retry cause: lock_conflict, idempotency_race, deadlock, or unknown.
	Cause string
	// Delay sets the pause before the next attempt.
	Delay time.Duration
}

// RetryObserver receives retry events; it is called before the pause.
// It may be called concurrently from multiple goroutines and must not panic:
// a panic in the observer would bring down request processing.
type RetryObserver func(RetryEvent)

// Option configures the Repository.
type Option func(*Repository)

// WithRetryPolicy sets the retry policy instead of the default one.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(r *Repository) { r.retryPolicy = policy }
}

// WithRetryObserver attaches a retry observer (log, metrics).
func WithRetryObserver(observer RetryObserver) Option {
	return func(r *Repository) { r.retryObserver = observer }
}

// retryCause classifies the retry cause for observers.
func retryCause(err error) string {
	switch {
	case errors.Is(err, errLockConflict):
		return "lock_conflict"
	case errors.Is(err, errIdempotentRace):
		return "idempotency_race"
	case errors.Is(err, errReserveStateRace):
		return "reserve_state_race"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
		return "deadlock"
	}
	return "unknown"
}

// restrictedAccountTypes sets the account types where a negative balance is
// forbidden. Transit accounts (clearing, fee, suspense) may go negative until clearing.
var restrictedAccountTypes = map[string]struct{}{
	string(domain.AccountTypeMerchant): {},
	string(domain.AccountTypePayout):   {},
}

var (
	// errLockConflict reports a lost race for the balance lock_version.
	errLockConflict = errors.New("storage: balance lock conflict")
	// errIdempotentRace reports a concurrent insert of the same idempotency_key.
	errIdempotentRace = errors.New("storage: idempotency race")
)

// BalanceSnapshot describes the account balance after the posting.
type BalanceSnapshot struct {
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

// PostTransactionResult describes the result of a posting.
type PostTransactionResult struct {
	// TransactionID sets the transaction identifier.
	TransactionID string
	// Balances sets the account balances after the posting.
	Balances []BalanceSnapshot
	// Replayed reports that the result was returned for a repeated idempotency key.
	Replayed bool
}

// Repository performs double-entry transactions in PostgreSQL.
type Repository struct {
	db            *gorm.DB
	log           *zap.Logger
	retryPolicy   RetryPolicy
	retryObserver RetryObserver
	eventSink     TxnEventSink
}

// NewRepository creates a Repository; options configure the retry policy and observer.
func NewRepository(db *gorm.DB, log *zap.Logger, opts ...Option) *Repository {
	if log == nil {
		log = zap.NewNop()
	}
	repository := &Repository{db: db, log: log}
	for _, opt := range opts {
		opt(repository)
	}
	return repository
}

// PostTransaction performs a balanced posting with key idempotency and optimistic
// balance locking; on conflicts it retries according to the policy (full jitter,
// delay ceiling). Reusing a key with a different body is rejected with ErrIdempotencyReuse.
func (r *Repository) PostTransaction(ctx context.Context, idempotencyKey, refType, refID string, entries []domain.EntryInput) (PostTransactionResult, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return PostTransactionResult{}, fmt.Errorf("%w: empty idempotency_key", domain.ErrInvalidTransaction)
	}
	normalized, err := normalizeEntries(entries)
	if err != nil {
		return PostTransactionResult{}, err
	}
	if err := domain.ValidateEntries(normalized); err != nil {
		return PostTransactionResult{}, err
	}
	hash := requestHash(refType, refID, normalized)

	policy := normalizeRetryPolicy(r.retryPolicy)
	var lastErr error
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		result, err := r.postOnce(ctx, idempotencyKey, refType, refID, normalized, hash)
		if err == nil {
			return result, nil
		}
		if !isRetryable(err) {
			return PostTransactionResult{}, err
		}
		lastErr = err
		if attempt == policy.MaxAttempts-1 {
			break
		}
		// Full jitter reduces synchronized repeated collisions; cryptographic strength is not needed.
		delay := policy.nextDelay(attempt, rand.Int64N) //nolint:gosec // G404: jitter, not cryptography
		if r.retryObserver != nil {
			r.retryObserver(RetryEvent{Attempt: attempt + 1, Cause: retryCause(err), Delay: delay})
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return PostTransactionResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return PostTransactionResult{}, fmt.Errorf("%w: exceeded %d attempts: %w", domain.ErrConflict, policy.MaxAttempts, lastErr)
}

// normalizeEntries brings account identifiers to the canonical UUID form.
// Without normalization, a UUID in a different case would not match the key
// of the loaded accounts.
func normalizeEntries(entries []domain.EntryInput) ([]domain.EntryInput, error) {
	normalized := make([]domain.EntryInput, len(entries))
	for i := range entries {
		parsed, err := uuid.Parse(strings.TrimSpace(entries[i].AccountID))
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d: account_id %q", domain.ErrInvalidEntry, i, entries[i].AccountID)
		}
		normalized[i] = domain.EntryInput{
			AccountID: parsed.String(),
			Direction: entries[i].Direction,
			Amount:    entries[i].Amount,
		}
	}
	return normalized, nil
}

// requestHash computes the canonical hash of the request body for key reuse control.
// Strings are encoded with a length prefix so that separators inside values do not
// create different bodies with the same hash.
func requestHash(refType, refID string, entries []domain.EntryInput) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s|%d:", len(refType), refType, len(refID), refID, len(entries))
	for i := range entries {
		entry := entries[i]
		_, _ = fmt.Fprintf(digest, "%d:%s|%s|%d;", len(entry.AccountID), entry.AccountID, entry.Direction, entry.Amount)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// isRetryable reports that the error should be retried: a version conflict, a key race, or a deadlock.
func isRetryable(err error) bool {
	if errors.Is(err, errLockConflict) || errors.Is(err, errIdempotentRace) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// postOnce performs a single posting attempt in a database transaction.
func (r *Repository) postOnce(ctx context.Context, idempotencyKey, refType, refID string, entries []domain.EntryInput, hash string) (PostTransactionResult, error) {
	var result PostTransactionResult
	txID, err := uuid.NewV7()
	if err != nil {
		return PostTransactionResult{}, fmt.Errorf("storage: generate transaction id: %w", err)
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		existing, err := findTransaction(tx, idempotencyKey)
		if err == nil {
			if existing.RequestHash != hash {
				return fmt.Errorf("%w: key %q", domain.ErrIdempotencyReuse, idempotencyKey)
			}
			loaded, loadErr := loadResult(tx, existing)
			if loadErr != nil {
				return loadErr
			}
			loaded.Replayed = true
			result = loaded
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		accounts, err := loadAccounts(tx, entries)
		if err != nil {
			return err
		}
		deltas := make(map[string]int64, len(entries))
		for i := range entries {
			switch entries[i].Direction {
			case domain.DirectionCredit:
				deltas[entries[i].AccountID] += entries[i].Amount
			case domain.DirectionDebit:
				deltas[entries[i].AccountID] -= entries[i].Amount
			}
		}

		// Balances are updated before the entries are inserted and in a deterministic
		// order: CAS holds the row lock until commit, so entry identifiers are issued
		// in the order of commits per account (the 2.5 cursor pagination does not skip
		// records), and opposite transactions of two accounts do not produce a 40P01
		// deadlock.
		accountIDs := make([]string, 0, len(deltas))
		for accountID := range deltas {
			accountIDs = append(accountIDs, accountID)
		}
		sort.Strings(accountIDs)

		snapshots := make([]BalanceSnapshot, 0, len(accountIDs))
		for _, accountID := range accountIDs {
			snapshot, err := applyDelta(tx, accounts[accountID], deltas[accountID])
			if err != nil {
				return err
			}
			snapshots = append(snapshots, snapshot)
		}

		transaction := Transaction{
			ID:             txID.String(),
			IdempotencyKey: idempotencyKey,
			RefType:        refType,
			RefID:          refID,
			RequestHash:    hash,
			CreatedAt:      time.Now().UTC(),
		}
		if err := tx.Create(&transaction).Error; err != nil {
			if isUniqueViolation(err) {
				return errIdempotentRace
			}
			return fmt.Errorf("storage: insert transaction: %w", err)
		}

		for i := range entries {
			entry := Entry{
				TransactionID: transaction.ID,
				AccountID:     entries[i].AccountID,
				Direction:     string(entries[i].Direction),
				Amount:        entries[i].Amount,
				CreatedAt:     transaction.CreatedAt,
			}
			if err := tx.Create(&entry).Error; err != nil {
				return fmt.Errorf("storage: insert entry: %w", err)
			}
		}

		// The balance snapshot is written together with the posting: a key replay
		// must return balances_after as of the first posting (CBL-2.5).
		snapshotRows := make([]TransactionBalance, 0, len(snapshots))
		for _, snapshot := range snapshots {
			snapshotRows = append(snapshotRows, TransactionBalance{
				TransactionID: transaction.ID,
				AccountID:     snapshot.AccountID,
				Available:     snapshot.Available,
				Reserved:      snapshot.Reserved,
				CreatedAt:     transaction.CreatedAt,
			})
		}
		if err := tx.Create(&snapshotRows).Error; err != nil {
			return fmt.Errorf("storage: insert transaction balances: %w", err)
		}
		// The event is written in the same transaction: publication through the
		// outbox is atomic with the posting (CBL-000, invariant 6).
		eventEntries := make([]TxnEventEntry, 0, len(entries))
		for i := range entries {
			account := accounts[entries[i].AccountID]
			eventEntries = append(eventEntries, TxnEventEntry{
				AccountID: entries[i].AccountID,
				Direction: entries[i].Direction,
				Amount:    entries[i].Amount,
				Asset:     account.Asset,
				Decimals:  account.Decimals,
			})
		}
		if err := r.emitEvent(ctx, tx, TxnEvent{
			Type:          EventTransactionPosted,
			TransactionID: transaction.ID,
			RefType:       refType,
			RefID:         refID,
			OccurredAt:    transaction.CreatedAt,
			Key:           accountIDs[0],
			Entries:       eventEntries,
		}); err != nil {
			return err
		}
		result = PostTransactionResult{TransactionID: transaction.ID, Balances: snapshots}
		return nil
	})
	if err != nil {
		return PostTransactionResult{}, err
	}
	return result, nil
}

// findTransaction looks up a transaction by idempotency key.
func findTransaction(tx *gorm.DB, idempotencyKey string) (Transaction, error) {
	var transaction Transaction
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&transaction).Error
	return transaction, err
}

// loadAccounts loads the posting accounts and checks the unity of asset and decimals.
func loadAccounts(tx *gorm.DB, entries []domain.EntryInput) (map[string]*Account, error) {
	seen := make(map[string]struct{}, len(entries))
	ids := make([]string, 0, len(entries))
	for i := range entries {
		id := entries[i].AccountID
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	var accounts []Account
	if err := tx.Where("id IN ?", ids).Find(&accounts).Error; err != nil {
		return nil, fmt.Errorf("storage: load accounts: %w", err)
	}
	if len(accounts) != len(ids) {
		return nil, fmt.Errorf("%w: expected %d accounts, found %d", domain.ErrUnknownAccount, len(ids), len(accounts))
	}
	byID := make(map[string]*Account, len(accounts))
	for i := range accounts {
		byID[accounts[i].ID] = &accounts[i]
		if i > 0 && (accounts[i].Asset != accounts[0].Asset || accounts[i].Decimals != accounts[0].Decimals) {
			return nil, fmt.Errorf("%w: %s/%d vs %s/%d", domain.ErrAssetMismatch,
				accounts[0].Asset, accounts[0].Decimals, accounts[i].Asset, accounts[i].Decimals)
		}
	}
	return byID, nil
}

// applyDelta updates the account balance with checks of lock_version, overflow,
// and insufficient funds for accounts where a negative balance is forbidden.
func applyDelta(tx *gorm.DB, account *Account, delta int64) (BalanceSnapshot, error) {
	var balance Balance
	err := tx.Where("account_id = ?", account.ID).First(&balance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return BalanceSnapshot{}, fmt.Errorf("%w: account %s has no balance row", domain.ErrUnknownAccount, account.ID)
	}
	if err != nil {
		return BalanceSnapshot{}, fmt.Errorf("storage: load balance %s: %w", account.ID, err)
	}
	if delta > 0 && balance.Available > math.MaxInt64-delta {
		return BalanceSnapshot{}, fmt.Errorf("%w: balance overflow for account %s", domain.ErrInvalidTransaction, account.ID)
	}
	if delta < 0 && balance.Available < math.MinInt64-delta {
		return BalanceSnapshot{}, fmt.Errorf("%w: balance underflow for account %s", domain.ErrInvalidTransaction, account.ID)
	}
	available := balance.Available + delta
	if _, restricted := restrictedAccountTypes[account.Type]; restricted && available < 0 {
		return BalanceSnapshot{}, fmt.Errorf("%w: account %s", domain.ErrInsufficientFunds, account.ID)
	}
	update := tx.Model(&Balance{}).
		Where("account_id = ? AND lock_version = ?", account.ID, balance.LockVersion).
		Updates(map[string]any{
			"available":    available,
			"lock_version": balance.LockVersion + 1,
			"updated_at":   time.Now().UTC(),
		})
	if update.Error != nil {
		return BalanceSnapshot{}, fmt.Errorf("storage: update balance %s: %w", account.ID, update.Error)
	}
	if update.RowsAffected == 0 {
		return BalanceSnapshot{}, errLockConflict
	}
	return BalanceSnapshot{
		AccountID: account.ID,
		Asset:     account.Asset,
		Decimals:  account.Decimals,
		Available: available,
		Reserved:  balance.Reserved,
	}, nil
}

// loadResult assembles the result of an existing transaction for a key replay.
// The balances_after snapshot has priority (CBL-2.5); for transactions predating
// migration 00008 (dev data), the previous reading of current balances is used.
func loadResult(tx *gorm.DB, transaction Transaction) (PostTransactionResult, error) {
	var snapshotRows []TransactionBalance
	if err := tx.Where("transaction_id = ?", transaction.ID).Find(&snapshotRows).Error; err != nil {
		return PostTransactionResult{}, fmt.Errorf("storage: load transaction balances: %w", err)
	}
	if len(snapshotRows) > 0 {
		ids := make([]string, 0, len(snapshotRows))
		for _, row := range snapshotRows {
			ids = append(ids, row.AccountID)
		}
		var accounts []Account
		if err := tx.Where("id IN ?", ids).Find(&accounts).Error; err != nil {
			return PostTransactionResult{}, fmt.Errorf("storage: load accounts: %w", err)
		}
		accountByID := make(map[string]Account, len(accounts))
		for _, account := range accounts {
			accountByID[account.ID] = account
		}
		snapshots := make([]BalanceSnapshot, 0, len(snapshotRows))
		for _, row := range snapshotRows {
			account := accountByID[row.AccountID]
			snapshots = append(snapshots, BalanceSnapshot{
				AccountID: row.AccountID,
				Asset:     account.Asset,
				Decimals:  account.Decimals,
				Available: row.Available,
				Reserved:  row.Reserved,
			})
		}
		sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].AccountID < snapshots[j].AccountID })
		return PostTransactionResult{TransactionID: transaction.ID, Balances: snapshots}, nil
	}

	var entries []Entry
	if err := tx.Where("transaction_id = ?", transaction.ID).Find(&entries).Error; err != nil {
		return PostTransactionResult{}, fmt.Errorf("storage: load entries: %w", err)
	}
	ids := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i := range entries {
		if _, ok := seen[entries[i].AccountID]; ok {
			continue
		}
		seen[entries[i].AccountID] = struct{}{}
		ids = append(ids, entries[i].AccountID)
	}
	var accounts []Account
	if err := tx.Where("id IN ?", ids).Find(&accounts).Error; err != nil {
		return PostTransactionResult{}, fmt.Errorf("storage: load accounts: %w", err)
	}
	accountByID := make(map[string]Account, len(accounts))
	for _, account := range accounts {
		accountByID[account.ID] = account
	}
	var balances []Balance
	if err := tx.Where("account_id IN ?", ids).Find(&balances).Error; err != nil {
		return PostTransactionResult{}, fmt.Errorf("storage: load balances: %w", err)
	}
	snapshots := make([]BalanceSnapshot, 0, len(balances))
	for _, balance := range balances {
		account := accountByID[balance.AccountID]
		snapshots = append(snapshots, BalanceSnapshot{
			AccountID: account.ID,
			Asset:     account.Asset,
			Decimals:  account.Decimals,
			Available: balance.Available,
			Reserved:  balance.Reserved,
		})
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].AccountID < snapshots[j].AccountID })
	return PostTransactionResult{TransactionID: transaction.ID, Balances: snapshots}, nil
}

// isUniqueViolation checks the PostgreSQL unique constraint code 23505.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
