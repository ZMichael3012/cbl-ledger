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
	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// errReserveStateRace reports a lost reserve finalization race:
// a concurrent operation changed the state, and the attempt must be retried.
var errReserveStateRace = errors.New("storage: reserve state race")

// ReserveInput describes a request to hold funds on an account.
type ReserveInput struct {
	// IdempotencyKey sets the idempotency key of the reserve creation.
	IdempotencyKey string
	// AccountID sets the hold account.
	AccountID string
	// Asset sets the asset of the amount for verification against the account.
	Asset string
	// Decimals sets the number of decimal places for verification against the account.
	Decimals int32
	// Amount sets the hold amount in asset minor units.
	Amount int64
	// RefID sets the hold business reference.
	RefID string
}

// ReserveResult describes the result of a reserve operation.
type ReserveResult struct {
	// ReserveID sets the reserve identifier.
	ReserveID string
	// Status sets the current reserve state.
	Status domain.ReserveStatus
	// Replayed reports that the result was returned for a repeated operation key.
	Replayed bool
}

// Reserve creates a hold of funds: it moves the amount from available to reserved
// on the merchant account. Reservation is available only for accounts of type
// merchant; a key replay with the same body returns the original reserve, while a
// different body is rejected with ErrIdempotencyReuse.
func (r *Repository) Reserve(ctx context.Context, input ReserveInput) (ReserveResult, error) {
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return ReserveResult{}, fmt.Errorf("%w: empty idempotency_key", domain.ErrInvalidReserve)
	}
	if strings.TrimSpace(input.RefID) == "" {
		return ReserveResult{}, fmt.Errorf("%w: empty ref_id", domain.ErrInvalidReserve)
	}
	if err := domain.ValidateReserveAmount(input.Amount); err != nil {
		return ReserveResult{}, err
	}
	accountID, err := uuid.Parse(strings.TrimSpace(input.AccountID))
	if err != nil {
		return ReserveResult{}, fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, input.AccountID)
	}
	input.AccountID = accountID.String()
	hash := reserveRequestHash(input)

	return r.withReserveRetry(ctx, func() (ReserveResult, error) {
		return r.reserveOnce(ctx, input, hash)
	})
}

// reserveOnce performs a single reservation attempt in a database transaction.
func (r *Repository) reserveOnce(ctx context.Context, input ReserveInput, hash string) (ReserveResult, error) {
	reserveID, err := uuid.NewV7()
	if err != nil {
		return ReserveResult{}, fmt.Errorf("storage: generate reserve id: %w", err)
	}
	var result ReserveResult
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		existing, err := findReserveByKey(tx, input.IdempotencyKey)
		if err == nil {
			if existing.RequestHash != hash {
				return fmt.Errorf("%w: key %q", domain.ErrIdempotencyReuse, input.IdempotencyKey)
			}
			result = ReserveResult{ReserveID: existing.ID, Status: domain.ReserveStatus(existing.Status), Replayed: true}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		account, err := loadAccountByID(tx, input.AccountID)
		if err != nil {
			return err
		}
		if account.Type != string(domain.AccountTypeMerchant) {
			return fmt.Errorf("%w: type %s", domain.ErrReserveNotAllowed, account.Type)
		}
		// Without a merchant reference the account cannot be unambiguously linked
		// to a payout account: fail-closed, otherwise captures of different
		// merchants would mix in a shared account.
		if account.MerchantRef == nil || strings.TrimSpace(*account.MerchantRef) == "" {
			return fmt.Errorf("%w: merchant account %s without merchant_ref", domain.ErrReserveNotAllowed, account.ID)
		}
		if account.Asset != input.Asset || account.Decimals != input.Decimals {
			return fmt.Errorf("%w: %s/%d vs %s/%d", domain.ErrAssetMismatch,
				account.Asset, account.Decimals, input.Asset, input.Decimals)
		}

		balance, err := loadBalance(tx, account.ID)
		if err != nil {
			return err
		}
		if balance.Reserved > math.MaxInt64-input.Amount {
			return fmt.Errorf("%w: reserved overflow for account %s", domain.ErrInvalidReserve, account.ID)
		}
		if balance.Available < input.Amount {
			return fmt.Errorf("%w: account %s", domain.ErrInsufficientFunds, account.ID)
		}
		now := time.Now().UTC()
		update := tx.Model(&Balance{}).
			Where("account_id = ? AND lock_version = ?", account.ID, balance.LockVersion).
			Updates(map[string]any{
				"available":    balance.Available - input.Amount,
				"reserved":     balance.Reserved + input.Amount,
				"lock_version": balance.LockVersion + 1,
				"updated_at":   now,
			})
		if update.Error != nil {
			return fmt.Errorf("storage: update balance %s: %w", account.ID, update.Error)
		}
		if update.RowsAffected == 0 {
			return errLockConflict
		}

		reserve := Reserve{
			ID:             reserveID.String(),
			AccountID:      account.ID,
			Amount:         input.Amount,
			RefID:          input.RefID,
			Status:         string(domain.ReserveStatusActive),
			IdempotencyKey: input.IdempotencyKey,
			RequestHash:    hash,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := tx.Create(&reserve).Error; err != nil {
			if isUniqueViolation(err) {
				return errIdempotentRace
			}
			return fmt.Errorf("storage: insert reserve: %w", err)
		}
		if err := r.emitEvent(ctx, tx, TxnEvent{
			Type:       EventReserveCreated,
			RefType:    reserveRefType,
			RefID:      reserve.RefID,
			OccurredAt: reserve.CreatedAt,
			Key:        reserve.AccountID,
		}); err != nil {
			return err
		}
		result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatusActive}
		return nil
	})
	if err != nil {
		return ReserveResult{}, err
	}
	return result, nil
}

// CommitReserve captures a hold: it decreases the account reserved, credits the
// funds to the payout account, and creates a balanced posting. A replay in the
// committed status is idempotent; capturing a released or unknown reserve is rejected.
func (r *Repository) CommitReserve(ctx context.Context, idempotencyKey, reserveID string) (ReserveResult, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return ReserveResult{}, fmt.Errorf("%w: empty idempotency_key", domain.ErrInvalidReserve)
	}
	parsedReserveID, err := uuid.Parse(strings.TrimSpace(reserveID))
	if err != nil {
		return ReserveResult{}, fmt.Errorf("%w: reserve_id %q", domain.ErrUnknownReserve, reserveID)
	}
	hash := reserveOpHash(string(reserveOpCommit), parsedReserveID.String())

	return r.withReserveRetry(ctx, func() (ReserveResult, error) {
		return r.commitOnce(ctx, idempotencyKey, parsedReserveID.String(), hash)
	})
}

// commitOnce performs a single reserve capture attempt in a database transaction.
func (r *Repository) commitOnce(ctx context.Context, idempotencyKey, reserveID, hash string) (ReserveResult, error) {
	var result ReserveResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		operation, err := findReserveOperation(tx, idempotencyKey)
		if err == nil {
			if operation.RequestHash != hash {
				return fmt.Errorf("%w: key %q", domain.ErrIdempotencyReuse, idempotencyKey)
			}
			reserve, reserveErr := findReserveByID(tx, operation.ReserveID)
			if reserveErr != nil {
				return reserveErr
			}
			result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatus(reserve.Status), Replayed: true}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		reserve, err := findReserveByID(tx, reserveID)
		if err != nil {
			return err
		}
		switch domain.ReserveStatus(reserve.Status) {
		case domain.ReserveStatusCommitted:
			if err := recordReserveOperation(tx, reserve.ID, reserveOpCommit, idempotencyKey, hash); err != nil {
				return err
			}
			result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatusCommitted}
			return nil
		case domain.ReserveStatusReleased:
			return fmt.Errorf("%w: reserve %s is released", domain.ErrReserveState, reserve.ID)
		case domain.ReserveStatusActive:
		}
		// The commit key also lands in transactions: a key taken there is a key
		// body conflict (idempotency_key_reuse), not a concurrent race with a retry.
		if _, err := findTransaction(tx, idempotencyKey); err == nil {
			return fmt.Errorf("%w: key %q already used by posted transaction", domain.ErrIdempotencyReuse, idempotencyKey)
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		source, err := loadAccountByID(tx, reserve.AccountID)
		if err != nil {
			return err
		}
		sourceBalance, err := loadBalance(tx, source.ID)
		if err != nil {
			return err
		}
		// The reserve and the balance are read in different READ COMMITTED snapshots:
		// a mismatch means concurrent finalization (or data corruption), and the
		// attempt must be retried.
		if sourceBalance.Reserved < reserve.Amount {
			return errReserveStateRace
		}

		payoutRef := ""
		if source.MerchantRef != nil {
			payoutRef = *source.MerchantRef
		}
		destination, err := ensureAccountTx(tx, domain.PayoutAccountKey(payoutRef, source.Asset, source.Decimals))
		if errors.Is(err, domain.ErrAccountConflict) {
			// An existing payout account with a different scale is an asset mismatch
			// in the terms of 2.4, not a provisioning conflict.
			return fmt.Errorf("%w: payout account decimals mismatch: %w", domain.ErrAssetMismatch, err)
		}
		if err != nil {
			return err
		}
		destinationBalance, err := loadBalance(tx, destination.ID)
		if err != nil {
			return err
		}
		if destinationBalance.Available > math.MaxInt64-reserve.Amount {
			return fmt.Errorf("storage: payout balance overflow for account %s", destination.ID)
		}

		// Balances are updated in a deterministic order: otherwise opposite commits
		// of two accounts could produce a 40P01 deadlock (as in postOnce).
		now := time.Now().UTC()
		updates := []struct {
			accountID   string
			lockVersion int64
			values      map[string]any
		}{
			{
				accountID:   source.ID,
				lockVersion: sourceBalance.LockVersion,
				values: map[string]any{
					"reserved":     sourceBalance.Reserved - reserve.Amount,
					"lock_version": sourceBalance.LockVersion + 1,
					"updated_at":   now,
				},
			},
			{
				accountID:   destination.ID,
				lockVersion: destinationBalance.LockVersion,
				values: map[string]any{
					"available":    destinationBalance.Available + reserve.Amount,
					"lock_version": destinationBalance.LockVersion + 1,
					"updated_at":   now,
				},
			},
		}
		sort.Slice(updates, func(i, j int) bool { return updates[i].accountID < updates[j].accountID })
		for _, update := range updates {
			affected := tx.Model(&Balance{}).
				Where("account_id = ? AND lock_version = ?", update.accountID, update.lockVersion).
				Updates(update.values)
			if affected.Error != nil {
				return fmt.Errorf("storage: update balance %s: %w", update.accountID, affected.Error)
			}
			if affected.RowsAffected == 0 {
				return errLockConflict
			}
		}

		transactionID, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("storage: generate transaction id: %w", err)
		}
		transaction := Transaction{
			ID:             transactionID.String(),
			IdempotencyKey: idempotencyKey,
			RefType:        reserveRefType,
			RefID:          reserve.RefID,
			RequestHash:    hash,
			CreatedAt:      now,
		}
		if err := tx.Create(&transaction).Error; err != nil {
			if isUniqueViolation(err) {
				return errIdempotentRace
			}
			return fmt.Errorf("storage: insert transaction: %w", err)
		}
		entries := []Entry{
			{TransactionID: transaction.ID, AccountID: source.ID, Direction: string(domain.DirectionDebit), Amount: reserve.Amount, CreatedAt: now},
			{TransactionID: transaction.ID, AccountID: destination.ID, Direction: string(domain.DirectionCredit), Amount: reserve.Amount, CreatedAt: now},
		}
		if err := tx.Create(&entries).Error; err != nil {
			return fmt.Errorf("storage: insert entries: %w", err)
		}

		finalized := tx.Model(&Reserve{}).
			Where("id = ? AND status = ?", reserve.ID, string(domain.ReserveStatusActive)).
			Updates(map[string]any{
				"status":                  string(domain.ReserveStatusCommitted),
				"transaction_id":          transaction.ID,
				"counterparty_account_id": destination.ID,
				"updated_at":              now,
			})
		if finalized.Error != nil {
			return fmt.Errorf("storage: finalize reserve %s: %w", reserve.ID, finalized.Error)
		}
		if finalized.RowsAffected == 0 {
			return errReserveStateRace
		}

		if err := recordReserveOperation(tx, reserve.ID, reserveOpCommit, idempotencyKey, hash); err != nil {
			return err
		}
		if err := r.emitEvent(ctx, tx, TxnEvent{
			Type:          EventReserveCommitted,
			TransactionID: transaction.ID,
			RefType:       reserveRefType,
			RefID:         reserve.RefID,
			OccurredAt:    now,
			Key:           source.ID,
			Entries: []TxnEventEntry{
				{AccountID: source.ID, Direction: domain.DirectionDebit, Amount: reserve.Amount, Asset: source.Asset, Decimals: source.Decimals},
				{AccountID: destination.ID, Direction: domain.DirectionCredit, Amount: reserve.Amount, Asset: destination.Asset, Decimals: destination.Decimals},
			},
		}); err != nil {
			return err
		}
		result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatusCommitted}
		return nil
	})
	if err != nil {
		return ReserveResult{}, err
	}
	return result, nil
}

// ReleaseReserve releases a hold: it moves reserved back to available. A replay
// in the released status is idempotent; releasing a captured or unknown reserve
// is rejected.
func (r *Repository) ReleaseReserve(ctx context.Context, idempotencyKey, reserveID, reason string) (ReserveResult, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return ReserveResult{}, fmt.Errorf("%w: empty idempotency_key", domain.ErrInvalidReserve)
	}
	parsedReserveID, err := uuid.Parse(strings.TrimSpace(reserveID))
	if err != nil {
		return ReserveResult{}, fmt.Errorf("%w: reserve_id %q", domain.ErrUnknownReserve, reserveID)
	}
	hash := reserveOpHash(string(reserveOpRelease), parsedReserveID.String())

	return r.withReserveRetry(ctx, func() (ReserveResult, error) {
		return r.releaseOnce(ctx, idempotencyKey, parsedReserveID.String(), reason, hash)
	})
}

// releaseOnce performs a single reserve release attempt in a database transaction.
func (r *Repository) releaseOnce(ctx context.Context, idempotencyKey, reserveID, reason, hash string) (ReserveResult, error) {
	var result ReserveResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		operation, err := findReserveOperation(tx, idempotencyKey)
		if err == nil {
			if operation.RequestHash != hash {
				return fmt.Errorf("%w: key %q", domain.ErrIdempotencyReuse, idempotencyKey)
			}
			reserve, reserveErr := findReserveByID(tx, operation.ReserveID)
			if reserveErr != nil {
				return reserveErr
			}
			result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatus(reserve.Status), Replayed: true}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		reserve, err := findReserveByID(tx, reserveID)
		if err != nil {
			return err
		}
		switch domain.ReserveStatus(reserve.Status) {
		case domain.ReserveStatusReleased:
			if err := recordReserveOperation(tx, reserve.ID, reserveOpRelease, idempotencyKey, hash); err != nil {
				return err
			}
			result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatusReleased}
			return nil
		case domain.ReserveStatusCommitted:
			return fmt.Errorf("%w: reserve %s is committed", domain.ErrReserveState, reserve.ID)
		case domain.ReserveStatusActive:
		}

		balance, err := loadBalance(tx, reserve.AccountID)
		if err != nil {
			return err
		}
		// See commitOnce: a mismatch between the reserve and balance snapshots is a concurrent race.
		if balance.Reserved < reserve.Amount {
			return errReserveStateRace
		}
		if balance.Available > math.MaxInt64-reserve.Amount {
			return fmt.Errorf("storage: balance overflow for account %s", reserve.AccountID)
		}
		now := time.Now().UTC()
		update := tx.Model(&Balance{}).
			Where("account_id = ? AND lock_version = ?", reserve.AccountID, balance.LockVersion).
			Updates(map[string]any{
				"available":    balance.Available + reserve.Amount,
				"reserved":     balance.Reserved - reserve.Amount,
				"lock_version": balance.LockVersion + 1,
				"updated_at":   now,
			})
		if update.Error != nil {
			return fmt.Errorf("storage: update balance %s: %w", reserve.AccountID, update.Error)
		}
		if update.RowsAffected == 0 {
			return errLockConflict
		}

		finalized := tx.Model(&Reserve{}).
			Where("id = ? AND status = ?", reserve.ID, string(domain.ReserveStatusActive)).
			Updates(map[string]any{
				"status":     string(domain.ReserveStatusReleased),
				"reason":     reason,
				"updated_at": now,
			})
		if finalized.Error != nil {
			return fmt.Errorf("storage: finalize reserve %s: %w", reserve.ID, finalized.Error)
		}
		if finalized.RowsAffected == 0 {
			return errReserveStateRace
		}

		if err := recordReserveOperation(tx, reserve.ID, reserveOpRelease, idempotencyKey, hash); err != nil {
			return err
		}
		if err := r.emitEvent(ctx, tx, TxnEvent{
			Type:       EventReserveReleased,
			RefType:    reserveRefType,
			RefID:      reserve.RefID,
			OccurredAt: now,
			Key:        reserve.AccountID,
		}); err != nil {
			return err
		}
		result = ReserveResult{ReserveID: reserve.ID, Status: domain.ReserveStatusReleased}
		return nil
	})
	if err != nil {
		return ReserveResult{}, err
	}
	return result, nil
}

// recordReserveOperation records the reserve operation key; a unique key
// violation is treated as a concurrent race and retried by the policy.
func recordReserveOperation(tx *gorm.DB, reserveID string, operation op, idempotencyKey, hash string) error {
	row := ReserveOperation{
		ReserveID:      reserveID,
		Op:             string(operation),
		IdempotencyKey: idempotencyKey,
		RequestHash:    hash,
		CreatedAt:      time.Now().UTC(),
	}
	if err := tx.Create(&row).Error; err != nil {
		if isUniqueViolation(err) {
			return errIdempotentRace
		}
		return fmt.Errorf("storage: insert reserve operation: %w", err)
	}
	return nil
}

// withReserveRetry retries a reserve operation on concurrent conflicts
// according to the repository retry policy.
func (r *Repository) withReserveRetry(ctx context.Context, fn func() (ReserveResult, error)) (ReserveResult, error) {
	policy := normalizeRetryPolicy(r.retryPolicy)
	var lastErr error
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		if !isRetryable(err) && !errors.Is(err, errReserveStateRace) {
			return ReserveResult{}, err
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
			return ReserveResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ReserveResult{}, fmt.Errorf("%w: exceeded %d attempts: %w", domain.ErrConflict, policy.MaxAttempts, lastErr)
}

// op sets the kind of reserve operation.
type op string

// reserveOpCommit and reserveOpRelease set the kinds of reserve operations.
const (
	reserveOpCommit  op = "commit"
	reserveOpRelease op = "release"
)

// reserveRefType sets the business reference type of the reserve capture postings.
// The Reserve contract does not pass ref_type, so the postings are marked with the reserve.
const reserveRefType = "reserve"

// findReserveByKey looks up a reserve by idempotency key.
func findReserveByKey(tx *gorm.DB, idempotencyKey string) (Reserve, error) {
	var reserve Reserve
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&reserve).Error
	return reserve, err
}

// findReserveByID looks up a reserve by identifier.
func findReserveByID(tx *gorm.DB, reserveID string) (Reserve, error) {
	var reserve Reserve
	err := tx.Where("id = ?", reserveID).First(&reserve).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Reserve{}, fmt.Errorf("%w: %s", domain.ErrUnknownReserve, reserveID)
	}
	return reserve, err
}

// findReserveOperation looks up a reserve operation by idempotency key.
func findReserveOperation(tx *gorm.DB, idempotencyKey string) (ReserveOperation, error) {
	var operation ReserveOperation
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&operation).Error
	return operation, err
}

// loadAccountByID loads an account by identifier.
func loadAccountByID(tx *gorm.DB, accountID string) (Account, error) {
	var account Account
	err := tx.Where("id = ?", accountID).First(&account).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Account{}, fmt.Errorf("%w: %s", domain.ErrUnknownAccount, accountID)
	}
	if err != nil {
		return Account{}, fmt.Errorf("storage: load account %s: %w", accountID, err)
	}
	return account, nil
}

// loadBalance loads the account balance.
func loadBalance(tx *gorm.DB, accountID string) (Balance, error) {
	var balance Balance
	err := tx.Where("account_id = ?", accountID).First(&balance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Balance{}, fmt.Errorf("%w: account %s has no balance row", domain.ErrUnknownAccount, accountID)
	}
	if err != nil {
		return Balance{}, fmt.Errorf("storage: load balance %s: %w", accountID, err)
	}
	return balance, nil
}

// reserveRequestHash computes the canonical hash of the reserve request body.
func reserveRequestHash(input ReserveInput) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s|%d|%d|%d:%s",
		len(input.AccountID), input.AccountID,
		len(input.Asset), input.Asset,
		input.Decimals, input.Amount,
		len(input.RefID), input.RefID)
	return hex.EncodeToString(digest.Sum(nil))
}

// reserveOpHash computes the canonical hash of the reserve operation body.
// Reason is not part of the hash: a replay with a different reason text remains a replay.
func reserveOpHash(operation, reserveID string) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s", len(operation), operation, len(reserveID), reserveID)
	return hex.EncodeToString(digest.Sum(nil))
}
