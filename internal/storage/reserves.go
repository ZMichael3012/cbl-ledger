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

// errReserveStateRace сообщает о проигранной гонке финализации резерва:
// состояние изменила конкурирующая операция, попытку нужно повторить.
var errReserveStateRace = errors.New("storage: reserve state race")

// ReserveInput описывает запрос удержания средств на счете.
type ReserveInput struct {
	// IdempotencyKey задает ключ идемпотентности создания резерва.
	IdempotencyKey string
	// AccountID задает счет удержания.
	AccountID string
	// Asset задает актив суммы для сверки со счетом.
	Asset string
	// Decimals задает количество знаков после точки для сверки со счетом.
	Decimals int32
	// Amount задает сумму удержания в минимальных единицах актива.
	Amount int64
	// RefID задает бизнес-ссылку удержания.
	RefID string
}

// ReserveResult описывает результат операции резерва.
type ReserveResult struct {
	// ReserveID задает идентификатор резерва.
	ReserveID string
	// Status задает текущее состояние резерва.
	Status domain.ReserveStatus
	// Replayed сообщает, что результат возвращен по повторному ключу операции.
	Replayed bool
}

// Reserve создает удержание средств: переводит сумму из available в reserved счета мерчанта.
// Резервирование доступно только для счетов типа merchant; повтор ключа с тем же телом
// возвращает исходный резерв, с другим телом отклоняется ErrIdempotencyReuse.
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

// reserveOnce выполняет одну попытку резервирования в транзакции базы.
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
		// Без ссылки на мерчанта счет нельзя однозначно связать со счетом выплат:
		// fail-closed, иначе коммиты разных мерчантов смешались бы в общем счете.
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

// CommitReserve захватывает удержание: уменьшает reserved счета, зачисляет средства
// на счет выплат и создает сбалансированную проводку. Повтор в статусе committed
// идемпотентен; захват освобожденного или неизвестного резерва отклоняется.
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

// commitOnce выполняет одну попытку захвата резерва в транзакции базы.
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
		// Ключ коммита попадает и в transactions: занятый там ключ - это конфликт
		// тела ключа (idempotency_key_reuse), а не конкурентная гонка с повтором.
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
		// Резерв и баланс читаются в разных снапшотах READ COMMITTED: расхождение
		// означает конкурентную финализацию (или порчу данных) - попытку нужно повторить.
		if sourceBalance.Reserved < reserve.Amount {
			return errReserveStateRace
		}

		payoutRef := ""
		if source.MerchantRef != nil {
			payoutRef = *source.MerchantRef
		}
		destination, err := ensureAccountTx(tx, domain.PayoutAccountKey(payoutRef, source.Asset, source.Decimals))
		if errors.Is(err, domain.ErrAccountConflict) {
			// Существующий счет выплат с другой шкалой - расхождение активов
			// в терминах 2.4, а не конфликт провижининга.
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

		// Балансы обновляются в детерминированном порядке: иначе встречные коммиты
		// двух счетов могут дать deadlock 40P01 (как в postOnce).
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

// ReleaseReserve возвращает удержание: переводит reserved обратно в available.
// Повтор в статусе released идемпотентен; возврат захваченного или неизвестного
// резерва отклоняется.
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

// releaseOnce выполняет одну попытку возврата резерва в транзакции базы.
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
		// См. commitOnce: расхождение снапшотов резерва и баланса - конкурентная гонка.
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

// recordReserveOperation фиксирует ключ операции над резервом; нарушение
// уникальности ключа считается конкурентной гонкой и повторяется политикой.
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

// withReserveRetry повторяет операцию резерва при конкурентных конфликтах
// согласно политике повторов репозитория.
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
		// Полный jitter снижает синхронные повторные столкновения; криптостойкость не нужна.
		delay := policy.nextDelay(attempt, rand.Int64N) //nolint:gosec // G404: jitter, не криптография
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

// op задает вид операции над резервом.
type op string

// reserveOpCommit и reserveOpRelease задают виды операций над резервом.
const (
	reserveOpCommit  op = "commit"
	reserveOpRelease op = "release"
)

// reserveRefType задает тип бизнес-ссылки проводок захвата резерва.
// Контракт Reserve не передает ref_type, поэтому проводки маркируются резервом.
const reserveRefType = "reserve"

// findReserveByKey ищет резерв по ключу идемпотентности.
func findReserveByKey(tx *gorm.DB, idempotencyKey string) (Reserve, error) {
	var reserve Reserve
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&reserve).Error
	return reserve, err
}

// findReserveByID ищет резерв по идентификатору.
func findReserveByID(tx *gorm.DB, reserveID string) (Reserve, error) {
	var reserve Reserve
	err := tx.Where("id = ?", reserveID).First(&reserve).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Reserve{}, fmt.Errorf("%w: %s", domain.ErrUnknownReserve, reserveID)
	}
	return reserve, err
}

// findReserveOperation ищет операцию над резервом по ключу идемпотентности.
func findReserveOperation(tx *gorm.DB, idempotencyKey string) (ReserveOperation, error) {
	var operation ReserveOperation
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&operation).Error
	return operation, err
}

// loadAccountByID загружает счет по идентификатору.
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

// loadBalance загружает баланс счета.
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

// reserveRequestHash вычисляет канонический хэш тела запроса резерва.
func reserveRequestHash(input ReserveInput) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s|%d|%d|%d:%s",
		len(input.AccountID), input.AccountID,
		len(input.Asset), input.Asset,
		input.Decimals, input.Amount,
		len(input.RefID), input.RefID)
	return hex.EncodeToString(digest.Sum(nil))
}

// reserveOpHash вычисляет канонический хэш тела операции над резервом.
// Reason не входит в хэш: повтор с другим текстом причины остается повтором.
func reserveOpHash(operation, reserveID string) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s", len(operation), operation, len(reserveID), reserveID)
	return hex.EncodeToString(digest.Sum(nil))
}
