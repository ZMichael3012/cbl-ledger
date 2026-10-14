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

// maxOptimisticRetries задает предел повторов при конфликте оптимистичной блокировки.
const maxOptimisticRetries = 8

// restrictedAccountTypes задает типы счетов, где отрицательный баланс запрещен.
// Транзитные счета (clearing, fee, suspense) могут уходить в минус до клиринга.
var restrictedAccountTypes = map[string]struct{}{
	string(domain.AccountTypeMerchant): {},
	string(domain.AccountTypePayout):   {},
}

var (
	// errLockConflict сообщает о проигранной гонке за lock_version баланса.
	errLockConflict = errors.New("storage: balance lock conflict")
	// errIdempotentRace сообщает о конкурентной вставке того же idempotency_key.
	errIdempotentRace = errors.New("storage: idempotency race")
)

// BalanceSnapshot описывает остаток счета после проводки.
type BalanceSnapshot struct {
	// AccountID задает счет.
	AccountID string
	// Asset задает код актива счета.
	Asset string
	// Decimals задает количество знаков после точки.
	Decimals int32
	// Available задает доступный остаток.
	Available int64
	// Reserved задает зарезервированный остаток.
	Reserved int64
}

// PostTransactionResult описывает результат проводки.
type PostTransactionResult struct {
	// TransactionID задает идентификатор транзакции.
	TransactionID string
	// Balances задает остатки счетов после проводки.
	Balances []BalanceSnapshot
	// Replayed сообщает, что результат возвращен по повторному ключу идемпотентности.
	Replayed bool
}

// Repository выполняет транзакции двойной записи в PostgreSQL.
type Repository struct {
	db  *gorm.DB
	log *zap.Logger
}

// NewRepository создает Repository.
func NewRepository(db *gorm.DB, log *zap.Logger) *Repository {
	if log == nil {
		log = zap.NewNop()
	}
	return &Repository{db: db, log: log}
}

// PostTransaction выполняет сбалансированную проводку с идемпотентностью по ключу
// и оптимистичной блокировкой балансов; при конфликтах повторяет с backoff и jitter.
// Повтор ключа с другим телом отклоняется ошибкой ErrIdempotencyReuse.
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

	backoff := 5 * time.Millisecond
	for attempt := 0; attempt < maxOptimisticRetries; attempt++ {
		result, err := r.postOnce(ctx, idempotencyKey, refType, refID, normalized, hash)
		switch {
		case err == nil:
			return result, nil
		case isRetryable(err):
			// Jitter снижает столкновения повторов; криптостойкость здесь не требуется.
			delay := backoff + time.Duration(rand.Int64N(int64(backoff)+1)) //nolint:gosec // G404: jitter, не криптография
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return PostTransactionResult{}, ctx.Err()
			case <-timer.C:
			}
			backoff *= 2
		default:
			return PostTransactionResult{}, err
		}
	}
	return PostTransactionResult{}, fmt.Errorf("%w: exceeded %d retries", domain.ErrConflict, maxOptimisticRetries)
}

// normalizeEntries приводит идентификаторы счетов к каноническому виду UUID.
// Без нормализации UUID в другом регистре не совпал бы с ключом загруженных счетов.
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

// requestHash вычисляет канонический хэш тела запроса для контроля повтора ключа.
// Строки кодируются с префиксом длины, чтобы разделители внутри значений
// не создавали разные тела с одинаковым хэшем.
func requestHash(refType, refID string, entries []domain.EntryInput) string {
	digest := sha256.New()
	_, _ = fmt.Fprintf(digest, "%d:%s|%d:%s|%d:", len(refType), refType, len(refID), refID, len(entries))
	for i := range entries {
		entry := entries[i]
		_, _ = fmt.Fprintf(digest, "%d:%s|%s|%d;", len(entry.AccountID), entry.AccountID, entry.Direction, entry.Amount)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

// isRetryable сообщает, что ошибку следует повторить: конфликт версии, гонка ключа или deadlock.
func isRetryable(err error) bool {
	if errors.Is(err, errLockConflict) || errors.Is(err, errIdempotentRace) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// postOnce выполняет одну попытку проводки в транзакции базы.
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

		// Счета обновляются в детерминированном порядке: иначе встречные транзакции
		// двух счетов могут дать deadlock 40P01.
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
		result = PostTransactionResult{TransactionID: transaction.ID, Balances: snapshots}
		return nil
	})
	if err != nil {
		return PostTransactionResult{}, err
	}
	return result, nil
}

// findTransaction ищет транзакцию по ключу идемпотентности.
func findTransaction(tx *gorm.DB, idempotencyKey string) (Transaction, error) {
	var transaction Transaction
	err := tx.Where("idempotency_key = ?", idempotencyKey).First(&transaction).Error
	return transaction, err
}

// loadAccounts загружает счета проводок и проверяет единство актива и decimals.
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

// applyDelta обновляет баланс счета с проверкой lock_version, переполнения
// и нехватки средств для счетов с запретом отрицательного баланса.
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

// loadResult собирает результат существующей транзакции для повтора по ключу.
func loadResult(tx *gorm.DB, transaction Transaction) (PostTransactionResult, error) {
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

// isUniqueViolation проверяет код 23505 уникального ограничения PostgreSQL.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
