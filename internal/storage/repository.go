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

// RetryPolicy задает политику повторов при конкурентных конфликтах.
type RetryPolicy struct {
	// MaxAttempts задает предельное число попыток проводки.
	MaxAttempts int
	// BaseDelay задает базовую паузу между попытками.
	BaseDelay time.Duration
	// MaxDelay задает потолок паузы между попытками.
	MaxDelay time.Duration
}

// DefaultRetryPolicy возвращает политику повторов по умолчанию:
// 8 попыток, база 5 мс, потолок 250 мс, суммарно не более ~1 с ожидания.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 8,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    250 * time.Millisecond,
	}
}

// maxRetryAttemptsCeiling ограничивает кастомную политику сверху, чтобы долгоживущие
// вызовы (например, consumer 2.7) не могли зациклиться на конфликтах.
const maxRetryAttemptsCeiling = 32

// normalizeRetryPolicy заменяет невалидные поля политики значениями по умолчанию,
// чтобы нулевое значение Repository работало без явной конфигурации.
// Потолок ниже базовой паузы поднимается до нее, число попыток ограничено сверху.
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

// nextDelay вычисляет паузу перед попыткой attempt по схеме полного jitter:
// равномерное значение из [0, min(MaxDelay, BaseDelay << attempt)].
// randN инжектируется для детерминированных тестов.
func (policy RetryPolicy) nextDelay(attempt int, randN func(int64) int64) time.Duration {
	limit := policy.BaseDelay
	for i := 0; i < attempt && limit < policy.MaxDelay; i++ {
		// Удвоение только при заведомо безопасном значении: иначе переполнение int64.
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

// RetryEvent описывает один повтор проводки для наблюдателей.
type RetryEvent struct {
	// Attempt задает номер повторяемой попытки, начиная с 1.
	Attempt int
	// Cause задает причину повтора: lock_conflict, idempotency_race, deadlock или unknown.
	Cause string
	// Delay задает паузу перед следующей попыткой.
	Delay time.Duration
}

// RetryObserver получает события повторов; вызывается перед паузой.
// Может вызываться конкурентно из нескольких горутин и не должен паниковать:
// паника наблюдателя уронила бы обработку запроса.
type RetryObserver func(RetryEvent)

// Option настраивает Repository.
type Option func(*Repository)

// WithRetryPolicy задает политику повторов вместо политики по умолчанию.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(r *Repository) { r.retryPolicy = policy }
}

// WithRetryObserver подключает наблюдателя повторов (лог, метрики).
func WithRetryObserver(observer RetryObserver) Option {
	return func(r *Repository) { r.retryObserver = observer }
}

// retryCause классифицирует причину повтора для наблюдателей.
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
	db            *gorm.DB
	log           *zap.Logger
	retryPolicy   RetryPolicy
	retryObserver RetryObserver
	eventSink     TxnEventSink
}

// NewRepository создает Repository; опции настраивают политику и наблюдателя повторов.
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

// PostTransaction выполняет сбалансированную проводку с идемпотентностью по ключу
// и оптимистичной блокировкой балансов; при конфликтах повторяет согласно политике
// (полный jitter, потолок паузы). Повтор ключа с другим телом отклоняется ErrIdempotencyReuse.
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
		// Полный jitter снижает синхронные повторные столкновения; криптостойкость не нужна.
		delay := policy.nextDelay(attempt, rand.Int64N) //nolint:gosec // G404: jitter, не криптография
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

		// Балансы обновляются до вставки проводок и в детерминированном порядке:
		// CAS удерживает блокировку строки до коммита, поэтому идентификаторы
		// проводок выписываются в порядке коммитов по счету (курсорная пагинация
		// 2.5 не пропускает записи), а встречные транзакции двух счетов не дают
		// deadlock 40P01.
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

		// Снимок балансов пишется вместе с проводкой: повтор по ключу обязан
		// вернуть balances_after на момент первой проводки (CBL-2.5).
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
		// Событие пишется в ту же транзакцию: публикация через outbox атомарна
		// с проводкой (CBL-000, инвариант 6).
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
// Приоритет - снимок balances_after (CBL-2.5); для транзакций до миграции 00008
// (dev-данные) используется прежнее чтение текущих балансов.
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

// isUniqueViolation проверяет код 23505 уникального ограничения PostgreSQL.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
