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

// Пределы выписки: limit 0 заменяется значением по умолчанию,
// большее значение клампится потолком. Список счетов мерчанта ограничен
// серверным пределом: число счетов плана - активы на мерчанта, превышение
// предела - аномалия плана счетов (ErrTooManyResults).
const (
	defaultEntriesLimit = 50
	maxEntriesLimit     = 200
	maxMerchantAccounts = 1000
)

// AccountBalanceView описывает баланс счета для чтения.
type AccountBalanceView struct {
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

// EntryView описывает проводку выписки с бизнес-ссылкой транзакции.
type EntryView struct {
	// EntryID задает монотонный идентификатор проводки.
	EntryID int64 `gorm:"column:entry_id"`
	// TransactionID задает транзакцию проводки.
	TransactionID string `gorm:"column:transaction_id"`
	// AccountID задает счет проводки.
	AccountID string `gorm:"column:account_id"`
	// Direction задает направление проводки.
	Direction domain.Direction `gorm:"column:direction"`
	// Amount задает сумму в минимальных единицах актива.
	Amount int64 `gorm:"column:amount"`
	// Asset задает код актива счета.
	Asset string `gorm:"column:asset"`
	// Decimals задает количество знаков после точки.
	Decimals int32 `gorm:"column:decimals"`
	// RefType задает тип бизнес-ссылки транзакции.
	RefType string `gorm:"column:ref_type"`
	// RefID задает идентификатор бизнес-ссылки транзакции.
	RefID string `gorm:"column:ref_id"`
	// CreatedAt задает время проводки.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// MerchantAccountView описывает счет мерчанта с балансом для BFF-выписок (2.10).
type MerchantAccountView struct {
	// AccountID задает счет.
	AccountID string
	// Type задает тип счета плана счетов: merchant или payout.
	Type string
	// Asset задает код актива счета.
	Asset string
	// Decimals задает количество знаков после точки.
	Decimals int32
	// Available задает доступный остаток.
	Available int64
	// Reserved задает зарезервированный остаток.
	Reserved int64
}

// EntryFilter описывает фильтр выписки (2.10): заполняется account_id или
// merchant_ref; период применяется к created_at транзакции.
type EntryFilter struct {
	// AccountID задает счет выписки; пусто при выписке по merchant_ref.
	AccountID string
	// MerchantRef задает фильтр счетов мерчанта (merchant и payout).
	MerchantRef string
	// PeriodFrom задает нижнюю границу периода включительно (nil - без границы).
	PeriodFrom *time.Time
	// PeriodTo задает верхнюю границу периода исключительно (nil - без границы).
	PeriodTo *time.Time
	// Limit задает размер страницы; 0 - значение по умолчанию.
	Limit uint32
	// Cursor задает курсор продолжения (entry_id последней записи).
	Cursor string
}

// merchantAccountTypes задает типы счетов, видимые мерчанту в выписке и балансах.
var merchantAccountTypes = [2]string{string(domain.AccountTypeMerchant), string(domain.AccountTypePayout)}

// GetBalance возвращает баланс счета; неизвестный счет или отсутствие
// балансового ряда отклоняются ErrUnknownAccount.
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

// ListMerchantAccounts возвращает счета мерчанта с балансами по возрастанию
// (asset, type); счета без балансового ряда отдаются нулевыми (аномалия
// провижининга не маскируется только точечным GetBalance, который отвечает
// ненайденным). Пустая и невалидная ссылка отклоняются ErrInvalidQuery:
// список без мерчанта запрещен (fail-closed); превышение серверного предела
// счетов - ErrTooManyResults.
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

// ListEntries возвращает страницу выписки по возрастанию entry_id. Заполняется
// account_id или merchant_ref (хотя бы одно): merchant_ref ограничивает выписку
// счетами мерчанта (merchant и payout), account_id вне набора отклоняется
// ErrUnknownAccount без раскрытия существования (fail-closed). Период фильтрует
// created_at транзакции: from включительно, to исключительно. Курсор -
// десятичный entry_id последней записи предыдущей страницы; nextCursor непуст,
// только если за страницей есть более новые записи.
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
			// Чужой или платформенный счет отвечает как ненайденный: существование
			// счетов других мерчантов не раскрывается.
			return nil, "", fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, filter.AccountID)
		}
		query = query.Where("e.account_id = ?", parsed.String())
	}
	if merchantRef != "" {
		// Выписка мерчанта охватывает только его счета плана счетов (merchant и
		// payout); платформенные счета (clearing/fee/suspense) недоступны.
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
	// limit+1 различает "страница кончилась" и "есть продолжение"; потолок
	// клампится выше, поэтому переполнение int невозможно.
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

// parseCursor переводит непрозрачный курсор в entry_id; 0 означает начало выписки.
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
