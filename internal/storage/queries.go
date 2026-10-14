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
// большее значение клампится потолком.
const (
	defaultEntriesLimit = 50
	maxEntriesLimit     = 200
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

// ListEntries возвращает страницу выписки счета по возрастанию entry_id.
// Курсор - десятичный entry_id последней записи предыдущей страницы; nextCursor
// непуст, только если за страницей есть более новые записи.
func (r *Repository) ListEntries(ctx context.Context, accountID string, limit uint32, cursor string) ([]EntryView, string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(accountID))
	if err != nil {
		return nil, "", fmt.Errorf("%w: account_id %q", domain.ErrUnknownAccount, accountID)
	}
	afterID, err := parseCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit == 0 {
		limit = defaultEntriesLimit
	}
	if limit > maxEntriesLimit {
		limit = maxEntriesLimit
	}

	db := r.db.WithContext(ctx)
	if _, err := loadAccountByID(db, parsed.String()); err != nil {
		return nil, "", err
	}
	query := db.Table("entries AS e").
		Select("e.id AS entry_id, e.transaction_id, e.account_id, e.direction, e.amount, "+
			"t.created_at, t.ref_type, t.ref_id, a.asset, a.decimals").
		Joins("JOIN transactions AS t ON t.id = e.transaction_id").
		Joins("JOIN accounts AS a ON a.id = e.account_id").
		Where("e.account_id = ?", parsed.String())
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
