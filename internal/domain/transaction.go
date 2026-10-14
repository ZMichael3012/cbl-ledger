package domain

import (
	"fmt"
	"math"
	"strings"
)

// MaxEntries задает предельное число проводок в одной транзакции.
const MaxEntries = 100

// EntryInput описывает проводку во входе транзакции двойной записи.
type EntryInput struct {
	// AccountID задает счет проводки.
	AccountID string
	// Direction задает направление проводки.
	Direction Direction
	// Amount задает сумму в минимальных единицах актива.
	Amount int64
}

// ValidateEntries проверяет инварианты транзакции: минимум две проводки,
// положительные суммы, корректные направления и равенство дебетов и кредитов.
func ValidateEntries(entries []EntryInput) error {
	if len(entries) < 2 {
		return fmt.Errorf("%w: at least two entries required", ErrInvalidTransaction)
	}
	if len(entries) > MaxEntries {
		return fmt.Errorf("%w: entries limit %d exceeded", ErrInvalidTransaction, MaxEntries)
	}
	var debit, credit int64
	for i := range entries {
		entry := entries[i]
		if strings.TrimSpace(entry.AccountID) == "" {
			return fmt.Errorf("%w: entry %d: empty account_id", ErrInvalidEntry, i)
		}
		if !entry.Direction.Valid() {
			return fmt.Errorf("%w: entry %d: unknown direction %q", ErrInvalidEntry, i, entry.Direction)
		}
		if entry.Amount <= 0 {
			return fmt.Errorf("%w: entry %d: amount must be positive", ErrInvalidEntry, i)
		}
		switch entry.Direction {
		case DirectionDebit:
			if entry.Amount > math.MaxInt64-debit {
				return fmt.Errorf("%w: debit sum overflow", ErrInvalidEntry)
			}
			debit += entry.Amount
		case DirectionCredit:
			if entry.Amount > math.MaxInt64-credit {
				return fmt.Errorf("%w: credit sum overflow", ErrInvalidEntry)
			}
			credit += entry.Amount
		}
	}
	if debit != credit {
		return fmt.Errorf("%w: debit %d != credit %d", ErrUnbalanced, debit, credit)
	}
	return nil
}
