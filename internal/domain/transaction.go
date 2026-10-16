package domain

import (
	"fmt"
	"math"
	"strings"
)

// MaxEntries sets the maximum number of entries in one transaction.
const MaxEntries = 100

// EntryInput describes an entry in a double-entry transaction input.
type EntryInput struct {
	// AccountID sets the account of the entry.
	AccountID string
	// Direction sets the direction of the entry.
	Direction Direction
	// Amount sets the amount in minor units of the asset.
	Amount int64
}

// ValidateEntries checks the transaction invariants: at least two entries,
// positive amounts, valid directions and equal debits and credits.
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
