package domain

import "fmt"

// ReserveStatus describes the state of a funds reservation.
type ReserveStatus string

// Reserve states.
const (
	// ReserveStatusActive describes an active hold on funds.
	ReserveStatusActive ReserveStatus = "active"
	// ReserveStatusCommitted describes a reserve converted into an entry.
	ReserveStatusCommitted ReserveStatus = "committed"
	// ReserveStatusReleased describes a released reserve.
	ReserveStatusReleased ReserveStatus = "released"
)

// Valid reports whether the reserve status is known.
func (s ReserveStatus) Valid() bool {
	switch s {
	case ReserveStatusActive, ReserveStatusCommitted, ReserveStatusReleased:
		return true
	default:
		return false
	}
}

// Terminal reports that the reserve is finished and no transitions from it are possible.
func (s ReserveStatus) Terminal() bool {
	return s == ReserveStatusCommitted || s == ReserveStatusReleased
}

// ValidateReserveAmount checks the hold amount: strictly positive.
func ValidateReserveAmount(amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidReserve)
	}
	return nil
}
