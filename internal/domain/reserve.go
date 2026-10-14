package domain

import "fmt"

// ReserveStatus описывает состояние резерва средств.
type ReserveStatus string

// Состояния резерва.
const (
	// ReserveStatusActive описывает активное удержание средств.
	ReserveStatusActive ReserveStatus = "active"
	// ReserveStatusCommitted описывает резерв, превращенный в проводку.
	ReserveStatusCommitted ReserveStatus = "committed"
	// ReserveStatusReleased описывает возвращенный резерв.
	ReserveStatusReleased ReserveStatus = "released"
)

// Valid сообщает, что статус резерва известен.
func (s ReserveStatus) Valid() bool {
	switch s {
	case ReserveStatusActive, ReserveStatusCommitted, ReserveStatusReleased:
		return true
	default:
		return false
	}
}

// Terminal сообщает, что резерв завершен и переходы из него невозможны.
func (s ReserveStatus) Terminal() bool {
	return s == ReserveStatusCommitted || s == ReserveStatusReleased
}

// ValidateReserveAmount проверяет сумму удержания: строго положительная.
func ValidateReserveAmount(amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidReserve)
	}
	return nil
}
