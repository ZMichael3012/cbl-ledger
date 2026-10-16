// Package domain contains domain models, invariants and rules of the ledger service
// that do not depend on transport or storage.
package domain

// AccountType describes the purpose of an account in the ledger account plan.
type AccountType string

// Account plan types.
const (
	// AccountTypeMerchant describes a merchant balance account.
	AccountTypeMerchant AccountType = "merchant"
	// AccountTypeClearing describes a transit account for incoming payments.
	AccountTypeClearing AccountType = "clearing"
	// AccountTypeFee describes a platform fee account.
	AccountTypeFee AccountType = "fee"
	// AccountTypePayout describes a payout account.
	AccountTypePayout AccountType = "payout"
	// AccountTypeSuspense describes a suspense account for unresolved discrepancies.
	AccountTypeSuspense AccountType = "suspense"
)

// Valid reports whether the account type belongs to the account plan.
func (t AccountType) Valid() bool {
	switch t {
	case AccountTypeMerchant, AccountTypeClearing, AccountTypeFee, AccountTypePayout, AccountTypeSuspense:
		return true
	default:
		return false
	}
}

// Direction describes the direction of an entry in double-entry bookkeeping.
type Direction string

// Entry directions.
const (
	// DirectionDebit decreases the account balance.
	DirectionDebit Direction = "debit"
	// DirectionCredit increases the account balance.
	DirectionCredit Direction = "credit"
)

// Valid reports whether the direction is valid.
func (d Direction) Valid() bool {
	return d == DirectionDebit || d == DirectionCredit
}
