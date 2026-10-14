// Пакет domain содержит доменные модели, инварианты и правила сервиса ledger,
// не зависящие от транспорта и хранилищ.
package domain

// AccountType описывает назначение счета в плане счетов леджера.
type AccountType string

// Типы счетов плана счетов.
const (
	// AccountTypeMerchant описывает балансовый счет мерчанта.
	AccountTypeMerchant AccountType = "merchant"
	// AccountTypeClearing описывает транзитный счет приема платежей.
	AccountTypeClearing AccountType = "clearing"
	// AccountTypeFee описывает счет комиссий платформы.
	AccountTypeFee AccountType = "fee"
	// AccountTypePayout описывает счет выплат.
	AccountTypePayout AccountType = "payout"
	// AccountTypeSuspense описывает счет невыясненных расхождений.
	AccountTypeSuspense AccountType = "suspense"
)

// Valid сообщает, что тип счета входит в план счетов.
func (t AccountType) Valid() bool {
	switch t {
	case AccountTypeMerchant, AccountTypeClearing, AccountTypeFee, AccountTypePayout, AccountTypeSuspense:
		return true
	default:
		return false
	}
}

// Direction описывает направление проводки в двойной записи.
type Direction string

// Направления проводок.
const (
	// DirectionDebit уменьшает баланс счета.
	DirectionDebit Direction = "debit"
	// DirectionCredit увеличивает баланс счета.
	DirectionCredit Direction = "credit"
)

// Valid сообщает, что направление корректно.
func (d Direction) Valid() bool {
	return d == DirectionDebit || d == DirectionCredit
}
