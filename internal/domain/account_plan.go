package domain

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// accountNamePrefix задает версионированный префикс канонического имени счета:
// смена схемы вывода идентификаторов выпускается новой версией префикса.
const accountNamePrefix = "cbl:ledger:account:v1"

// assetPattern ограничивает код актива плана счетов.
var assetPattern = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)

// merchantRefPattern ограничивает ссылку на мерчанта: без двоеточий,
// чтобы каноническое имя счета оставалось однозначным.
var merchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// AccountKey описывает бизнес-ключ счета плана счетов.
type AccountKey struct {
	// Type задает тип счета плана счетов.
	Type AccountType
	// Asset задает код актива счета.
	Asset string
	// Decimals задает количество знаков после точки.
	Decimals int32
	// MerchantRef задает ссылку на мерчанта (обязательна для merchant и payout).
	MerchantRef string
}

// Validate проверяет правила плана счетов для бизнес-ключа.
func (k AccountKey) Validate() error {
	if !k.Type.Valid() {
		return fmt.Errorf("%w: unknown account type %q", ErrInvalidAccountPlan, k.Type)
	}
	if !assetPattern.MatchString(k.Asset) {
		return fmt.Errorf("%w: asset %q", ErrInvalidAccountPlan, k.Asset)
	}
	if k.Decimals < 0 || k.Decimals > 18 {
		return fmt.Errorf("%w: decimals %d", ErrInvalidAccountPlan, k.Decimals)
	}
	personal := k.Type == AccountTypeMerchant || k.Type == AccountTypePayout
	if personal && !merchantRefPattern.MatchString(k.MerchantRef) {
		return fmt.Errorf("%w: %s account requires merchant_ref", ErrInvalidAccountPlan, k.Type)
	}
	if !personal && k.MerchantRef != "" {
		return fmt.Errorf("%w: %s account is platform-level", ErrInvalidAccountPlan, k.Type)
	}
	return nil
}

// CanonicalName возвращает каноническое имя счета для вывода идентификатора.
func (k AccountKey) CanonicalName() string {
	return fmt.Sprintf("%s:%s:%s:%d:%s", accountNamePrefix, k.Type, k.Asset, k.Decimals, k.MerchantRef)
}

// DeriveAccountID выводит детерминированный UUIDv5 счета из бизнес-ключа (ADR-2.6).
// Одинаковый ключ в любом сервисе дает один и тот же идентификатор; ключ обязан
// проходить Validate, иначе идентификатор не определен.
func DeriveAccountID(key AccountKey) (uuid.UUID, error) {
	if err := key.Validate(); err != nil {
		return uuid.Nil, err
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key.CanonicalName())), nil
}

// MerchantAccountKey собирает ключ мерчантского счета.
func MerchantAccountKey(merchantRef, asset string, decimals int32) AccountKey {
	return AccountKey{Type: AccountTypeMerchant, Asset: asset, Decimals: decimals, MerchantRef: merchantRef}
}

// PayoutAccountKey собирает ключ счета выплат мерчанта.
func PayoutAccountKey(merchantRef, asset string, decimals int32) AccountKey {
	return AccountKey{Type: AccountTypePayout, Asset: asset, Decimals: decimals, MerchantRef: merchantRef}
}

// PlatformAccountKey собирает ключ платформенного счета (clearing, fee или suspense).
func PlatformAccountKey(accountType AccountType, asset string, decimals int32) AccountKey {
	return AccountKey{Type: accountType, Asset: asset, Decimals: decimals}
}
