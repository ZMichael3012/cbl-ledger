package domain

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// accountNamePrefix sets the versioned prefix of the canonical account name:
// a change of the identifier derivation scheme is released as a new prefix version.
const accountNamePrefix = "cbl:ledger:account:v1"

// assetPattern constrains the account plan asset code.
var assetPattern = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)

// merchantRefPattern constrains the merchant reference: no colons,
// so that the canonical account name stays unambiguous.
var merchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// AccountKey describes the business key of an account plan account.
type AccountKey struct {
	// Type sets the account plan account type.
	Type AccountType
	// Asset sets the account asset code.
	Asset string
	// Decimals sets the number of decimal places.
	Decimals int32
	// MerchantRef sets the merchant reference (required for merchant and payout).
	MerchantRef string
}

// Validate checks the account plan rules for the business key.
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

// CanonicalName returns the canonical account name used to derive the identifier.
func (k AccountKey) CanonicalName() string {
	return fmt.Sprintf("%s:%s:%s:%d:%s", accountNamePrefix, k.Type, k.Asset, k.Decimals, k.MerchantRef)
}

// DeriveAccountID derives the deterministic UUIDv5 of an account from the business key (ADR-2.6).
// The same key in any service yields the same identifier; the key must
// pass Validate, otherwise the identifier is undefined.
func DeriveAccountID(key AccountKey) (uuid.UUID, error) {
	if err := key.Validate(); err != nil {
		return uuid.Nil, err
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key.CanonicalName())), nil
}

// MerchantAccountKey builds a merchant account key.
func MerchantAccountKey(merchantRef, asset string, decimals int32) AccountKey {
	return AccountKey{Type: AccountTypeMerchant, Asset: asset, Decimals: decimals, MerchantRef: merchantRef}
}

// ValidMerchantRef reports whether the merchant reference is allowed by the account plan;
// used by the transport for untrusted input of statement filters.
func ValidMerchantRef(ref string) bool {
	return merchantRefPattern.MatchString(ref)
}

// PayoutAccountKey builds a merchant payout account key.
func PayoutAccountKey(merchantRef, asset string, decimals int32) AccountKey {
	return AccountKey{Type: AccountTypePayout, Asset: asset, Decimals: decimals, MerchantRef: merchantRef}
}

// PlatformAccountKey builds a platform account key (clearing, fee or suspense).
func PlatformAccountKey(accountType AccountType, asset string, decimals int32) AccountKey {
	return AccountKey{Type: accountType, Asset: asset, Decimals: decimals}
}
