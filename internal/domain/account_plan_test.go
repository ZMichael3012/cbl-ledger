package domain_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-ledger/internal/domain"
)

func TestAccountKeyValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  domain.AccountKey
		want error
	}{
		{"merchant with ref", domain.MerchantAccountKey("m-1", "USDT", 2), nil},
		{"payout with ref", domain.PayoutAccountKey("m-1", "USDT", 2), nil},
		{"ref at length limit", domain.MerchantAccountKey(strings.Repeat("m", 128), "USDT", 2), nil},
		{"ref above length limit", domain.MerchantAccountKey(strings.Repeat("m", 129), "USDT", 2), domain.ErrInvalidAccountPlan},
		{"clearing without ref", domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2), nil},
		{"fee without ref", domain.PlatformAccountKey(domain.AccountTypeFee, "BTC", 8), nil},
		{"suspense without ref", domain.PlatformAccountKey(domain.AccountTypeSuspense, "USDT", 0), nil},
		{"decimals at upper bound", domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 18), nil},
		{"unknown type", domain.AccountKey{Type: "vault", Asset: "USDT", Decimals: 2}, domain.ErrInvalidAccountPlan},
		{"asset lowercase", domain.PlatformAccountKey(domain.AccountTypeFee, "usdt", 2), domain.ErrInvalidAccountPlan},
		{"asset empty", domain.PlatformAccountKey(domain.AccountTypeFee, "", 2), domain.ErrInvalidAccountPlan},
		{"asset too long", domain.PlatformAccountKey(domain.AccountTypeFee, "AAAAAAAAAAAAAAAAA", 2), domain.ErrInvalidAccountPlan},
		{"asset with dash", domain.PlatformAccountKey(domain.AccountTypeFee, "US-DT", 2), domain.ErrInvalidAccountPlan},
		{"negative decimals", domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", -1), domain.ErrInvalidAccountPlan},
		{"decimals above eighteen", domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 19), domain.ErrInvalidAccountPlan},
		{"merchant without ref", domain.MerchantAccountKey("", "USDT", 2), domain.ErrInvalidAccountPlan},
		{"payout without ref", domain.PayoutAccountKey("", "USDT", 2), domain.ErrInvalidAccountPlan},
		{"merchant ref with colon", domain.MerchantAccountKey("m:1", "USDT", 2), domain.ErrInvalidAccountPlan},
		{"clearing with ref", domain.AccountKey{Type: domain.AccountTypeClearing, Asset: "USDT", Decimals: 2, MerchantRef: "m-1"}, domain.ErrInvalidAccountPlan},
		{"fee with ref", domain.AccountKey{Type: domain.AccountTypeFee, Asset: "USDT", Decimals: 2, MerchantRef: "m-1"}, domain.ErrInvalidAccountPlan},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.key.Validate()
			if tt.want == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, tt.want))
		})
	}
}

func TestDeriveAccountIDGoldenVectors(t *testing.T) {
	t.Parallel()

	// Векторы фиксируют кросс-сервисный контракт (CBL-003, план счетов):
	// их обязаны воспроизводить все реализации вывода account_id.
	vectors := []struct {
		key domain.AccountKey
		id  string
	}{
		{domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2), "dec27141-ec8f-55de-a325-04a35cd6b0f2"},
		{domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2), "b138b712-5940-59bc-82bd-19f653642227"},
		{domain.PlatformAccountKey(domain.AccountTypeSuspense, "USDT", 2), "cb2ccbda-92f9-5889-ba9f-c7ec90b8cfa0"},
		{domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 0), "8ee4265f-342c-5dbc-8d80-2516a5641b59"},
		{domain.MerchantAccountKey("m-1", "USDT", 2), "58686062-c133-5d4e-b08b-0d33a0db924f"},
		{domain.MerchantAccountKey("m-1", "BTC", 8), "e1a671a1-a662-5305-b144-6e623099ea46"},
		{domain.MerchantAccountKey("M-1_a", "USDT", 2), "00f2dc5b-a0c1-51c1-b2eb-f261e0ed538f"},
		{domain.PayoutAccountKey("m-1", "USDT", 2), "0595b42d-8f10-5bb5-b98f-a349594bc3f8"},
	}
	for _, vector := range vectors {
		id, err := domain.DeriveAccountID(vector.key)
		require.NoError(t, err)
		assert.Equal(t, vector.id, id.String(), "ключ %s", vector.key.CanonicalName())
	}
}

func TestDeriveAccountIDDeterministic(t *testing.T) {
	t.Parallel()

	first, err := domain.DeriveAccountID(domain.MerchantAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	second, err := domain.DeriveAccountID(domain.MerchantAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	assert.Equal(t, first, second, "одинаковый ключ дает одинаковый id")

	otherDecimals, err := domain.DeriveAccountID(domain.MerchantAccountKey("m-1", "USDT", 6))
	require.NoError(t, err)
	assert.NotEqual(t, first, otherDecimals, "decimals входит в ключ")

	otherType, err := domain.DeriveAccountID(domain.PayoutAccountKey("m-1", "USDT", 2))
	require.NoError(t, err)
	assert.NotEqual(t, first, otherType, "тип входит в ключ")

	_, err = domain.DeriveAccountID(domain.AccountKey{Type: "vault", Asset: "USDT", Decimals: 2})
	require.Error(t, err)
	assert.ErrorIs(t, err, domain.ErrInvalidAccountPlan)
}

func TestCanonicalName(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		"cbl:ledger:account:v1:merchant:USDT:2:m-1",
		domain.MerchantAccountKey("m-1", "USDT", 2).CanonicalName())
	assert.Equal(t,
		"cbl:ledger:account:v1:clearing:USDT:2:",
		domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2).CanonicalName())
}
