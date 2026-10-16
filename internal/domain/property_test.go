package domain_test

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// maxEntriesLocal is an independent copy of the entries limit (for checking without the domain).
const maxEntriesLocal = 100

// propertyRand returns the property test generator: a fixed seed
// by default, overridable via PROPERTY_SEED to localize rare failures.
func propertyRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := int64(20261014)
	if raw := os.Getenv("PROPERTY_SEED"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err, "PROPERTY_SEED")
		seed = parsed
	}
	t.Logf("property domain seed: %d", seed)
	return rand.New(rand.NewSource(seed)) //nolint:gosec // G404: property test seed, not cryptography
}

// rawEntryGen describes a raw input: the direction may be invalid,
// the account may be empty, the amount may have any sign.
type rawEntryGen struct {
	Amount     int64
	Direction  int
	AccountIdx int
}

// rawCase describes a random unbalanced input.
type rawCase struct {
	Entries []rawEntryGen
}

// toEntries converts the raw input into domain entries.
func (c rawCase) toEntries() []domain.EntryInput {
	entries := make([]domain.EntryInput, 0, len(c.Entries))
	for i, item := range c.Entries {
		var direction domain.Direction
		switch item.Direction {
		case 0:
			direction = domain.DirectionDebit
		case 1:
			direction = domain.DirectionCredit
		default:
			direction = domain.Direction(fmt.Sprintf("unknown-%d", item.Direction))
		}
		accountID := fmt.Sprintf("acc-%d", i%3)
		if item.AccountIdx < 0 {
			accountID = " "
		}
		entries = append(entries, domain.EntryInput{
			AccountID: accountID,
			Direction: direction,
			Amount:    item.Amount,
		})
	}
	return entries
}

// balancedCase describes an input that is balanced by construction.
type balancedCase struct {
	Amounts []int64
}

// toEntries builds debit/credit pairs of the same amount (within MaxEntries).
func (c balancedCase) toEntries() []domain.EntryInput {
	entries := make([]domain.EntryInput, 0, len(c.Amounts)*2)
	for i, value := range c.Amounts {
		amount := value
		if amount <= 0 {
			amount = -amount
		}
		if amount == 0 || amount == math.MinInt64 {
			amount = 1
		}
		if len(entries)+2 > maxEntriesLocal {
			break
		}
		entries = append(entries,
			domain.EntryInput{AccountID: fmt.Sprintf("acc-%d", i%3), Direction: domain.DirectionDebit, Amount: amount},
			domain.EntryInput{AccountID: fmt.Sprintf("acc-%d", i%3), Direction: domain.DirectionCredit, Amount: amount},
		)
	}
	return entries
}

// independentValid independently (big.Int, direction strings) checks the expected
// validity of the set: lengths, positive amounts, directions, non-empty accounts,
// no int64 overflow, equal debits and credits.
func independentValid(entries []domain.EntryInput) bool {
	if len(entries) < 2 || len(entries) > maxEntriesLocal {
		return false
	}
	debit, credit := big.NewInt(0), big.NewInt(0)
	for _, entry := range entries {
		if entry.Amount <= 0 || strings.TrimSpace(entry.AccountID) == "" {
			return false
		}
		amount := big.NewInt(entry.Amount)
		switch string(entry.Direction) {
		case "debit":
			debit.Add(debit, amount)
		case "credit":
			credit.Add(credit, amount)
		default:
			return false
		}
	}
	limit := big.NewInt(math.MaxInt64)
	if debit.Cmp(limit) > 0 || credit.Cmp(limit) > 0 {
		return false
	}
	return debit.Cmp(credit) == 0
}

// checkPropertyValidateEntries compares the domain verdict with the independent recount.
func checkPropertyValidateEntries(t *testing.T, entries []domain.EntryInput, label string) bool {
	t.Helper()
	err := domain.ValidateEntries(entries)
	expected := independentValid(entries)
	if (err == nil) != expected {
		t.Logf("%s: input %+v, err=%v, expected validity=%v", label, entries, err, expected)
		return false
	}
	if err == nil {
		// An accepted set must be balanced under the independent recount.
		debit, credit := big.NewInt(0), big.NewInt(0)
		for _, entry := range entries {
			amount := big.NewInt(entry.Amount)
			if entry.Direction == domain.DirectionDebit {
				debit.Add(debit, amount)
			} else {
				credit.Add(credit, amount)
			}
		}
		return debit.Cmp(credit) == 0
	}
	return true
}

func TestPropertyValidateEntriesBalanced(t *testing.T) {
	t.Parallel()

	// Guaranteed balanced inputs: the "accept" side of the property
	// must execute (not only rejections).
	rng := propertyRand(t)
	accepted := 0
	property := func(c balancedCase) bool {
		entries := c.toEntries()
		if err := domain.ValidateEntries(entries); err == nil {
			accepted++
		}
		return checkPropertyValidateEntries(t, entries, "balanced")
	}
	require.NoError(t, quick.Check(property, &quick.Config{MaxCount: 200, Rand: rng}))
	require.Greater(t, accepted, 0, "the property must accept balanced inputs")
}

func TestPropertyValidateEntriesRaw(t *testing.T) {
	t.Parallel()

	// Raw inputs: amounts of any sign and order, invalid directions and empty accounts.
	rng := propertyRand(t)
	rejected := 0
	property := func(c rawCase) bool {
		entries := c.toEntries()
		if err := domain.ValidateEntries(entries); err != nil {
			rejected++
		}
		return checkPropertyValidateEntries(t, entries, "raw")
	}
	require.NoError(t, quick.Check(property, &quick.Config{MaxCount: 200, Rand: rng}))
	require.Greater(t, rejected, 0, "raw inputs must produce rejections")
}

func TestPropertyValidateEntriesBoundaries(t *testing.T) {
	t.Parallel()

	build := func(pairs int) []domain.EntryInput {
		entries := make([]domain.EntryInput, 0, pairs*2)
		for i := 1; i <= pairs; i++ {
			entries = append(entries,
				domain.EntryInput{AccountID: fmt.Sprintf("a-%d", i), Direction: domain.DirectionDebit, Amount: int64(i)},
				domain.EntryInput{AccountID: fmt.Sprintf("b-%d", i), Direction: domain.DirectionCredit, Amount: int64(i)},
			)
		}
		return entries
	}

	// The limit is accepted, the limit minus a step is accepted, the limit plus a pair is rejected.
	require.NoError(t, domain.ValidateEntries(build(maxEntriesLocal/2)))
	require.NoError(t, domain.ValidateEntries(build(maxEntriesLocal/2-1)))
	require.ErrorIs(t, domain.ValidateEntries(build(maxEntriesLocal/2+1)), domain.ErrInvalidTransaction)

	// int64 overflow is rejected even when the sums are formally equal.
	overflow := []domain.EntryInput{
		{AccountID: "a", Direction: domain.DirectionDebit, Amount: math.MaxInt64},
		{AccountID: "b", Direction: domain.DirectionDebit, Amount: math.MaxInt64},
		{AccountID: "c", Direction: domain.DirectionCredit, Amount: math.MaxInt64},
		{AccountID: "d", Direction: domain.DirectionCredit, Amount: math.MaxInt64},
	}
	require.ErrorIs(t, domain.ValidateEntries(overflow), domain.ErrInvalidEntry)
}
