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

// maxEntriesLocal - независимая копия предела проводок (для проверки без домена).
const maxEntriesLocal = 100

// propertyRand возвращает генератор property-тестов: фиксированный сид
// по умолчанию, переопределяемый PROPERTY_SEED для локализации редких сбоев.
func propertyRand(t *testing.T) *rand.Rand {
	t.Helper()
	seed := int64(20261014)
	if raw := os.Getenv("PROPERTY_SEED"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err, "PROPERTY_SEED")
		seed = parsed
	}
	t.Logf("property domain seed: %d", seed)
	return rand.New(rand.NewSource(seed)) //nolint:gosec // G404: сид property-теста, не криптография
}

// rawEntryGen описывает сырой вход: направление может быть некорректным,
// счет - пустым, сумма - любого знака.
type rawEntryGen struct {
	Amount     int64
	Direction  int
	AccountIdx int
}

// rawCase описывает случайный несбалансированный вход.
type rawCase struct {
	Entries []rawEntryGen
}

// toEntries переводит сырой вход в проводки домена.
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

// balancedCase описывает вход, который по построению сбалансирован.
type balancedCase struct {
	Amounts []int64
}

// toEntries строит пары дебет/кредит одной суммы (в пределах MaxEntries).
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

// independentValid независимо (big.Int, строки направлений) проверяет ожидаемую
// валидность набора: длины, положительные суммы, направления, непустые счета,
// отсутствие переполнения int64, равенство дебетов и кредитов.
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

// checkPropertyValidateEntries сверяет вердикт домена с независимым пересчетом.
func checkPropertyValidateEntries(t *testing.T, entries []domain.EntryInput, label string) bool {
	t.Helper()
	err := domain.ValidateEntries(entries)
	expected := independentValid(entries)
	if (err == nil) != expected {
		t.Logf("%s: вход %+v, err=%v, ожидание валидности=%v", label, entries, err, expected)
		return false
	}
	if err == nil {
		// Принятый набор обязан быть сбалансированным при независимом пересчете.
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

	// Гарантированно сбалансированные входы: направление "принятие" свойства
	// обязано исполняться (не только отказы).
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
	require.Greater(t, accepted, 0, "свойство обязано принимать сбалансированные входы")
}

func TestPropertyValidateEntriesRaw(t *testing.T) {
	t.Parallel()

	// Сырые входы: суммы любого знака и порядка, невалидные направления и пустые счета.
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
	require.Greater(t, rejected, 0, "сырые входы обязаны порождать отказы")
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

	// Предел принимается, предел минус шаг принимается, предел плюс пара отклоняется.
	require.NoError(t, domain.ValidateEntries(build(maxEntriesLocal/2)))
	require.NoError(t, domain.ValidateEntries(build(maxEntriesLocal/2-1)))
	require.ErrorIs(t, domain.ValidateEntries(build(maxEntriesLocal/2+1)), domain.ErrInvalidTransaction)

	// Переполнение int64 отклоняется даже при формально равных суммах.
	overflow := []domain.EntryInput{
		{AccountID: "a", Direction: domain.DirectionDebit, Amount: math.MaxInt64},
		{AccountID: "b", Direction: domain.DirectionDebit, Amount: math.MaxInt64},
		{AccountID: "c", Direction: domain.DirectionCredit, Amount: math.MaxInt64},
		{AccountID: "d", Direction: domain.DirectionCredit, Amount: math.MaxInt64},
	}
	require.ErrorIs(t, domain.ValidateEntries(overflow), domain.ErrInvalidEntry)
}
