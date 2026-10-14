package storage

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicy(t *testing.T) {
	want := RetryPolicy{
		MaxAttempts: 8,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    250 * time.Millisecond,
	}
	assert.Equal(t, want, DefaultRetryPolicy())
}

func TestNormalizeRetryPolicy(t *testing.T) {
	defaults := DefaultRetryPolicy()

	assert.Equal(t, defaults, normalizeRetryPolicy(RetryPolicy{}), "нулевая политика заменяется значениями по умолчанию")

	partial := normalizeRetryPolicy(RetryPolicy{MaxAttempts: -3, BaseDelay: -time.Second})
	assert.Equal(t, defaults.MaxAttempts, partial.MaxAttempts)
	assert.Equal(t, defaults.BaseDelay, partial.BaseDelay)
	assert.Equal(t, defaults.MaxDelay, partial.MaxDelay)

	capped := normalizeRetryPolicy(RetryPolicy{MaxAttempts: 4, BaseDelay: time.Second, MaxDelay: time.Millisecond})
	assert.Equal(t, 4, capped.MaxAttempts)
	assert.Equal(t, time.Second, capped.BaseDelay)
	assert.Equal(t, time.Second, capped.MaxDelay, "потолок не может быть ниже базовой паузы")

	custom := RetryPolicy{MaxAttempts: 3, BaseDelay: 2 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	assert.Equal(t, custom, normalizeRetryPolicy(custom), "валидная политика не меняется")

	ceiling := normalizeRetryPolicy(RetryPolicy{MaxAttempts: 1000})
	assert.Equal(t, maxRetryAttemptsCeiling, ceiling.MaxAttempts, "число попыток ограничено сверху")
}

func TestNextDelayBounds(t *testing.T) {
	policy := DefaultRetryPolicy()

	var gotArg int64
	delay := policy.nextDelay(0, func(n int64) int64 { gotArg = n; return n - 1 })
	assert.Equal(t, policy.BaseDelay, delay)
	assert.Equal(t, int64(policy.BaseDelay)+1, gotArg, "диапазон аргумента равен задержке плюс один")

	delay = policy.nextDelay(1, func(n int64) int64 { return n - 1 })
	assert.Equal(t, 2*policy.BaseDelay, delay)

	delay = policy.nextDelay(6, func(n int64) int64 { return n - 1 })
	assert.Equal(t, policy.MaxDelay, delay, "потолок ограничивает задержку")

	delay = policy.nextDelay(30, func(n int64) int64 { return n - 1 })
	assert.Equal(t, policy.MaxDelay, delay, "большие номера попыток не переполняют задержку")

	zero := policy.nextDelay(3, func(int64) int64 { return 0 })
	assert.Zero(t, zero, "полный jitter допускает нулевую паузу")
}

func TestNextDelayExtremeMaxDelayNoPanic(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 8, BaseDelay: 5 * time.Millisecond, MaxDelay: time.Duration(math.MaxInt64)}

	var gotArg int64
	delay := policy.nextDelay(62, func(n int64) int64 { gotArg = n; return n - 1 })
	assert.Equal(t, time.Duration(math.MaxInt64-1), delay)
	assert.Equal(t, int64(math.MaxInt64), gotArg, "граница не переполняется добавлением единицы")
}

func TestRetryCause(t *testing.T) {
	assert.Equal(t, "lock_conflict", retryCause(errLockConflict))
	assert.Equal(t, "idempotency_race", retryCause(fmt.Errorf("storage: wrap: %w", errIdempotentRace)))
	assert.Equal(t, "deadlock", retryCause(fmt.Errorf("storage: tx: %w", &pgconn.PgError{Code: "40P01"})))
	assert.Equal(t, "unknown", retryCause(errors.New("unexpected")))
}
