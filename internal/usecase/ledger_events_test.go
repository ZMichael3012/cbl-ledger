package usecase

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func TestBuildLedgerEventEnvelope(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	event := storage.TxnEvent{
		Type:          storage.EventTransactionPosted,
		TransactionID: "01a138fd-0000-7000-8000-000000000001",
		RefType:       "payment",
		RefID:         "pay-1",
		OccurredAt:    occurredAt,
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Key:           "acc-1",
		Entries: []storage.TxnEventEntry{
			{AccountID: "acc-1", Direction: domain.DirectionDebit, Amount: 100_000, Asset: "USDT", Decimals: 2},
			{AccountID: "acc-2", Direction: domain.DirectionCredit, Amount: 100_000, Asset: "USDT", Decimals: 2},
		},
	}

	envelope, err := buildLedgerEventEnvelope(event)
	require.NoError(t, err)
	assert.Equal(t, TopicLedgerEvents, envelope.Topic)
	assert.Equal(t, "acc-1", envelope.Key)
	assert.Equal(t, storage.EventTransactionPosted, envelope.Headers[outbox.HeaderEventType])
	assert.Equal(t, ProducerLedger, envelope.Headers[outbox.HeaderProducer])
	assert.Equal(t, SchemaVersion, envelope.Headers[outbox.HeaderSchemaVersion])
	assert.Equal(t, event.Traceparent, envelope.Headers[outbox.HeaderTraceparent])
	assert.NotEmpty(t, envelope.Headers[outbox.HeaderEventID])
	assert.Equal(t, occurredAt.Format(time.RFC3339Nano), envelope.Headers[outbox.HeaderOccurredAt])

	var payload eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(envelope.Payload, &payload))
	assert.Equal(t, event.TransactionID, payload.GetTransactionId())
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED, payload.GetType())
	assert.Equal(t, "payment", payload.GetRefType())
	assert.Equal(t, "pay-1", payload.GetRefId())
	require.Len(t, payload.GetEntries(), 2)
	assert.Equal(t, commonv1.Direction_DIRECTION_DEBIT, payload.GetEntries()[0].GetDirection())
	assert.Equal(t, int64(100_000), payload.GetEntries()[0].GetAmount().GetMinorUnits())
	assert.Equal(t, "USDT", payload.GetEntries()[0].GetAmount().GetAsset())
	assert.Equal(t, uint32(2), payload.GetEntries()[0].GetAmount().GetDecimals())
	assert.Equal(t, occurredAt, payload.GetOccurredAt().AsTime())
}

func TestBuildLedgerEventEnvelopeRejects(t *testing.T) {
	t.Parallel()

	valid := storage.TxnEvent{
		Type:       storage.EventReserveCreated,
		RefType:    "reserve",
		RefID:      "payout-1",
		OccurredAt: time.Now().UTC(),
		Key:        "acc-1",
	}

	t.Run("zero occurred_at", func(t *testing.T) {
		broken := valid
		broken.OccurredAt = time.Time{}
		_, err := buildLedgerEventEnvelope(broken)
		require.Error(t, err)
	})

	t.Run("empty key", func(t *testing.T) {
		broken := valid
		broken.Key = ""
		_, err := buildLedgerEventEnvelope(broken)
		require.Error(t, err)
	})

	t.Run("empty event type", func(t *testing.T) {
		broken := valid
		broken.Type = ""
		_, err := buildLedgerEventEnvelope(broken)
		require.Error(t, err)
	})
}

func TestLedgerEventTypeMapping(t *testing.T) {
	t.Parallel()

	tests := map[string]eventsv1.LedgerEventType{
		storage.EventTransactionPosted: eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED,
		storage.EventReserveCreated:    eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_CREATED,
		storage.EventReserveCommitted:  eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_COMMITTED,
		storage.EventReserveReleased:   eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_RELEASED,
		"unknown":                      eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_UNSPECIFIED,
	}
	for input, want := range tests {
		assert.Equal(t, want, ledgerEventType(input), input)
	}
}

func completedEvent(paymentID, merchantID string, received int64, fee *commonv1.Money) *eventsv1.PaymentEvent {
	return &eventsv1.PaymentEvent{
		PaymentId:      paymentID,
		MerchantId:     merchantID,
		Type:           eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		ReceivedAmount: &commonv1.Money{MinorUnits: received, Asset: "USDT", Decimals: 2},
		Fee:            fee,
		OccurredAt:     timestamppb.New(time.Now().UTC()),
	}
}

func TestBuildCompletedPosting(t *testing.T) {
	t.Parallel()

	const paymentID = "01a138fd-0000-7000-8000-000000000001"

	t.Run("with fee", func(t *testing.T) {
		event := completedEvent(paymentID, "m-1", 100_000, &commonv1.Money{MinorUnits: 1_500, Asset: "USDT", Decimals: 2})
		posting, err := buildCompletedPosting(event)
		require.NoError(t, err)
		assert.Equal(t, int64(100_000), posting.Received)
		assert.Equal(t, int64(1_500), posting.Fee)
		assert.Equal(t, "USDT", posting.Asset)
		assert.Equal(t, int32(2), posting.Decimals)
		assert.Equal(t, "m-1", posting.MerchantKey.MerchantRef)
	})

	t.Run("without fee", func(t *testing.T) {
		posting, err := buildCompletedPosting(completedEvent(paymentID, "m-1", 100, nil))
		require.NoError(t, err)
		assert.Zero(t, posting.Fee)
	})

	t.Run("fee equals received", func(t *testing.T) {
		posting, err := buildCompletedPosting(completedEvent(paymentID, "m-1", 100, &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2}))
		require.NoError(t, err)
		assert.Equal(t, int64(100), posting.Fee)
	})

	t.Run("payment id canonicalized", func(t *testing.T) {
		// Бизнес-ключ обязан не зависеть от формы UUID в событии.
		upper := completedEvent("01A138FD-0000-7000-8000-000000000001", "m-1", 100, nil)
		posting, err := buildCompletedPosting(upper)
		require.NoError(t, err)
		assert.Equal(t, paymentID, posting.PaymentID)

		compact := completedEvent("01a138fd000070008000000000000001", "m-1", 100, nil)
		posting, err = buildCompletedPosting(compact)
		require.NoError(t, err)
		assert.Equal(t, paymentID, posting.PaymentID)

		braced := completedEvent("{01a138fd-0000-7000-8000-000000000001}", "m-1", 100, nil)
		posting, err = buildCompletedPosting(braced)
		require.NoError(t, err)
		assert.Equal(t, paymentID, posting.PaymentID)
	})

	rejections := map[string]*eventsv1.PaymentEvent{
		"invalid payment_id":  completedEvent("not-a-uuid", "m-1", 100, nil),
		"missing merchant":    completedEvent(paymentID, "", 100, nil),
		"merchant with colon": completedEvent(paymentID, "m:1", 100, nil),
		"zero received":       completedEvent(paymentID, "m-1", 0, nil),
		"negative received":   completedEvent(paymentID, "m-1", -1, nil),
		"negative fee": completedEvent(paymentID, "m-1", 100,
			&commonv1.Money{MinorUnits: -1, Asset: "USDT", Decimals: 2}),
		"fee exceeds received": completedEvent(paymentID, "m-1", 100,
			&commonv1.Money{MinorUnits: 101, Asset: "USDT", Decimals: 2}),
		"fee other asset": completedEvent(paymentID, "m-1", 100,
			&commonv1.Money{MinorUnits: 1, Asset: "BTC", Decimals: 8}),
		"fee other decimals": completedEvent(paymentID, "m-1", 100,
			&commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 6}),
	}
	for name, event := range rejections {
		t.Run(name, func(t *testing.T) {
			_, err := buildCompletedPosting(event)
			require.Error(t, err)
			assert.True(t, errors.Is(err, errInvalidPaymentEvent), "должна быть доменная ошибка: %v", err)
		})
	}
}
