package usecase

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// Константы событий ledger.events (CBL-003, раздел 6).
const (
	// ProducerLedger задает имя сервиса-продюсера событий леджера.
	ProducerLedger = "cbl-ledger"
	// TopicLedgerEvents задает топик событий леджера.
	TopicLedgerEvents = "ledger.events"
	// SchemaVersion задает версию схемы событий леджера.
	SchemaVersion = "v1"
)

// LedgerEventSink собирает protobuf-конверты ledger.events и пишет их в outbox
// в транзакции бизнес-изменения (storage.TxnEventSink).
type LedgerEventSink struct {
	store *outbox.Store
	log   *zap.Logger
}

// NewLedgerEventSink создает LedgerEventSink.
func NewLedgerEventSink(log *zap.Logger) *LedgerEventSink {
	if log == nil {
		log = zap.NewNop()
	}
	return &LedgerEventSink{store: outbox.NewStore(), log: log}
}

// AppendTxnEvent собирает конверт события и записывает его в outbox.
func (s *LedgerEventSink) AppendTxnEvent(ctx context.Context, tx *gorm.DB, event storage.TxnEvent) error {
	envelope, err := buildLedgerEventEnvelope(event)
	if err != nil {
		return err
	}
	if err := s.store.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append ledger event: %w", err)
	}
	s.log.Debug("ledger event queued in outbox",
		zap.String("event_id", envelope.EventID),
		zap.String("event_type", event.Type),
		zap.String("transaction_id", event.TransactionID))
	return nil
}

// buildLedgerEventEnvelope собирает конверт ledger.events: protobuf-тело
// billing.events.v1.LedgerEvent и заголовки конверта CBL.
func buildLedgerEventEnvelope(event storage.TxnEvent) (outbox.Envelope, error) {
	if event.OccurredAt.IsZero() {
		return outbox.Envelope{}, fmt.Errorf("usecase: event %s has zero occurred_at", event.Type)
	}
	payload := &eventsv1.LedgerEvent{
		TransactionId: event.TransactionID,
		Type:          ledgerEventType(event.Type),
		RefType:       event.RefType,
		RefId:         event.RefID,
		OccurredAt:    timestamppb.New(event.OccurredAt.UTC()),
	}
	for _, entry := range event.Entries {
		payload.Entries = append(payload.Entries, &eventsv1.LedgerEntry{
			AccountId: entry.AccountID,
			Direction: entryDirection(entry.Direction),
			Amount: &commonv1.Money{
				MinorUnits: entry.Amount,
				Asset:      entry.Asset,
				Decimals:   decimalsToUint32(entry.Decimals),
			},
		})
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: marshal ledger event: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerLedger,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if event.Traceparent != "" {
		headers[outbox.HeaderTraceparent] = event.Traceparent
	}
	envelope, err := outbox.NewEnvelope(event.Type, TopicLedgerEvents, event.Key, headers, body)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: build ledger envelope: %w", err)
	}
	return envelope, nil
}

// ledgerEventType переводит строковый тип события в контрактный enum.
func ledgerEventType(eventType string) eventsv1.LedgerEventType {
	switch eventType {
	case storage.EventTransactionPosted:
		return eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED
	case storage.EventReserveCreated:
		return eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_CREATED
	case storage.EventReserveCommitted:
		return eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_COMMITTED
	case storage.EventReserveReleased:
		return eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_RELEASED
	default:
		return eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_UNSPECIFIED
	}
}

// entryDirection переводит доменное направление в контрактное.
func entryDirection(direction domain.Direction) commonv1.Direction {
	switch direction {
	case domain.DirectionDebit:
		return commonv1.Direction_DIRECTION_DEBIT
	case domain.DirectionCredit:
		return commonv1.Direction_DIRECTION_CREDIT
	default:
		return commonv1.Direction_DIRECTION_UNSPECIFIED
	}
}

// decimalsToUint32 безопасно переводит decimals в uint32 контракта.
func decimalsToUint32(decimals int32) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals)
}
