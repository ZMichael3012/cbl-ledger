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

// ledger.events event constants (CBL-003, section 6).
const (
	// ProducerLedger sets the name of the ledger event producer service.
	ProducerLedger = "cbl-ledger"
	// TopicLedgerEvents sets the ledger events topic.
	TopicLedgerEvents = "ledger.events"
	// SchemaVersion sets the version of the ledger event schema.
	SchemaVersion = "v1"
)

// LedgerEventSink builds ledger.events protobuf envelopes and writes them to the outbox
// within the business change transaction (storage.TxnEventSink).
type LedgerEventSink struct {
	store *outbox.Store
	log   *zap.Logger
}

// NewLedgerEventSink creates a LedgerEventSink.
func NewLedgerEventSink(log *zap.Logger) *LedgerEventSink {
	if log == nil {
		log = zap.NewNop()
	}
	return &LedgerEventSink{store: outbox.NewStore(), log: log}
}

// AppendTxnEvent builds the event envelope and writes it to the outbox.
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

// buildLedgerEventEnvelope builds the ledger.events envelope: the protobuf body
// billing.events.v1.LedgerEvent and the CBL envelope headers.
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

// ledgerEventType converts the string event type into the contract enum.
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

// entryDirection converts the domain direction into the contract one.
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

// decimalsToUint32 safely converts decimals into the contract uint32.
func decimalsToUint32(decimals int32) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals)
}
