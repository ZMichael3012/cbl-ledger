package storage

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// Event types of ledger.events (CBL-003, section 6).
const (
	// EventTransactionPosted describes a double-entry posting.
	EventTransactionPosted = "transaction.posted"
	// EventReserveCreated describes the creation of a hold.
	EventReserveCreated = "reserve.created"
	// EventReserveCommitted describes the capture of a hold.
	EventReserveCommitted = "reserve.committed"
	// EventReserveReleased describes the release of a hold.
	EventReserveReleased = "reserve.released"
)

// TxnEventEntry describes an entry of a ledger domain event.
type TxnEventEntry struct {
	// AccountID sets the account of the entry.
	AccountID string
	// Direction sets the direction of the entry.
	Direction domain.Direction
	// Amount sets the amount in asset minor units.
	Amount int64
	// Asset sets the account asset code.
	Asset string
	// Decimals sets the number of decimal places.
	Decimals int32
}

// TxnEvent describes a ledger.events domain event for publication through the outbox.
type TxnEvent struct {
	// Type sets the event type: transaction.posted, reserve.created/committed/released.
	Type string
	// TransactionID sets the posting of the event (empty for created/released).
	TransactionID string
	// RefType sets the type of the operation business reference.
	RefType string
	// RefID sets the identifier of the operation business reference.
	RefID string
	// OccurredAt sets the event time.
	OccurredAt time.Time
	// Traceparent sets the W3C trace context for the envelope (if known).
	Traceparent string
	// Key sets the ledger.events partition key.
	Key string
	// Entries sets the entries of the event.
	Entries []TxnEventEntry
}

// TxnEventSink writes an event to the outbox within the business change transaction.
type TxnEventSink interface {
	// AppendTxnEvent appends the event envelope within the given transaction.
	AppendTxnEvent(ctx context.Context, tx *gorm.DB, event TxnEvent) error
}

// WithEventSink enables ledger.events event writing; without a sink no events are written.
func WithEventSink(sink TxnEventSink) Option {
	return func(r *Repository) { r.eventSink = sink }
}

// emitEvent passes the event to the sink if one is configured.
func (r *Repository) emitEvent(ctx context.Context, tx *gorm.DB, event TxnEvent) error {
	if r.eventSink == nil {
		return nil
	}
	if event.Key == "" {
		return fmt.Errorf("storage: empty key for event %s", event.Type)
	}
	if event.Traceparent == "" {
		event.Traceparent = traceparentFromContext(ctx)
	}
	return r.eventSink.AppendTxnEvent(ctx, tx, event)
}

// traceparentKey sets the context key for the W3C traceparent.
type traceparentKey struct{}

// traceparentPattern sets the W3C trace context format; an arbitrary string
// from a Kafka header must not reach envelopes and downstream logs.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// WithTraceparent stores the traceparent in the context: the consumer puts the
// Kafka message header there, and the ledger event inherits the tracing of the
// source. A header invalid in the W3C format is ignored.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if !traceparentPattern.MatchString(traceparent) {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, traceparent)
}

// traceparentFromContext extracts the traceparent from the context.
func traceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentKey{}).(string)
	return value
}
