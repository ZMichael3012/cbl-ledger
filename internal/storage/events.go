package storage

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-ledger/internal/domain"
)

// Типы событий ledger.events (CBL-003, раздел 6).
const (
	// EventTransactionPosted описывает проводку двойной записи.
	EventTransactionPosted = "transaction.posted"
	// EventReserveCreated описывает создание удержания.
	EventReserveCreated = "reserve.created"
	// EventReserveCommitted описывает захват удержания.
	EventReserveCommitted = "reserve.committed"
	// EventReserveReleased описывает возврат удержания.
	EventReserveReleased = "reserve.released"
)

// TxnEventEntry описывает проводку доменного события леджера.
type TxnEventEntry struct {
	// AccountID задает счет проводки.
	AccountID string
	// Direction задает направление проводки.
	Direction domain.Direction
	// Amount задает сумму в минимальных единицах актива.
	Amount int64
	// Asset задает код актива счета.
	Asset string
	// Decimals задает количество знаков после точки.
	Decimals int32
}

// TxnEvent описывает доменное событие ledger.events для публикации через outbox.
type TxnEvent struct {
	// Type задает тип события: transaction.posted, reserve.created/committed/released.
	Type string
	// TransactionID задает проводку события (пусто для created/released).
	TransactionID string
	// RefType задает тип бизнес-ссылки операции.
	RefType string
	// RefID задает идентификатор бизнес-ссылки операции.
	RefID string
	// OccurredAt задает время события.
	OccurredAt time.Time
	// Traceparent задает W3C trace context для конверта (если известен).
	Traceparent string
	// Key задает ключ партиции ledger.events.
	Key string
	// Entries задает проводки события.
	Entries []TxnEventEntry
}

// TxnEventSink записывает событие в outbox в транзакции бизнес-изменения.
type TxnEventSink interface {
	// AppendTxnEvent добавляет конверт события в переданной транзакции.
	AppendTxnEvent(ctx context.Context, tx *gorm.DB, event TxnEvent) error
}

// WithEventSink подключает запись событий ledger.events; без sink события не пишутся.
func WithEventSink(sink TxnEventSink) Option {
	return func(r *Repository) { r.eventSink = sink }
}

// emitEvent передает событие sink-у, если он настроен.
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

// traceparentKey задает ключ контекста для W3C traceparent.
type traceparentKey struct{}

// traceparentPattern задает формат W3C trace context; произвольная строка
// из заголовка Kafka не должна попадать в конверты и логи downstream.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// WithTraceparent сохраняет traceparent в контексте: consumer кладет заголовок
// Kafka-сообщения, и событие леджера наследует трассировку источника.
// Невалидный по формату W3C заголовок игнорируется.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if !traceparentPattern.MatchString(traceparent) {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, traceparent)
}

// traceparentFromContext извлекает traceparent из контекста.
func traceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentKey{}).(string)
	return value
}
