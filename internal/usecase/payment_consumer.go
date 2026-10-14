package usecase

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// Константы consumer-конвейера платежей.
const (
	// TopicPaymentEvents задает топик событий платежей.
	TopicPaymentEvents = "payment.events"
	// ConsumerGroupPayments задает группу консьюмера платежей по умолчанию.
	ConsumerGroupPayments = "cbl-ledger-payments"
	// paymentRefType задает тип бизнес-ссылки проводок по платежам.
	paymentRefType = "payment"
)

// errInvalidPaymentEvent сообщает о некорректном событии платежа: такой вход
// не ретраится бессмысленно долго, а штатно уходит в retry-топики и DLQ.
var errInvalidPaymentEvent = errors.New("usecase: invalid payment event")

// PaymentConsumer превращает payment.completed в проводку леджера:
// Dr clearing / Cr merchant (за вычетом комиссии) / Cr fee.
// Реализует inbox.Handler; бизнес-операция идемпотентна по ключу
// `payment:{payment_id}:completed`, поэтому транзакция Guard не требуется (ADR-2.7).
type PaymentConsumer struct {
	ledger *Ledger
	log    *zap.Logger
}

// NewPaymentConsumer создает PaymentConsumer.
func NewPaymentConsumer(ledger *Ledger, log *zap.Logger) *PaymentConsumer {
	if log == nil {
		log = zap.NewNop()
	}
	return &PaymentConsumer{ledger: ledger, log: log}
}

// completedPosting описывает проверенные параметры проводки завершенного платежа.
type completedPosting struct {
	// PaymentID задает канонический UUID платежа (нижний регистр с дефисами).
	PaymentID string
	// MerchantKey задает ключ мерчантского счета плана счетов.
	MerchantKey domain.AccountKey
	// Asset задает актив полученной суммы.
	Asset string
	// Decimals задает шкалу полученной суммы.
	Decimals int32
	// Received задает фактически полученную сумму.
	Received int64
	// Fee задает комиссию платформы (может быть нулем).
	Fee int64
}

// Handle обрабатывает событие платежа: completed превращается в проводку,
// остальные типы подтверждаются без эффекта (леджер не ведет их состояния).
func (c *PaymentConsumer) Handle(ctx context.Context, _ *gorm.DB, msg inbox.Message) error {
	var event eventsv1.PaymentEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidPaymentEvent, err)
	}
	switch event.GetType() {
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED:
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED:
		// Заполненный enum обязателен: пустой тип не подтверждает завершение,
		// но и не является poison-сообщением - подтверждаем без эффекта (FR-1).
		c.log.Warn("payment event without type skipped", zap.String("payment_id", event.GetPaymentId()))
		return nil
	default:
		c.log.Debug("payment event skipped",
			zap.String("payment_id", event.GetPaymentId()),
			zap.String("type", event.GetType().String()))
		return nil
	}

	posting, err := buildCompletedPosting(&event)
	if err != nil {
		return err
	}
	ctx = storage.WithTraceparent(ctx, msg.Headers[outbox.HeaderTraceparent])

	clearing, err := c.ledger.EnsureAccount(ctx, domain.PlatformAccountKey(domain.AccountTypeClearing, posting.Asset, posting.Decimals))
	if err != nil {
		return fmt.Errorf("usecase: ensure clearing account: %w", err)
	}
	merchant, err := c.ledger.EnsureAccount(ctx, posting.MerchantKey)
	if err != nil {
		return fmt.Errorf("usecase: ensure merchant account: %w", err)
	}

	entries := make([]domain.EntryInput, 0, 3)
	entries = append(entries, domain.EntryInput{
		AccountID: clearing.ID,
		Direction: domain.DirectionDebit,
		Amount:    posting.Received,
	})
	if net := posting.Received - posting.Fee; net > 0 {
		entries = append(entries, domain.EntryInput{
			AccountID: merchant.ID,
			Direction: domain.DirectionCredit,
			Amount:    net,
		})
	}
	if posting.Fee > 0 {
		feeAccount, err := c.ledger.EnsureAccount(ctx, domain.PlatformAccountKey(domain.AccountTypeFee, posting.Asset, posting.Decimals))
		if err != nil {
			return fmt.Errorf("usecase: ensure fee account: %w", err)
		}
		entries = append(entries, domain.EntryInput{
			AccountID: feeAccount.ID,
			Direction: domain.DirectionCredit,
			Amount:    posting.Fee,
		})
	}

	result, err := c.ledger.PostTransaction(ctx, PostTransactionRequest{
		IdempotencyKey: fmt.Sprintf("payment:%s:completed", posting.PaymentID),
		RefType:        paymentRefType,
		RefID:          posting.PaymentID,
		Entries:        entries,
	})
	if err != nil {
		return fmt.Errorf("usecase: post payment transaction: %w", err)
	}
	if result.Replayed {
		c.log.Info("payment completed replayed",
			zap.String("payment_id", posting.PaymentID),
			zap.String("transaction_id", result.TransactionID))
		return nil
	}
	c.log.Info("payment completed posted",
		zap.String("payment_id", posting.PaymentID),
		zap.Int64("received", posting.Received),
		zap.Int64("fee", posting.Fee))
	return nil
}

// buildCompletedPosting проверяет недоверенное событие завершенного платежа
// и собирает параметры проводки по плану счетов. Идентификатор платежа
// канонизируется: бизнес-ключ идемпотентности обязан не зависеть от формы
// записи UUID (регистр, скобки, urn, форма без дефисов).
func buildCompletedPosting(event *eventsv1.PaymentEvent) (completedPosting, error) {
	parsedPaymentID, err := uuid.Parse(event.GetPaymentId())
	if err != nil {
		return completedPosting{}, fmt.Errorf("%w: payment_id %q", errInvalidPaymentEvent, event.GetPaymentId())
	}
	received := event.GetReceivedAmount()
	if received == nil || received.GetMinorUnits() <= 0 {
		return completedPosting{}, fmt.Errorf("%w: received_amount must be positive", errInvalidPaymentEvent)
	}
	asset := received.GetAsset()
	decimals := int32(received.GetDecimals()) //nolint:gosec // G115: переполнение отсеет Validate плана счетов
	merchantKey := domain.MerchantAccountKey(event.GetMerchantId(), asset, decimals)
	if err := merchantKey.Validate(); err != nil {
		return completedPosting{}, fmt.Errorf("%w: merchant account key: %w", errInvalidPaymentEvent, err)
	}
	fee := int64(0)
	if event.GetFee() != nil {
		fee = event.GetFee().GetMinorUnits()
		if fee < 0 {
			return completedPosting{}, fmt.Errorf("%w: fee must not be negative", errInvalidPaymentEvent)
		}
		if event.GetFee().GetAsset() != asset || event.GetFee().GetDecimals() != received.GetDecimals() {
			return completedPosting{}, fmt.Errorf("%w: fee asset or decimals mismatch", errInvalidPaymentEvent)
		}
	}
	if fee > received.GetMinorUnits() {
		return completedPosting{}, fmt.Errorf("%w: fee %d exceeds received %d", errInvalidPaymentEvent, fee, received.GetMinorUnits())
	}
	return completedPosting{
		PaymentID:   parsedPaymentID.String(),
		MerchantKey: merchantKey,
		Asset:       asset,
		Decimals:    decimals,
		Received:    received.GetMinorUnits(),
		Fee:         fee,
	}, nil
}
