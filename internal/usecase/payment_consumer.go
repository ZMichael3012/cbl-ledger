package usecase

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// Payment consumer pipeline constants.
const (
	// TopicPaymentEvents sets the payment events topic.
	TopicPaymentEvents = "payment.events"
	// ConsumerGroupPayments sets the default payment consumer group.
	ConsumerGroupPayments = "cbl-ledger-payments"
	// paymentRefType sets the business reference type for payment entries.
	paymentRefType = "payment"
)

// errInvalidPaymentEvent reports an invalid payment event: such input
// is not retried pointlessly long, but goes through retry topics to the DLQ as designed.
var errInvalidPaymentEvent = errors.New("usecase: invalid payment event")

// PaymentConsumer converts payment.completed into a ledger entry:
// Dr clearing / Cr merchant (net of the fee) / Cr fee.
// It implements inbox.Handler; the business operation is idempotent by the key
// `payment:{payment_id}:completed`, so a Guard transaction is not required (ADR-2.7).
type PaymentConsumer struct {
	ledger *Ledger
	log    *zap.Logger
}

// NewPaymentConsumer creates a PaymentConsumer.
func NewPaymentConsumer(ledger *Ledger, log *zap.Logger) *PaymentConsumer {
	if log == nil {
		log = zap.NewNop()
	}
	return &PaymentConsumer{ledger: ledger, log: log}
}

// completedPosting describes the validated posting parameters of a completed payment.
type completedPosting struct {
	// PaymentID sets the canonical payment UUID (lowercase with hyphens).
	PaymentID string
	// MerchantKey sets the account plan key of the merchant account.
	MerchantKey domain.AccountKey
	// Asset sets the asset of the received amount.
	Asset string
	// Decimals sets the scale of the received amount.
	Decimals int32
	// Received sets the actually received amount.
	Received int64
	// Fee sets the platform fee (may be zero).
	Fee int64
}

// Handle processes a payment event: completed turns into an entry,
// other types are acknowledged without effect (the ledger does not track their states).
func (c *PaymentConsumer) Handle(ctx context.Context, _ *gorm.DB, msg inbox.Message) error {
	var event eventsv1.PaymentEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidPaymentEvent, err)
	}
	switch event.GetType() {
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED:
	case eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED:
		// A populated enum is required: an empty type does not confirm completion,
		// but is not a poison message either - we acknowledge it without effect (FR-1).
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

// buildCompletedPosting validates the untrusted completed payment event
// and builds the posting parameters according to the account plan. The payment identifier
// is canonicalized: the idempotency business key must not depend on the UUID
// notation (case, braces, urn, hyphenless form).
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
	decimals := int32(received.GetDecimals()) //nolint:gosec // G115: the account plan Validate rejects overflow
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
	// The aggregate fee must equal the provider split total when the split is
	// present (cf-4.1, 17.10): the fee drives real money, so an inconsistent
	// split is rejected fail-closed. Without components the fee is accepted
	// as-is (legacy producers).
	splitTotal, hasSplit, err := feeSplitTotal(event, asset, received.GetDecimals())
	if err != nil {
		return completedPosting{}, err
	}
	if hasSplit && splitTotal != fee {
		return completedPosting{}, fmt.Errorf("%w: fee split total %d does not match fee %d", errInvalidPaymentEvent, splitTotal, fee)
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

// feeSplitTotal validates the optional provider fee split (CF-4, 17.10):
// every non-zero component must be non-negative and carry the received asset
// and scale, and the sum must not overflow. A nil or zero component means the
// provider did not report it and is treated as absent; the boolean reports
// whether any non-zero component is present.
func feeSplitTotal(event *eventsv1.PaymentEvent, asset string, decimals uint32) (int64, bool, error) {
	components := []*commonv1.Money{event.GetGatewayFee(), event.GetNetworkFee(), event.GetFxSpread()}
	var total int64
	for i, part := range components {
		if part == nil {
			continue
		}
		if part.GetMinorUnits() < 0 {
			return 0, false, fmt.Errorf("%w: fee split component %d must not be negative", errInvalidPaymentEvent, i)
		}
		if part.GetMinorUnits() == 0 {
			continue
		}
		if part.GetAsset() != asset || part.GetDecimals() != decimals {
			return 0, false, fmt.Errorf("%w: fee split component %d asset or decimals mismatch", errInvalidPaymentEvent, i)
		}
		if part.GetMinorUnits() > math.MaxInt64-total {
			return 0, false, fmt.Errorf("%w: fee split total overflows", errInvalidPaymentEvent)
		}
		total += part.GetMinorUnits()
	}
	if total == 0 {
		return 0, false, nil
	}
	return total, true, nil
}
