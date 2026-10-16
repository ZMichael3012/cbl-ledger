//go:build integration

package usecase

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

func setupPaymentDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("ledger"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

// setupPaymentProcessor assembles the pipeline as in main: the repository with the sink,
// the ledger, the consumer and the inbox processor with the dedup barrier.
func setupPaymentProcessor(t *testing.T, db *gorm.DB) (*inbox.Processor, *storage.Repository) {
	t.Helper()
	repo := storage.NewRepository(db, zap.NewNop(),
		storage.WithEventSink(NewLedgerEventSink(zap.NewNop())))
	ledger := NewLedger(repo, zap.NewNop())
	consumer := NewPaymentConsumer(ledger, zap.NewNop())
	processor := inbox.NewProcessor(ConsumerGroupPayments, db, inbox.NewGuard(), consumer, nil,
		inbox.NewRouter(TopicPaymentEvents, 3), zap.NewNop())
	return processor, repo
}

func paymentMessage(t *testing.T, event *eventsv1.PaymentEvent, traceparent string) inbox.Message {
	t.Helper()
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	headers := map[string]string{outbox.HeaderEventID: uuid.Must(uuid.NewV7()).String()}
	if traceparent != "" {
		headers[outbox.HeaderTraceparent] = traceparent
	}
	return inbox.Message{Topic: TopicPaymentEvents, Key: event.GetPaymentId(), Headers: headers, Value: body}
}

func countRows(t *testing.T, db *gorm.DB, model any) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.Model(model).Count(&count).Error)
	return count
}

func TestIntegrationPaymentCompletedPosting(t *testing.T) {
	db := setupPaymentDB(t)
	processor, repo := setupPaymentProcessor(t, db)
	ctx := context.Background()

	const paymentID = "01a138fd-0000-7000-8000-000000000101"
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	event := completedEvent(paymentID, "m-1", 100_000, &commonv1.Money{MinorUnits: 1_500, Asset: "USDT", Decimals: 2})

	message := paymentMessage(t, event, traceparent)
	require.NoError(t, processor.Process(ctx, message))

	merchant, err := repo.GetBalance(ctx, accountID(t, domain.MerchantAccountKey("m-1", "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(98_500), merchant.Available, "merchant credit net of the fee")

	clearing, err := repo.GetBalance(ctx, accountID(t, domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(-100_000), clearing.Available, "the transit clearing account goes negative")

	fee, err := repo.GetBalance(ctx, accountID(t, domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(1_500), fee.Available)

	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(3), countRows(t, db, &storage.Entry{}))
	assert.Equal(t, int64(3), countRows(t, db, &storage.TransactionBalance{}), "balance snapshots after the posting are written")

	var record outbox.Record
	require.NoError(t, db.Where("topic = ?", TopicLedgerEvents).First(&record).Error)
	assert.Equal(t, storage.EventTransactionPosted, record.Headers[outbox.HeaderEventType])
	assert.Equal(t, ProducerLedger, record.Headers[outbox.HeaderProducer])
	assert.Equal(t, traceparent, record.Headers[outbox.HeaderTraceparent], "the source traceparent is propagated")
	keys := []string{
		accountID(t, domain.MerchantAccountKey("m-1", "USDT", 2)),
		accountID(t, domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2)),
		accountID(t, domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2)),
	}
	assert.Equal(t, minString(keys), record.Key, "the partition key is the minimum account_id")

	var payload eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(record.Payload, &payload))
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED, payload.GetType())
	assert.Equal(t, "payment", payload.GetRefType())
	assert.Equal(t, paymentID, payload.GetRefId())
	require.Len(t, payload.GetEntries(), 3)

	// A duplicate by event_id is suppressed by the inbox barrier.
	require.NoError(t, processor.Process(ctx, message))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "the duplicate does not create a posting")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}))

	// A repeated notification with a new event_id is idempotent by the business key.
	replay := paymentMessage(t, event, "")
	require.NoError(t, processor.Process(ctx, replay))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "replay does not create a second posting")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}), "replay does not publish the event again")
	assert.Equal(t, int64(2), countRows(t, db, &inbox.Record{}))
}

func TestIntegrationPaymentCompletedTemplates(t *testing.T) {
	db := setupPaymentDB(t)
	processor, repo := setupPaymentProcessor(t, db)
	ctx := context.Background()

	// Without a fee: two entries.
	noFee := completedEvent("01a138fd-0000-7000-8000-000000000102", "m-2", 50_000, nil)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, noFee, "")))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(2), countRows(t, db, &storage.Entry{}))
	merchant, err := repo.GetBalance(ctx, accountID(t, domain.MerchantAccountKey("m-2", "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(50_000), merchant.Available)

	// The fee equals the received amount: there is no merchant credit.
	fullFee := completedEvent("01a138fd-0000-7000-8000-000000000103", "m-3", 100,
		&commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2})
	require.NoError(t, processor.Process(ctx, paymentMessage(t, fullFee, "")))
	assert.Equal(t, int64(2), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(4), countRows(t, db, &storage.Entry{}))
	merchant, err = repo.GetBalance(ctx, accountID(t, domain.MerchantAccountKey("m-3", "USDT", 2)))
	require.NoError(t, err)
	assert.Zero(t, merchant.Available)

	// Non-completed types are acknowledged without effect, including the empty enum.
	detected := completedEvent("01a138fd-0000-7000-8000-000000000104", "m-4", 100, nil)
	detected.Type = eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED
	require.NoError(t, processor.Process(ctx, paymentMessage(t, detected, "")))

	unspecified := completedEvent("01a138fd-0000-7000-8000-000000000105", "m-4", 100, nil)
	unspecified.Type = eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED
	require.NoError(t, processor.Process(ctx, paymentMessage(t, unspecified, "")), "the empty enum is skipped")

	unknownEnum := completedEvent("01a138fd-0000-7000-8000-000000000106", "m-4", 100, nil)
	unknownEnum.Type = eventsv1.PaymentEventType(99)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, unknownEnum, "")), "the unknown type is skipped")

	// An invalid traceparent is not propagated into the envelope.
	badTrace := completedEvent("01a138fd-0000-7000-8000-000000000107", "m-5", 100, nil)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, badTrace, "not-a-traceparent")))
	var lastRecord outbox.Record
	require.NoError(t, db.Order("id DESC").First(&lastRecord).Error)
	assert.Empty(t, lastRecord.Headers[outbox.HeaderTraceparent], "an invalid traceparent is ignored")

	// Invalid events are rejected deterministically.
	broken := completedEvent("not-a-uuid", "m-4", 100, nil)
	require.Error(t, processor.Process(ctx, paymentMessage(t, broken, "")))

	corrupt := inbox.Message{
		Topic:   TopicPaymentEvents,
		Key:     "pay-x",
		Headers: map[string]string{outbox.HeaderEventID: uuid.Must(uuid.NewV7()).String()},
		Value:   []byte{0xff, 0xff, 0xff},
	}
	require.Error(t, processor.Process(ctx, corrupt))

	assert.Equal(t, int64(3), countRows(t, db, &storage.Transaction{}), "rejections and skips do not create postings")
}

func TestIntegrationReserveLifecycleEvents(t *testing.T) {
	db := setupPaymentDB(t)
	_, repo := setupPaymentProcessor(t, db)
	ctx := context.Background()

	merchant, err := repo.EnsureAccount(ctx, domain.MerchantAccountKey("m-events", "USDT", 2))
	require.NoError(t, err)
	require.NoError(t, db.Model(&storage.Balance{}).
		Where("account_id = ?", merchant.ID).Update("available", 100_000).Error)

	reserve, err := repo.Reserve(ctx, storage.ReserveInput{
		IdempotencyKey: "payout-events:reserve", AccountID: merchant.ID,
		Asset: "USDT", Decimals: 2, Amount: 40_000, RefID: "payout-events",
	})
	require.NoError(t, err)
	_, err = repo.CommitReserve(ctx, "payout-events:commit", reserve.ReserveID)
	require.NoError(t, err)

	second, err := repo.Reserve(ctx, storage.ReserveInput{
		IdempotencyKey: "payout-events-2:reserve", AccountID: merchant.ID,
		Asset: "USDT", Decimals: 2, Amount: 10_000, RefID: "payout-events-2",
	})
	require.NoError(t, err)
	_, err = repo.ReleaseReserve(ctx, "payout-events-2:release", second.ReserveID, "cancelled")
	require.NoError(t, err)

	var records []outbox.Record
	require.NoError(t, db.Where("topic = ?", TopicLedgerEvents).Order("id").Find(&records).Error)
	require.Len(t, records, 4)

	wantTypes := []string{
		storage.EventReserveCreated,
		storage.EventReserveCommitted,
		storage.EventReserveCreated,
		storage.EventReserveReleased,
	}
	for i, record := range records {
		assert.Equal(t, wantTypes[i], record.Headers[outbox.HeaderEventType])
		assert.Equal(t, merchant.ID, record.Key, "the reserve event key is the hold account")
	}

	var committed eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(records[1].Payload, &committed))
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_COMMITTED, committed.GetType())
	assert.NotEmpty(t, committed.GetTransactionId())
	assert.Len(t, committed.GetEntries(), 2)

	var released eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(records[3].Payload, &released))
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_RELEASED, released.GetType())
	assert.Empty(t, released.GetEntries())
}

func TestIntegrationPaymentConcurrentDelivery(t *testing.T) {
	db := setupPaymentDB(t)
	processor, _ := setupPaymentProcessor(t, db)
	ctx := context.Background()

	const paymentID = "01a138fd-0000-7000-8000-000000000108"
	event := completedEvent(paymentID, "m-concurrent", 10_000, nil)

	// Two different event_ids of one payment and a duplicate of one event_id are processed
	// concurrently: exactly one effect must remain.
	first := paymentMessage(t, event, "")
	second := paymentMessage(t, event, "")
	duplicate := first
	var wg sync.WaitGroup
	errs := make([]error, 4)
	inputs := []inbox.Message{first, second, duplicate, first}
	for i := range inputs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = processor.Process(ctx, inputs[idx])
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "worker %d", i)
	}

	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "exactly one posting")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}), "exactly one event")
	account := accountID(t, domain.MerchantAccountKey("m-concurrent", "USDT", 2))
	assert.Equal(t, int64(10_000), balanceOf(t, db, account))
}

// balanceOf reads the available balance of an account.
func balanceOf(t *testing.T, db *gorm.DB, accountID string) int64 {
	t.Helper()
	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", accountID).First(&balance).Error)
	return balance.Available
}

// accountID returns the deterministic account identifier from the account plan (2.6).
func accountID(t *testing.T, key domain.AccountKey) string {
	t.Helper()
	id, err := domain.DeriveAccountID(key)
	require.NoError(t, err)
	return id.String()
}

// minString returns the minimum string of the set.
func minString(values []string) string {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}
