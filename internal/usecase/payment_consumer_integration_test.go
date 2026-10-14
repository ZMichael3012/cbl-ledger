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

// setupPaymentProcessor собирает конвейер как в main: репозиторий с sink,
// ledger, consumer и процессор inbox с dedup-барьером.
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
	assert.Equal(t, int64(98_500), merchant.Available, "кредит мерчанта за вычетом комиссии")

	clearing, err := repo.GetBalance(ctx, accountID(t, domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(-100_000), clearing.Available, "транзитный clearing уходит в минус")

	fee, err := repo.GetBalance(ctx, accountID(t, domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(1_500), fee.Available)

	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(3), countRows(t, db, &storage.Entry{}))
	assert.Equal(t, int64(3), countRows(t, db, &storage.TransactionBalance{}), "снимки после проводки записаны")

	var record outbox.Record
	require.NoError(t, db.Where("topic = ?", TopicLedgerEvents).First(&record).Error)
	assert.Equal(t, storage.EventTransactionPosted, record.Headers[outbox.HeaderEventType])
	assert.Equal(t, ProducerLedger, record.Headers[outbox.HeaderProducer])
	assert.Equal(t, traceparent, record.Headers[outbox.HeaderTraceparent], "traceparent источника пробрасывается")
	keys := []string{
		accountID(t, domain.MerchantAccountKey("m-1", "USDT", 2)),
		accountID(t, domain.PlatformAccountKey(domain.AccountTypeClearing, "USDT", 2)),
		accountID(t, domain.PlatformAccountKey(domain.AccountTypeFee, "USDT", 2)),
	}
	assert.Equal(t, minString(keys), record.Key, "ключ партиции - минимальный account_id")

	var payload eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(record.Payload, &payload))
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED, payload.GetType())
	assert.Equal(t, "payment", payload.GetRefType())
	assert.Equal(t, paymentID, payload.GetRefId())
	require.Len(t, payload.GetEntries(), 3)

	// Дубликат по event_id подавлен inbox-барьером.
	require.NoError(t, processor.Process(ctx, message))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "дубликат не создает проводку")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}))

	// Повтор уведомления с новым event_id идемпотентен по бизнес-ключу.
	replay := paymentMessage(t, event, "")
	require.NoError(t, processor.Process(ctx, replay))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "replay не создает вторую проводку")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}), "replay не публикует событие повторно")
	assert.Equal(t, int64(2), countRows(t, db, &inbox.Record{}))
}

func TestIntegrationPaymentCompletedTemplates(t *testing.T) {
	db := setupPaymentDB(t)
	processor, repo := setupPaymentProcessor(t, db)
	ctx := context.Background()

	// Без комиссии: две проводки.
	noFee := completedEvent("01a138fd-0000-7000-8000-000000000102", "m-2", 50_000, nil)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, noFee, "")))
	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(2), countRows(t, db, &storage.Entry{}))
	merchant, err := repo.GetBalance(ctx, accountID(t, domain.MerchantAccountKey("m-2", "USDT", 2)))
	require.NoError(t, err)
	assert.Equal(t, int64(50_000), merchant.Available)

	// Комиссия равна полученному: мерчантского кредита нет.
	fullFee := completedEvent("01a138fd-0000-7000-8000-000000000103", "m-3", 100,
		&commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2})
	require.NoError(t, processor.Process(ctx, paymentMessage(t, fullFee, "")))
	assert.Equal(t, int64(2), countRows(t, db, &storage.Transaction{}))
	assert.Equal(t, int64(4), countRows(t, db, &storage.Entry{}))
	merchant, err = repo.GetBalance(ctx, accountID(t, domain.MerchantAccountKey("m-3", "USDT", 2)))
	require.NoError(t, err)
	assert.Zero(t, merchant.Available)

	// Незавершенные типы подтверждаются без эффекта, включая пустой enum.
	detected := completedEvent("01a138fd-0000-7000-8000-000000000104", "m-4", 100, nil)
	detected.Type = eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_DETECTED
	require.NoError(t, processor.Process(ctx, paymentMessage(t, detected, "")))

	unspecified := completedEvent("01a138fd-0000-7000-8000-000000000105", "m-4", 100, nil)
	unspecified.Type = eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_UNSPECIFIED
	require.NoError(t, processor.Process(ctx, paymentMessage(t, unspecified, "")), "пустой enum пропускается")

	unknownEnum := completedEvent("01a138fd-0000-7000-8000-000000000106", "m-4", 100, nil)
	unknownEnum.Type = eventsv1.PaymentEventType(99)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, unknownEnum, "")), "неизвестный тип пропускается")

	// Некорректный traceparent не пробрасывается в конверт.
	badTrace := completedEvent("01a138fd-0000-7000-8000-000000000107", "m-5", 100, nil)
	require.NoError(t, processor.Process(ctx, paymentMessage(t, badTrace, "not-a-traceparent")))
	var lastRecord outbox.Record
	require.NoError(t, db.Order("id DESC").First(&lastRecord).Error)
	assert.Empty(t, lastRecord.Headers[outbox.HeaderTraceparent], "невалидный traceparent игнорируется")

	// Некорректные события отклоняются детерминированно.
	broken := completedEvent("not-a-uuid", "m-4", 100, nil)
	require.Error(t, processor.Process(ctx, paymentMessage(t, broken, "")))

	corrupt := inbox.Message{
		Topic:   TopicPaymentEvents,
		Key:     "pay-x",
		Headers: map[string]string{outbox.HeaderEventID: uuid.Must(uuid.NewV7()).String()},
		Value:   []byte{0xff, 0xff, 0xff},
	}
	require.Error(t, processor.Process(ctx, corrupt))

	assert.Equal(t, int64(3), countRows(t, db, &storage.Transaction{}), "отказы и пропуски не создают проводок")
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
		assert.Equal(t, merchant.ID, record.Key, "ключ события резерва - счет удержания")
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

	// Два разных event_id одного платежа и дубликат одного event_id обрабатываются
	// конкурентно: эффект обязан остаться ровно один.
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

	assert.Equal(t, int64(1), countRows(t, db, &storage.Transaction{}), "ровно одна проводка")
	assert.Equal(t, int64(1), countRows(t, db, &outbox.Record{}), "ровно одно событие")
	account := accountID(t, domain.MerchantAccountKey("m-concurrent", "USDT", 2))
	assert.Equal(t, int64(10_000), balanceOf(t, db, account))
}

// balanceOf читает доступный баланс счета.
func balanceOf(t *testing.T, db *gorm.DB, accountID string) int64 {
	t.Helper()
	var balance storage.Balance
	require.NoError(t, db.Where("account_id = ?", accountID).First(&balance).Error)
	return balance.Available
}

// accountID возвращает детерминированный идентификатор счета по плану (2.6).
func accountID(t *testing.T, key domain.AccountKey) string {
	t.Helper()
	id, err := domain.DeriveAccountID(key)
	require.NoError(t, err)
	return id.String()
}

// minString возвращает минимальную строку набора.
func minString(values []string) string {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}
