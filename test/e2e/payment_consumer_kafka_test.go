//go:build e2e

// Пакет e2e содержит сквозной сценарий consumer леджера: событие
// payment.completed из Kafka превращается в проводку в PostgreSQL, а конверт
// ledger.events публикуется обратно в Kafka. Стенд: реальные Kafka и PostgreSQL.
package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
	"github.com/cbl/cbl-ledger/internal/usecase"
)

// Топики стенда: вход consumer и выход продюсера.
const (
	paymentTopic = usecase.TopicPaymentEvents
	ledgerTopic  = usecase.TopicLedgerEvents
)

func setupE2EPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("ledger_e2e"),
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

func setupE2EKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-ledger-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	topics := []string{
		paymentTopic, paymentTopic + inbox.RetrySuffix5s, paymentTopic + inbox.RetrySuffix1m,
		paymentTopic + inbox.RetrySuffix10m, paymentTopic + inbox.DLQSuffix, ledgerTopic,
	}
	for _, topic := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, topic)
		require.NoError(t, err, topic)
	}
	return brokers
}

func TestPaymentConsumerKafkaFlow(t *testing.T) {
	db := setupE2EPostgres(t)
	brokers := setupE2EKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	repo := storage.NewRepository(db, zap.NewNop(),
		storage.WithEventSink(usecase.NewLedgerEventSink(zap.NewNop())))
	ledger := usecase.NewLedger(repo, zap.NewNop())

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	consumer := usecase.NewPaymentConsumer(ledger, zap.NewNop())
	processor := inbox.NewProcessor(usecase.ConsumerGroupPayments, db, inbox.NewGuard(), consumer, publisher,
		inbox.NewRouter(paymentTopic, 3), zap.NewNop())
	topics := []string{
		paymentTopic, paymentTopic + inbox.RetrySuffix5s,
		paymentTopic + inbox.RetrySuffix1m, paymentTopic + inbox.RetrySuffix10m,
	}
	runner, err := kafkaconsumer.New(brokers, usecase.ConsumerGroupPayments, topics, processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(ledgerTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumerClient.Close()

	const paymentID = "01a138fd-0000-7000-8000-000000000201"
	event := &eventsv1.PaymentEvent{
		PaymentId:      paymentID,
		MerchantId:     "m-kafka",
		Type:           eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		ReceivedAmount: &commonv1.Money{MinorUnits: 100_000, Asset: "USDT", Decimals: 2},
		Fee:            &commonv1.Money{MinorUnits: 2_000, Asset: "USDT", Decimals: 2},
	}
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	eventID := uuid.Must(uuid.NewV7()).String()
	record := &kgo.Record{
		Topic: paymentTopic,
		Key:   []byte(paymentID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(eventID)},
			{Key: outbox.HeaderEventType, Value: []byte("payment.completed")},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-payment")},
		},
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()
	require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())

	// Ждем конверт ledger.events в Kafka.
	envelope := pollEnvelope(t, ctx, consumerClient, ledgerTopic)
	assert.Equal(t, storage.EventTransactionPosted, headerValue(envelope, outbox.HeaderEventType))
	assert.Equal(t, usecase.ProducerLedger, headerValue(envelope, outbox.HeaderProducer))
	var payload eventsv1.LedgerEvent
	require.NoError(t, proto.Unmarshal(envelope.Value, &payload))
	assert.Equal(t, eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED, payload.GetType())
	assert.Equal(t, paymentID, payload.GetRefId())
	require.Len(t, payload.GetEntries(), 3)

	merchantID, err := domain.DeriveAccountID(domain.MerchantAccountKey("m-kafka", "USDT", 2))
	require.NoError(t, err)
	balance, err := repo.GetBalance(ctx, merchantID.String())
	require.NoError(t, err)
	assert.Equal(t, int64(98_000), balance.Available)

	// Дубликат события не создает вторую проводку.
	require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())
	time.Sleep(2 * time.Second)
	var transactions int64
	require.NoError(t, db.Model(&storage.Transaction{}).Count(&transactions).Error)
	assert.Equal(t, int64(1), transactions, "inbox и бизнес-ключ защищают от дублей")

	var outboxRows int64
	require.NoError(t, db.Model(&outbox.Record{}).Count(&outboxRows).Error)
	assert.Equal(t, int64(1), outboxRows)
}

// headerValue возвращает значение заголовка Kafka-записи.
func headerValue(record *kgo.Record, key string) string {
	for _, header := range record.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

// pollEnvelope ждет первое сообщение топика или падает по таймауту контекста.
func pollEnvelope(t *testing.T, ctx context.Context, client *kgo.Client, topic string) *kgo.Record {
	t.Helper()
	var found *kgo.Record
	for found == nil {
		select {
		case <-ctx.Done():
			require.FailNow(t, "ledger.events envelope not received", topic)
		default:
		}
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := client.PollFetches(pollCtx)
		cancel()
		require.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Topic == topic && found == nil {
				found = r
			}
		})
	}
	return found
}
