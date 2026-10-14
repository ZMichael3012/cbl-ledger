// main запускает HTTP- и gRPC-сервис ledger платформы CBL.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-kit/ginmw"
	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-ledger/internal/storage"
	"github.com/cbl/cbl-ledger/internal/transport/grpcserver"
	"github.com/cbl/cbl-ledger/internal/usecase"
)

// main поднимает gRPC-сервер LedgerService, административный HTTP-сервер,
// применяет миграции и завершает работу по SIGINT/SIGTERM.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		log.Error("PG_DSN is required")
		return
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Error("connect postgres failed", zap.Error(err))
		return
	}

	// Пул ограничен: без потолка всплеск нагрузки исчерпывает max_connections PostgreSQL
	// (обнаружено нагрузочным тестом 2.9: ошибки under load при 500 rps).
	// Потолок конфигурации ограничен сверху, чтобы опечатка в env не исчерпала БД.
	sqlDB, err := db.DB()
	if err != nil {
		log.Error("resolve sql db failed", zap.Error(err))
		return
	}
	maxOpen := min(getenvInt("DB_MAX_OPEN_CONNS", 20), 100)
	maxIdle := min(getenvInt("DB_MAX_IDLE_CONNS", 10), maxOpen)
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	log.Info("postgres pool configured", zap.Int("max_open", maxOpen), zap.Int("max_idle", maxIdle))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if err := storagex.Migrate(ctx, db, migrationsDir); err != nil {
		log.Error("apply migrations failed", zap.String("dir", migrationsDir), zap.Error(err))
		return
	}

	repo := storage.NewRepository(db, log,
		storage.WithRetryObserver(func(event storage.RetryEvent) {
			log.Warn("ledger retry",
				zap.Int("attempt", event.Attempt),
				zap.String("cause", event.Cause),
				zap.Duration("delay", event.Delay))
		}),
		storage.WithEventSink(usecase.NewLedgerEventSink(log)))
	ledgerSvc := usecase.NewLedger(repo, log)

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		brokerList := splitCSV(brokers)
		publisher, err := kafkapub.New(brokerList)
		if err != nil {
			log.Error("kafka publisher failed", zap.Error(err))
			return
		}
		defer publisher.Close()

		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(runErr))
			}
		}()

		group := getenv("KAFKA_CONSUMER_GROUP", usecase.ConsumerGroupPayments)
		consumer := usecase.NewPaymentConsumer(ledgerSvc, log)
		processor := inbox.NewProcessor(group, db, inbox.NewGuard(), consumer, publisher,
			inbox.NewRouter(usecase.TopicPaymentEvents, 3), log)
		topics := []string{
			usecase.TopicPaymentEvents,
			usecase.TopicPaymentEvents + inbox.RetrySuffix5s,
			usecase.TopicPaymentEvents + inbox.RetrySuffix1m,
			usecase.TopicPaymentEvents + inbox.RetrySuffix10m,
		}
		runner, runErr := kafkaconsumer.New(brokerList, group, topics, processor, log)
		if runErr != nil {
			log.Error("payment consumer failed", zap.Error(runErr))
			return
		}
		defer runner.Close()
		go func() {
			if consumeErr := runner.Run(ctx); consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
				log.Error("payment consumer stopped", zap.Error(consumeErr))
			}
		}()
		log.Info("payment consumer started", zap.String("group", group))
	} else {
		log.Warn("KAFKA_BROKERS is empty: payment consumer disabled")
	}

	httpAddr := getenv("HTTP_ADDR", ":8040")
	grpcAddr := getenv("GRPC_ADDR", ":9040")
	metricsAddr := getenv("METRICS_ADDR", ":9240")

	router := gin.New()
	router.Use(ginmw.RequestID())
	router.Use(ginmw.AccessLog(log))
	router.Use(ginmw.Recovery(log))
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	httpSrv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<20),
		grpc.UnaryInterceptor(grpcmw.UnaryServer(log)),
	)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, grpcserver.NewLedger(ledgerSvc))
	reflection.Register(grpcSrv)

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
		return
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	log.Info("shutdown finished")
}

// metricsHandler отдает заглушку метрик до задачи 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// getenv возвращает значение переменной окружения или значение по умолчанию.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvInt возвращает положительное целое из переменной окружения или значение по умолчанию.
func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// splitCSV разбирает список значений из переменной окружения через запятую,
// отбрасывая пустые элементы и пробелы вокруг них.
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}
