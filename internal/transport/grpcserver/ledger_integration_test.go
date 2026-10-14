//go:build integration

package grpcserver_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-kit/grpcmw"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-ledger/internal/storage"
	"github.com/cbl/cbl-ledger/internal/transport/grpcserver"
	"github.com/cbl/cbl-ledger/internal/usecase"
)

func setupLedgerServer(t *testing.T) (ledgerv1.LedgerServiceClient, *gorm.DB) {
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
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "..", "migrations")))

	repo := storage.NewRepository(db, zap.NewNop())
	svc := usecase.NewLedger(repo, zap.NewNop())

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(zap.NewNop())))
	ledgerv1.RegisterLedgerServiceServer(server, grpcserver.NewLedger(svc))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn), db
}

func seedGrpcAccount(t *testing.T, db *gorm.DB, accountType, asset string, decimals int32, initial int64) storage.Account {
	t.Helper()
	account := storage.Account{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      accountType,
		Asset:     asset,
		Decimals:  decimals,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(&account).Error)
	require.NoError(t, db.Create(&storage.Balance{
		AccountID: account.ID,
		Available: initial,
		UpdatedAt: time.Now().UTC(),
	}).Error)
	return account
}

func entry(accountID string, direction commonv1.Direction, units int64) *ledgerv1.EntryInput {
	return &ledgerv1.EntryInput{
		AccountId: accountID,
		Direction: direction,
		Amount:    &commonv1.Money{MinorUnits: units, Asset: "USDT", Decimals: 2},
	}
}

func TestIntegrationGrpcPostTransaction(t *testing.T) {
	client, db := setupLedgerServer(t)
	merchant := seedGrpcAccount(t, db, "merchant", "USDT", 2, 5_000)
	fee := seedGrpcAccount(t, db, "fee", "USDT", 2, 0)
	ctx := context.Background()

	request := &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "pay-1:completed",
		RefType:        "payment",
		RefId:          "pay-1",
		Entries: []*ledgerv1.EntryInput{
			entry(merchant.ID, commonv1.Direction_DIRECTION_DEBIT, 1_000),
			entry(fee.ID, commonv1.Direction_DIRECTION_CREDIT, 1_000),
		},
	}
	first, err := client.PostTransaction(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, first.GetTransactionId())
	require.Len(t, first.GetBalancesAfter(), 2)

	replay, err := client.PostTransaction(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, first.GetTransactionId(), replay.GetTransactionId())

	mismatch := &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "pay-1:completed",
		RefType:        "payment",
		RefId:          "pay-1",
		Entries: []*ledgerv1.EntryInput{
			entry(merchant.ID, commonv1.Direction_DIRECTION_DEBIT, 2_000),
			entry(fee.ID, commonv1.Direction_DIRECTION_CREDIT, 2_000),
		},
	}
	_, err = client.PostTransaction(ctx, mismatch)
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "pay-2:completed",
		Entries: []*ledgerv1.EntryInput{
			entry(merchant.ID, commonv1.Direction_DIRECTION_DEBIT, 100_000),
			entry(fee.ID, commonv1.Direction_DIRECTION_CREDIT, 100_000),
		},
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	upper := &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "pay-3:completed",
		Entries: []*ledgerv1.EntryInput{
			entry(strings.ToUpper(merchant.ID), commonv1.Direction_DIRECTION_DEBIT, 100),
			entry(fee.ID, commonv1.Direction_DIRECTION_CREDIT, 100),
		},
	}
	_, err = client.PostTransaction(ctx, upper)
	require.NoError(t, err)
}

func TestIntegrationGrpcClearingMayGoNegative(t *testing.T) {
	client, db := setupLedgerServer(t)
	clearing := seedGrpcAccount(t, db, "clearing", "USDT", 2, 0)
	merchant := seedGrpcAccount(t, db, "merchant", "USDT", 2, 0)
	ctx := context.Background()

	response, err := client.PostTransaction(ctx, &ledgerv1.PostTransactionRequest{
		IdempotencyKey: "pay-4:completed",
		Entries: []*ledgerv1.EntryInput{
			entry(clearing.ID, commonv1.Direction_DIRECTION_DEBIT, 500),
			entry(merchant.ID, commonv1.Direction_DIRECTION_CREDIT, 500),
		},
	})
	require.NoError(t, err)
	require.Len(t, response.GetBalancesAfter(), 2)
	for _, balance := range response.GetBalancesAfter() {
		if balance.GetAccountId() == clearing.ID {
			assert.Equal(t, int64(-500), balance.GetAvailable().GetMinorUnits())
		}
	}
}
