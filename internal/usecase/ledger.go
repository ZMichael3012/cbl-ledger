// Пакет usecase содержит сценарии использования сервиса ledger:
// оркестрацию доменных правил, транзакций и публикации событий.
package usecase

import (
	"context"

	"go.uber.org/zap"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/storage"
)

// PostTransactionRequest описывает запрос проводки двойной записи.
type PostTransactionRequest struct {
	// IdempotencyKey задает ключ идемпотентности.
	IdempotencyKey string
	// RefType задает тип бизнес-ссылки.
	RefType string
	// RefID задает идентификатор бизнес-ссылки.
	RefID string
	// Entries задает проводки транзакции.
	Entries []domain.EntryInput
}

// Ledger описывает сценарии леджера.
type Ledger struct {
	repo *storage.Repository
	log  *zap.Logger
}

// NewLedger создает Ledger.
func NewLedger(repo *storage.Repository, log *zap.Logger) *Ledger {
	if log == nil {
		log = zap.NewNop()
	}
	return &Ledger{repo: repo, log: log}
}

// PostTransaction выполняет сбалансированную транзакцию двойной записи
// с идемпотентностью по ключу.
func (l *Ledger) PostTransaction(ctx context.Context, req PostTransactionRequest) (storage.PostTransactionResult, error) {
	return l.repo.PostTransaction(ctx, req.IdempotencyKey, req.RefType, req.RefID, req.Entries)
}

// ReserveRequest описывает запрос удержания средств на счете мерчанта.
type ReserveRequest struct {
	// IdempotencyKey задает ключ идемпотентности.
	IdempotencyKey string
	// AccountID задает счет удержания.
	AccountID string
	// Asset задает актив суммы для сверки со счетом.
	Asset string
	// Decimals задает количество знаков после точки для сверки со счетом.
	Decimals int32
	// Amount задает сумму удержания в минимальных единицах актива.
	Amount int64
	// RefID задает бизнес-ссылку удержания.
	RefID string
}

// CommitReserveRequest описывает запрос захвата резерва.
type CommitReserveRequest struct {
	// IdempotencyKey задает ключ идемпотентности.
	IdempotencyKey string
	// ReserveID задает захватываемый резерв.
	ReserveID string
}

// ReleaseReserveRequest описывает запрос возврата резерва.
type ReleaseReserveRequest struct {
	// IdempotencyKey задает ключ идемпотентности.
	IdempotencyKey string
	// ReserveID задает возвращаемый резерв.
	ReserveID string
	// Reason задает причину возврата.
	Reason string
}

// Reserve создает удержание средств на счете мерчанта.
func (l *Ledger) Reserve(ctx context.Context, req ReserveRequest) (storage.ReserveResult, error) {
	return l.repo.Reserve(ctx, storage.ReserveInput{
		IdempotencyKey: req.IdempotencyKey,
		AccountID:      req.AccountID,
		Asset:          req.Asset,
		Decimals:       req.Decimals,
		Amount:         req.Amount,
		RefID:          req.RefID,
	})
}

// CommitReserve превращает удержание в проводку на счет выплат.
func (l *Ledger) CommitReserve(ctx context.Context, req CommitReserveRequest) (storage.ReserveResult, error) {
	return l.repo.CommitReserve(ctx, req.IdempotencyKey, req.ReserveID)
}

// ReleaseReserve возвращает удержанные средства на доступный баланс.
func (l *Ledger) ReleaseReserve(ctx context.Context, req ReleaseReserveRequest) (storage.ReserveResult, error) {
	return l.repo.ReleaseReserve(ctx, req.IdempotencyKey, req.ReserveID, req.Reason)
}
