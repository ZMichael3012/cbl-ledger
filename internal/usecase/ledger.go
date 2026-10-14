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
