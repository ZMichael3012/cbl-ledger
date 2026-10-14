// Пакет grpcserver содержит gRPC-транспортный слой сервиса ledger:
// валидацию недоверенного ввода и маппинг доменных ошибок в gRPC-статусы.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/usecase"
)

// Пределы недоверенного ввода транспорта.
const (
	maxIdempotencyKeyLen = 128
	maxRefFieldLen       = 128
	maxReasonLen         = 200
	maxAssetLen          = 32
)

// Ledger реализует gRPC-сервис LedgerService.
type Ledger struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc *usecase.Ledger
}

// NewLedger создает сервер LedgerService.
func NewLedger(svc *usecase.Ledger) *Ledger {
	return &Ledger{svc: svc}
}

// validateIdempotencyKey проверяет ключ идемпотентности недоверенного запроса.
func validateIdempotencyKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return grpcmw.E(grpcmw.KindInvalidArgument, "missing_idempotency_key", "idempotency_key обязателен", nil)
	}
	if len(key) > maxIdempotencyKeyLen {
		return grpcmw.E(grpcmw.KindInvalidArgument, "idempotency_key_too_long", "idempotency_key длиннее предела", nil)
	}
	return nil
}

// validateReserveID проверяет идентификатор резерва недоверенного запроса.
func validateReserveID(reserveID string) error {
	if _, err := uuid.Parse(strings.TrimSpace(reserveID)); err != nil {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_reserve_id", "reserve_id не UUID", nil)
	}
	return nil
}

// PostTransaction выполняет сбалансированную транзакцию двойной записи.
// Повтор с тем же idempotency_key возвращает исходную транзакцию без повторного
// эффекта; повтор с другим телом отклоняется `idempotency_key_reuse`.
func (s *Ledger) PostTransaction(ctx context.Context, req *ledgerv1.PostTransactionRequest) (*ledgerv1.PostTransactionResponse, error) {
	if err := validateIdempotencyKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	if len(req.GetRefType()) > maxRefFieldLen || len(req.GetRefId()) > maxRefFieldLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "ref_too_long", "ref_type или ref_id длиннее предела", nil)
	}
	protoEntries := req.GetEntries()
	if len(protoEntries) > domain.MaxEntries {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "too_many_entries", fmt.Sprintf("проводок больше %d", domain.MaxEntries), nil)
	}
	entries := make([]domain.EntryInput, 0, len(protoEntries))
	for i := range protoEntries {
		if protoEntries[i].GetAmount() == nil {
			return nil, grpcmw.E(grpcmw.KindInvalidArgument, "missing_amount", fmt.Sprintf("проводка %d без суммы", i), nil)
		}
		accountID := strings.TrimSpace(protoEntries[i].GetAccountId())
		if _, err := uuid.Parse(accountID); err != nil {
			return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_account_id", fmt.Sprintf("проводка %d: account_id не UUID", i), nil)
		}
		direction, err := mapDirection(protoEntries[i].GetDirection())
		if err != nil {
			return nil, err
		}
		entries = append(entries, domain.EntryInput{
			AccountID: accountID,
			Direction: direction,
			Amount:    protoEntries[i].GetAmount().GetMinorUnits(),
		})
	}
	result, err := s.svc.PostTransaction(ctx, usecase.PostTransactionRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		RefType:        req.GetRefType(),
		RefID:          req.GetRefId(),
		Entries:        entries,
	})
	if err != nil {
		return nil, mapError(err)
	}
	resp := &ledgerv1.PostTransactionResponse{TransactionId: result.TransactionID}
	for _, balance := range result.Balances {
		money := func(units int64) *commonv1.Money {
			return &commonv1.Money{MinorUnits: units, Asset: balance.Asset, Decimals: decimalsToUint32(balance.Decimals)}
		}
		resp.BalancesAfter = append(resp.BalancesAfter, &ledgerv1.AccountBalance{
			AccountId: balance.AccountID,
			Available: money(balance.Available),
			Reserved:  money(balance.Reserved),
		})
	}
	return resp, nil
}

// Reserve создает удержание средств на счете мерчанта.
// Повтор с тем же idempotency_key возвращает исходный резерв без повторного эффекта.
func (s *Ledger) Reserve(ctx context.Context, req *ledgerv1.ReserveRequest) (*ledgerv1.ReserveResponse, error) {
	if err := validateIdempotencyKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	accountID := strings.TrimSpace(req.GetAccountId())
	if _, err := uuid.Parse(accountID); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_account_id", "account_id не UUID", nil)
	}
	if req.GetAmount() == nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "missing_amount", "amount обязателен", nil)
	}
	if req.GetAmount().GetMinorUnits() <= 0 {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_amount", "amount должен быть положительным", nil)
	}
	if asset := req.GetAmount().GetAsset(); len(asset) == 0 || len(asset) > maxAssetLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_asset", "asset пуст или длиннее предела", nil)
	}
	if refID := req.GetRefId(); strings.TrimSpace(refID) == "" {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "missing_ref_id", "ref_id обязателен", nil)
	} else if len(refID) > maxRefFieldLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "ref_too_long", "ref_id длиннее предела", nil)
	}
	result, err := s.svc.Reserve(ctx, usecase.ReserveRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		AccountID:      accountID,
		Asset:          req.GetAmount().GetAsset(),
		// Приведение uint32 к int32 может дать отрицательное значение при мусорном
		// входе: сверка со счетом отклонит его как asset_mismatch.
		Decimals: int32(req.GetAmount().GetDecimals()), //nolint:gosec // G115: переполнение отсекает сверка decimals счета
		Amount:   req.GetAmount().GetMinorUnits(),
		RefID:    req.GetRefId(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &ledgerv1.ReserveResponse{ReserveId: result.ReserveID, Status: mapReserveStatus(result.Status)}, nil
}

// CommitReserve захватывает удержание: резерв превращается в проводку на счет выплат.
// Повтор в статусе committed идемпотентен, захват released резерва отклоняется.
func (s *Ledger) CommitReserve(ctx context.Context, req *ledgerv1.CommitReserveRequest) (*ledgerv1.CommitReserveResponse, error) {
	if err := validateIdempotencyKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	if err := validateReserveID(req.GetReserveId()); err != nil {
		return nil, err
	}
	result, err := s.svc.CommitReserve(ctx, usecase.CommitReserveRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		ReserveID:      req.GetReserveId(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &ledgerv1.CommitReserveResponse{ReserveId: result.ReserveID, Status: mapReserveStatus(result.Status)}, nil
}

// ReleaseReserve возвращает удержанные средства на доступный баланс счета.
// Повтор в статусе released идемпотентен, возврат committed резерва отклоняется.
func (s *Ledger) ReleaseReserve(ctx context.Context, req *ledgerv1.ReleaseReserveRequest) (*ledgerv1.ReleaseReserveResponse, error) {
	if err := validateIdempotencyKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	if err := validateReserveID(req.GetReserveId()); err != nil {
		return nil, err
	}
	if len(req.GetReason()) > maxReasonLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "reason_too_long", "reason длиннее предела", nil)
	}
	result, err := s.svc.ReleaseReserve(ctx, usecase.ReleaseReserveRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		ReserveID:      req.GetReserveId(),
		Reason:         req.GetReason(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &ledgerv1.ReleaseReserveResponse{ReserveId: result.ReserveID, Status: mapReserveStatus(result.Status)}, nil
}

// mapReserveStatus переводит доменный статус резерва в контрактный.
func mapReserveStatus(status domain.ReserveStatus) ledgerv1.ReserveStatus {
	switch status {
	case domain.ReserveStatusActive:
		return ledgerv1.ReserveStatus_RESERVE_STATUS_ACTIVE
	case domain.ReserveStatusCommitted:
		return ledgerv1.ReserveStatus_RESERVE_STATUS_COMMITTED
	case domain.ReserveStatusReleased:
		return ledgerv1.ReserveStatus_RESERVE_STATUS_RELEASED
	default:
		return ledgerv1.ReserveStatus_RESERVE_STATUS_UNSPECIFIED
	}
}

// decimalsToUint32 безопасно переводит decimals счета в uint32 контракта.
// Отрицательные значения невозможны по CHECK-ограничению БД, но защита нужна линтеру.
func decimalsToUint32(decimals int32) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals)
}

// mapDirection переводит контрактное направление в доменное.
func mapDirection(direction commonv1.Direction) (domain.Direction, error) {
	switch direction {
	case commonv1.Direction_DIRECTION_DEBIT:
		return domain.DirectionDebit, nil
	case commonv1.Direction_DIRECTION_CREDIT:
		return domain.DirectionCredit, nil
	default:
		return "", grpcmw.E(grpcmw.KindInvalidArgument, "unknown_direction", "direction не распознан", nil)
	}
}

// mapError переводит доменные ошибки в ошибки платформы с классом для gRPC-кода.
func mapError(err error) error {
	switch {
	case errors.Is(err, domain.ErrIdempotencyReuse):
		return grpcmw.E(grpcmw.KindAlreadyExists, "idempotency_key_reuse", "ключ идемпотентности уже использован с другим телом", err)
	case errors.Is(err, domain.ErrUnknownAccount):
		return grpcmw.E(grpcmw.KindNotFound, "account_not_found", "счет не найден", err)
	case errors.Is(err, domain.ErrInsufficientFunds):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "insufficient_funds", "недостаточно средств", err)
	case errors.Is(err, domain.ErrUnbalanced):
		return grpcmw.E(grpcmw.KindInvalidArgument, "unbalanced_transaction", "дебеты не равны кредитам", err)
	case errors.Is(err, domain.ErrInvalidEntry), errors.Is(err, domain.ErrInvalidTransaction):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_transaction", "некорректная транзакция", err)
	case errors.Is(err, domain.ErrInvalidReserve):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_reserve", "некорректный резерв", nil)
	case errors.Is(err, domain.ErrUnknownReserve):
		return grpcmw.E(grpcmw.KindNotFound, "reserve_not_found", "резерв не найден", nil)
	case errors.Is(err, domain.ErrReserveState):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "reserve_invalid_state", "резерв в неподходящем состоянии", nil)
	case errors.Is(err, domain.ErrReserveNotAllowed):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "reserve_not_allowed", "счет не поддерживает резервирование", nil)
	case errors.Is(err, domain.ErrAssetMismatch):
		return grpcmw.E(grpcmw.KindInvalidArgument, "asset_mismatch", "активы или decimals счетов различаются", nil)
	case errors.Is(err, domain.ErrConflict):
		return grpcmw.E(grpcmw.KindResourceExhausted, "conflict", "конкурентный конфликт, повторите запрос", err)
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
