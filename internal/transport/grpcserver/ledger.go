// Package grpcserver contains the gRPC transport layer of the ledger service:
// validation of untrusted input and mapping of domain errors to gRPC statuses.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-ledger/internal/domain"
	"github.com/cbl/cbl-ledger/internal/usecase"
)

// Limits of untrusted input at the transport layer.
const (
	maxIdempotencyKeyLen = 128
	maxRefFieldLen       = 128
	maxReasonLen         = 200
	maxAssetLen          = 32
	maxCursorLen         = 64
)

// Ledger implements the gRPC service LedgerService.
type Ledger struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc *usecase.Ledger
}

// NewLedger creates the LedgerService server.
func NewLedger(svc *usecase.Ledger) *Ledger {
	return &Ledger{svc: svc}
}

// validateIdempotencyKey validates the idempotency key of an untrusted request.
func validateIdempotencyKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return grpcmw.E(grpcmw.KindInvalidArgument, "missing_idempotency_key", "idempotency_key обязателен", nil)
	}
	if len(key) > maxIdempotencyKeyLen {
		return grpcmw.E(grpcmw.KindInvalidArgument, "idempotency_key_too_long", "idempotency_key длиннее предела", nil)
	}
	return nil
}

// validateReserveID validates the reserve identifier of an untrusted request.
func validateReserveID(reserveID string) error {
	if _, err := uuid.Parse(strings.TrimSpace(reserveID)); err != nil {
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_reserve_id", "reserve_id не UUID", nil)
	}
	return nil
}

// PostTransaction executes a balanced double-entry transaction.
// A retry with the same idempotency_key returns the original transaction without
// a repeated effect; a retry with a different body is rejected with `idempotency_key_reuse`.
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
		resp.BalancesAfter = append(resp.BalancesAfter, &ledgerv1.AccountBalance{
			AccountId: balance.AccountID,
			Available: moneyValue(balance.Available, balance.Asset, balance.Decimals),
			Reserved:  moneyValue(balance.Reserved, balance.Asset, balance.Decimals),
		})
	}
	return resp, nil
}

// Reserve creates a hold of funds on the merchant account.
// A retry with the same idempotency_key returns the original reserve without a repeated effect.
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
		// Casting uint32 to int32 may yield a negative value on garbage
		// input: the comparison with the account rejects it as asset_mismatch.
		Decimals: int32(req.GetAmount().GetDecimals()), //nolint:gosec // G115: overflow is cut off by the account decimals comparison
		Amount:   req.GetAmount().GetMinorUnits(),
		RefID:    req.GetRefId(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &ledgerv1.ReserveResponse{ReserveId: result.ReserveID, Status: mapReserveStatus(result.Status)}, nil
}

// CommitReserve captures the hold: the reserve turns into an entry to the payout account.
// A retry in the committed status is idempotent, capturing a released reserve is rejected.
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

// ReleaseReserve returns the held funds to the available balance of the account.
// A retry in the released status is idempotent, releasing a committed reserve is rejected.
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

// GetBalance returns the available and reserved balance of the account.
func (s *Ledger) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.GetBalanceResponse, error) {
	accountID := strings.TrimSpace(req.GetAccountId())
	if _, err := uuid.Parse(accountID); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_account_id", "account_id не UUID", nil)
	}
	view, err := s.svc.GetBalance(ctx, accountID)
	if err != nil {
		return nil, mapError(err)
	}
	return &ledgerv1.GetBalanceResponse{Balance: &ledgerv1.AccountBalance{
		AccountId: view.AccountID,
		Available: moneyValue(view.Available, view.Asset, view.Decimals),
		Reserved:  moneyValue(view.Reserved, view.Asset, view.Decimals),
	}}, nil
}

// ListEntries returns a page of the statement by account or merchant (2.10).
// Either account_id or merchant_ref is filled in; merchant_ref limits the statement
// to the merchant accounts, the period filters the transaction created_at (from
// inclusive, to exclusive).
func (s *Ledger) ListEntries(ctx context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	accountID := strings.TrimSpace(req.GetAccountId())
	merchantRef := strings.TrimSpace(req.GetMerchantRef())
	if accountID == "" && merchantRef == "" {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_query", "нужен account_id или merchant_ref", nil)
	}
	if accountID != "" {
		if _, err := uuid.Parse(accountID); err != nil {
			return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_account_id", "account_id не UUID", nil)
		}
	}
	if merchantRef != "" && !domain.ValidMerchantRef(merchantRef) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_query", "merchant_ref не соответствует плану счетов", nil)
	}
	if len(req.GetCursor()) > maxCursorLen {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "cursor_too_long", "cursor длиннее предела", nil)
	}
	periodFrom, err := optionalTimestamp(req.GetPeriodFrom())
	if err != nil {
		return nil, err
	}
	periodTo, err := optionalTimestamp(req.GetPeriodTo())
	if err != nil {
		return nil, err
	}
	if periodFrom != nil && periodTo != nil && !periodTo.After(*periodFrom) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_period", "period_to должен быть позже period_from", nil)
	}
	entries, nextCursor, err := s.svc.ListEntries(ctx, usecase.ListEntriesFilter{
		AccountID:   accountID,
		MerchantRef: merchantRef,
		PeriodFrom:  periodFrom,
		PeriodTo:    periodTo,
		Limit:       req.GetLimit(),
		Cursor:      req.GetCursor(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	response := &ledgerv1.ListEntriesResponse{NextCursor: nextCursor}
	for _, entry := range entries {
		response.Entries = append(response.Entries, &ledgerv1.Entry{
			EntryId:       entry.EntryID,
			TransactionId: entry.TransactionID,
			AccountId:     entry.AccountID,
			Direction:     directionToProto(entry.Direction),
			Amount:        moneyValue(entry.Amount, entry.Asset, entry.Decimals),
			RefType:       entry.RefType,
			RefId:         entry.RefID,
			CreatedAt:     timestamppb.New(entry.CreatedAt),
		})
	}
	return response, nil
}

// ListMerchantAccounts returns the merchant accounts with balances (2.10):
// a prerequisite of 7.4 for balances and account ownership in the BFF.
func (s *Ledger) ListMerchantAccounts(ctx context.Context, req *ledgerv1.ListMerchantAccountsRequest) (*ledgerv1.ListMerchantAccountsResponse, error) {
	merchantRef := strings.TrimSpace(req.GetMerchantRef())
	if merchantRef == "" {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "missing_merchant_ref", "merchant_ref обязателен", nil)
	}
	if !domain.ValidMerchantRef(merchantRef) {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_query", "merchant_ref не соответствует плану счетов", nil)
	}
	accounts, err := s.svc.ListMerchantAccounts(ctx, merchantRef)
	if err != nil {
		return nil, mapError(err)
	}
	response := &ledgerv1.ListMerchantAccountsResponse{}
	for _, account := range accounts {
		response.Accounts = append(response.Accounts, &ledgerv1.MerchantAccount{
			AccountId: account.AccountID,
			Type:      account.Type,
			Asset:     account.Asset,
			Decimals:  decimalsToUint32(account.Decimals),
			Available: moneyValue(account.Available, account.Asset, account.Decimals),
			Reserved:  moneyValue(account.Reserved, account.Asset, account.Decimals),
		})
	}
	return response, nil
}

// optionalTimestamp converts an optional time boundary of the request;
// an invalid timestamp is rejected with the single code invalid_period.
func optionalTimestamp(ts *timestamppb.Timestamp) (*time.Time, error) {
	if ts == nil {
		return nil, nil
	}
	if err := ts.CheckValid(); err != nil {
		return nil, grpcmw.E(grpcmw.KindInvalidArgument, "invalid_period", "границы периода некорректны", nil)
	}
	value := ts.AsTime().UTC()
	return &value, nil
}

// moneyValue builds the contract amount with the asset and the account decimals.
func moneyValue(units int64, asset string, decimals int32) *commonv1.Money {
	return &commonv1.Money{MinorUnits: units, Asset: asset, Decimals: decimalsToUint32(decimals)}
}

// directionToProto converts the domain direction to the contract one.
func directionToProto(direction domain.Direction) commonv1.Direction {
	switch direction {
	case domain.DirectionDebit:
		return commonv1.Direction_DIRECTION_DEBIT
	case domain.DirectionCredit:
		return commonv1.Direction_DIRECTION_CREDIT
	default:
		return commonv1.Direction_DIRECTION_UNSPECIFIED
	}
}

// mapReserveStatus converts the domain reserve status to the contract one.
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

// decimalsToUint32 safely converts the account decimals to the contract uint32.
// Negative values are impossible due to the DB CHECK constraint, but the guard is needed for the linter.
func decimalsToUint32(decimals int32) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals)
}

// mapDirection converts the contract direction to the domain one.
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

// mapError converts domain errors to platform errors with a class for the gRPC code.
func mapError(err error) error {
	switch {
	case errors.Is(err, domain.ErrIdempotencyReuse):
		return grpcmw.E(grpcmw.KindAlreadyExists, "idempotency_key_reuse", "ключ идемпотентности уже использован с другим телом", err)
	case errors.Is(err, domain.ErrUnknownAccount):
		return grpcmw.E(grpcmw.KindNotFound, "account_not_found", "счет не найден", nil)
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
	case errors.Is(err, domain.ErrInvalidCursor):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_cursor", "курсор выписки некорректен", nil)
	case errors.Is(err, domain.ErrInvalidQuery):
		return grpcmw.E(grpcmw.KindInvalidArgument, "invalid_query", "параметры выписки некорректны", nil)
	case errors.Is(err, domain.ErrTooManyResults):
		return grpcmw.E(grpcmw.KindResourceExhausted, "too_many_results", "превышен предел выдачи", nil)
	case errors.Is(err, domain.ErrInvalidAccountPlan):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "invalid_account_plan", "ключ счета не соответствует плану счетов", nil)
	case errors.Is(err, domain.ErrAccountConflict):
		return grpcmw.E(grpcmw.KindFailedPrecondition, "account_conflict", "счет уже существует с другими параметрами", nil)
	case errors.Is(err, domain.ErrConflict):
		return grpcmw.E(grpcmw.KindResourceExhausted, "conflict", "конкурентный конфликт, повторите запрос", err)
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
