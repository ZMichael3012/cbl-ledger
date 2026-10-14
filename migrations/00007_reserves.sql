-- +goose Up
-- Резервы (удержания) под сагу выплат и возвратов: Reserve переводит средства
-- из available в reserved, CommitReserve превращает удержание в сбалансированную
-- проводку на счет выплат, ReleaseReserve возвращает средства без проводки.
CREATE TABLE reserves (
    id                      uuid PRIMARY KEY,
    account_id              uuid NOT NULL REFERENCES accounts (id),
    amount                  bigint NOT NULL CHECK (amount > 0),
    ref_id                  text NOT NULL,
    status                  text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'committed', 'released')),
    idempotency_key         text NOT NULL UNIQUE,
    request_hash            text NOT NULL,
    reason                  text NOT NULL DEFAULT '',
    transaction_id          uuid REFERENCES transactions (id),
    counterparty_account_id uuid REFERENCES accounts (id),
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'committed' OR transaction_id IS NOT NULL),
    CHECK (status = 'committed' OR transaction_id IS NULL),
    CHECK (status = 'committed' OR counterparty_account_id IS NULL)
);
CREATE INDEX reserves_account_idx ON reserves (account_id, created_at, id);
CREATE INDEX reserves_ref_idx ON reserves (ref_id);
CREATE INDEX reserves_active_idx ON reserves (status) WHERE status = 'active';

CREATE TABLE reserve_operations (
    id              bigserial PRIMARY KEY,
    reserve_id      uuid NOT NULL REFERENCES reserves (id),
    op              text NOT NULL CHECK (op IN ('commit', 'release')),
    idempotency_key text NOT NULL UNIQUE,
    request_hash    text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS reserve_operations;
DROP TABLE IF EXISTS reserves;
