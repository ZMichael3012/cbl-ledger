-- +goose Up
-- Balance snapshots after the entry: a repeated PostTransaction by idempotency key
-- returns balances_after as of the first posting, not the current account state (2.5).
-- created_at prepares time-based partitioning (table maintenance - 14.2).
CREATE TABLE transaction_balances (
    transaction_id uuid        NOT NULL REFERENCES transactions (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    available      bigint      NOT NULL,
    reserved       bigint      NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, account_id)
);

-- Statement index: ORDER BY entry_id with an account filter is not served by
-- entries_account_idx (account_id, created_at, id) - the third column gives no ordering.
CREATE INDEX entries_account_id_idx ON entries (account_id, id);

-- +goose Down
DROP INDEX IF EXISTS entries_account_id_idx;
DROP TABLE IF EXISTS transaction_balances;
