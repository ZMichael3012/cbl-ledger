-- +goose Up
CREATE TABLE accounts (
    id           uuid PRIMARY KEY,
    type         text NOT NULL CHECK (type IN ('merchant', 'clearing', 'fee', 'payout', 'suspense')),
    asset        text NOT NULL,
    decimals     int  NOT NULL CHECK (decimals BETWEEN 0 AND 18),
    merchant_ref text,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX accounts_type_asset_merchant_idx
    ON accounts (type, asset, COALESCE(merchant_ref, ''));

CREATE TABLE transactions (
    id              uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    ref_type        text NOT NULL,
    ref_id          text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX transactions_ref_idx ON transactions (ref_type, ref_id);

CREATE TABLE entries (
    id             bigserial PRIMARY KEY,
    transaction_id uuid   NOT NULL REFERENCES transactions (id),
    account_id     uuid   NOT NULL REFERENCES accounts (id),
    direction      text   NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount         bigint NOT NULL CHECK (amount > 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX entries_account_idx ON entries (account_id, created_at, id);
CREATE INDEX entries_transaction_idx ON entries (transaction_id);

-- +goose StatementBegin
CREATE FUNCTION entries_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'entries is append-only: % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER entries_append_only_trigger
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_append_only();

CREATE TABLE balances (
    account_id   uuid PRIMARY KEY REFERENCES accounts (id),
    available    bigint NOT NULL DEFAULT 0,
    reserved     bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    lock_version bigint NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS balances;
DROP TRIGGER IF EXISTS entries_append_only_trigger ON entries;
DROP FUNCTION IF EXISTS entries_append_only();
DROP TABLE IF EXISTS entries;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;
