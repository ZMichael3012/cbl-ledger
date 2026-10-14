-- +goose Up
-- Снимки балансов после проводки: повтор PostTransaction по ключу идемпотентности
-- возвращает balances_after на момент первой проводки, а не текущее состояние счетов (2.5).
-- created_at готовит партиционирование по времени (обслуживание таблицы - 14.2).
CREATE TABLE transaction_balances (
    transaction_id uuid        NOT NULL REFERENCES transactions (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    available      bigint      NOT NULL,
    reserved       bigint      NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, account_id)
);

-- Индекс выписки: ORDER BY entry_id при фильтре по счету не обслуживается
-- entries_account_idx (account_id, created_at, id) - третий столбец не дает порядок.
CREATE INDEX entries_account_id_idx ON entries (account_id, id);

-- +goose Down
DROP INDEX IF EXISTS entries_account_id_idx;
DROP TABLE IF EXISTS transaction_balances;
