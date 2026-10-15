-- +goose Up
-- Индекс для BFF-выписок (2.10): выборка счетов мерчанта по merchant_ref и
-- join-фильтр выписки. Частичный: платформенные счета (merchant_ref IS NULL)
-- в мерчантских чтениях не участвуют.
CREATE INDEX accounts_merchant_ref_idx
    ON accounts (merchant_ref, type, asset)
    WHERE merchant_ref IS NOT NULL;

-- +goose Down
DROP INDEX accounts_merchant_ref_idx;
