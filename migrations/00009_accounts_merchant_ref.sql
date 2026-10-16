-- +goose Up
-- Index for BFF statements (2.10): selection of merchant accounts by merchant_ref and
-- the join filter of the statement. Partial: platform accounts (merchant_ref IS NULL)
-- do not participate in merchant reads.
CREATE INDEX accounts_merchant_ref_idx
    ON accounts (merchant_ref, type, asset)
    WHERE merchant_ref IS NOT NULL;

-- +goose Down
DROP INDEX accounts_merchant_ref_idx;
