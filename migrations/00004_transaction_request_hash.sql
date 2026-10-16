-- +goose Up
-- Request body hash to control reuse of an idempotency_key
-- with a different body (CBL-003, section 5: ALREADY_EXISTS).
ALTER TABLE transactions ADD COLUMN request_hash text NOT NULL DEFAULT '';
UPDATE transactions SET request_hash = md5(id::text) WHERE request_hash = '';

-- +goose Down
ALTER TABLE transactions DROP COLUMN request_hash;
