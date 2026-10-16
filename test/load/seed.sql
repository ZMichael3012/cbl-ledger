-- Account seed for the CBL-2.9 load test (load stand only, not a production migration).
-- 256 pairs: a debit account (with a balance) and a credit account (zero) with deterministic UUIDs
-- that the k6 scenario recomputes (test/load/post_transaction.js).

INSERT INTO accounts (id, type, asset, decimals, merchant_ref)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::uuid,
       'merchant', 'USDT', 2, 'load-debit-' || i
FROM generate_series(0, 255) AS i
ON CONFLICT (id) DO NOTHING;

INSERT INTO accounts (id, type, asset, decimals, merchant_ref)
SELECT ('00000000-0000-4000-a000-' || lpad(to_hex(i), 12, '0'))::uuid,
       'merchant', 'USDT', 2, 'load-credit-' || i
FROM generate_series(0, 255) AS i
ON CONFLICT (id) DO NOTHING;

INSERT INTO balances (account_id, available, reserved, lock_version, updated_at)
SELECT a.id,
       CASE WHEN a.merchant_ref LIKE 'load-debit-%' THEN 1000000000000 ELSE 0 END,
       0, 0, now()
FROM accounts a
WHERE a.merchant_ref LIKE 'load-%'
ON CONFLICT (account_id) DO NOTHING;
