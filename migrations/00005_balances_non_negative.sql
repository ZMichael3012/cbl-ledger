-- +goose Up
-- Второй барьер денежного инварианта: баланс счетов merchant и payout
-- не может стать отрицательным даже при ошибке приложения.
-- Транзитные счета (clearing, fee, suspense) могут уходить в минус.
-- +goose StatementBegin
CREATE FUNCTION balances_non_negative() RETURNS trigger AS $$
DECLARE account_type text;
BEGIN
    SELECT type INTO account_type FROM accounts WHERE id = NEW.account_id;
    IF account_type IN ('merchant', 'payout') AND NEW.available < 0 THEN
        RAISE EXCEPTION 'balance of % account cannot be negative', account_type;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER balances_non_negative_trigger
    BEFORE UPDATE ON balances
    FOR EACH ROW EXECUTE FUNCTION balances_non_negative();

-- +goose Down
DROP TRIGGER IF EXISTS balances_non_negative_trigger ON balances;
DROP FUNCTION IF EXISTS balances_non_negative();
