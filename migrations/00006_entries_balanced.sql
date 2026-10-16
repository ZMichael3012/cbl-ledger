-- +goose Up
-- The second barrier of the money invariant (T-LED-02): at transaction commit the debit sum
-- must match the credit sum, and there must be at least two entries.
-- The CONSTRAINT TRIGGER is deferred until commit: entries are inserted one by one, and the
-- intermediate transaction state is unbalanced. The barrier also fires on a direct insert
-- bypassing the application.
-- +goose StatementBegin
CREATE FUNCTION entries_transaction_balanced() RETURNS trigger AS $$
DECLARE
    debit_sum   bigint;
    credit_sum  bigint;
    entry_count int;
BEGIN
    SELECT COALESCE(SUM(amount) FILTER (WHERE direction = 'debit'), 0),
           COALESCE(SUM(amount) FILTER (WHERE direction = 'credit'), 0),
           COUNT(*)
      INTO debit_sum, credit_sum, entry_count
      FROM public.entries
     WHERE transaction_id = NEW.transaction_id;
    IF entry_count < 2 THEN
        RAISE EXCEPTION 'transaction % has % entries, at least two required', NEW.transaction_id, entry_count;
    END IF;
    IF debit_sum <> credit_sum THEN
        RAISE EXCEPTION 'transaction % is unbalanced: debit % != credit %', NEW.transaction_id, debit_sum, credit_sum;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- Pinning search_path excludes table substitution by a hostile schema from the search path.
ALTER FUNCTION entries_transaction_balanced() SET search_path = pg_catalog, pg_temp;

CREATE CONSTRAINT TRIGGER entries_transaction_balanced_trigger
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_transaction_balanced();

-- +goose Down
DROP TRIGGER IF EXISTS entries_transaction_balanced_trigger ON entries;
DROP FUNCTION IF EXISTS entries_transaction_balanced();
