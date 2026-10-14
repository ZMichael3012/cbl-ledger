-- +goose Up
-- Второй барьер денежного инварианта (T-LED-02): на коммите транзакции сумма дебетов
-- обязана совпадать с суммой кредитов, а проводок должно быть не менее двух.
-- CONSTRAINT TRIGGER отложен до коммита: проводки вставляются по одной, и промежуточное
-- состояние транзакции несбалансированно. Барьер срабатывает и при прямой вставке в обход
-- приложения.
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

-- Фиксация search_path исключает подмену таблицы враждебной схемой из пути поиска.
ALTER FUNCTION entries_transaction_balanced() SET search_path = pg_catalog, pg_temp;

CREATE CONSTRAINT TRIGGER entries_transaction_balanced_trigger
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_transaction_balanced();

-- +goose Down
DROP TRIGGER IF EXISTS entries_transaction_balanced_trigger ON entries;
DROP FUNCTION IF EXISTS entries_transaction_balanced();
