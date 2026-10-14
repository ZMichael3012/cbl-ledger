# CBL-2.4. Резервы: Reserve, CommitReserve, ReleaseReserve

| Поле | Значение |
| :-- | :-- |
| Эпик | E2, Ledger |
| Репозиторий | `cbl-ledger` |
| Размер | M |
| Волна и параллельная группа | W1-LEDGER |
| Зависит от | 2.1, CF-1 |
| Версия контрактов | `cbl-protos@cf-1` (`billing/ledger/v1`, `billing/common/v1`) |
| Статус | done, ревью закрыто |

## 1. Контекст и цель

Резервы - денежный примитив саги выплат и возвратов (`CBL-000`, раздел 5: `requested ->
reserved (Ledger) -> sent_to_gateway -> completed/failed`, при `failed` - компенсация) и
задача 10.2. Резерв фиксирует средства мерчанта до подтверждения внешней операции, commit
превращает резерв в проводку (деньги уходят), release возвращает средства при отмене.

## 2. Границы

In scope: `Reserve`, `CommitReserve`, `ReleaseReserve` (gRPC, usecase, storage), таблицы
`reserves` и `reserve_operations`, идемпотентность и конечный автомат резерва, e2e-сценарий
compose (закрывает KI-ledger-03).
Out of scope: запросы `GetBalance`/`ListEntries` (2.5), план счетов и провижининг счетов
(2.6), публикация `ledger.events` (2.7, как и в 2.2), consumer `payment.events` (2.7).

## 3. Функциональные требования

FR-1. Модель резерва (ADR-2.4): резерв - удержание (encumbrance) на балансовом счете
мерчанта; `Reserve` переводит сумму из `available` в `reserved` того же счета и не создает
проводок: удержание не является проводкой двойной записи. `ReleaseReserve` возвращает сумму
из `reserved` в `available` (компенсация, проводок нет). `CommitReserve` захватывает
удержание: `reserved` источника уменьшается, сумма уходит на счет выплат, и создается
сбалансированная проводка в `transactions`/`entries` (дебет источника, кредит счета выплат).

FR-2. `Reserve(idempotency_key, account_id, amount, ref_id)`:
- счет обязан существовать, быть типа `merchant` и иметь непустой `merchant_ref` (иначе
  `reserve_not_allowed`): счет без ссылки на мерчанта не позволяет однозначно выбрать
  получателя коммита, fail-closed против смешивания средств разных мерчантов;
- актив и decimals обязаны совпадать со счетом (`asset_mismatch`);
- сумма строго положительная (`invalid_reserve`), проверки переполнения `reserved`;
- при `available < amount` - отказ `insufficient_funds` без изменений;
- успех: статус `RESERVE_STATUS_ACTIVE`, `reserve_id` (UUIDv7); `ref_id` - бизнес-ссылка
  (например, payout_id).

FR-3. `CommitReserve(idempotency_key, reserve_id)`:
- резерв в статусе `active`: захват. В одной транзакции PostgreSQL: `reserved` источника
  уменьшается на сумму удержания; счет выплат (`type=payout`, актив и `merchant_ref`
  источника) создается идемпотентно при отсутствии, его `available` растет на сумму;
  пишутся строка `transactions` (`ref_type='reserve'`, `ref_id` резерва, ключ и хэш
  коммита) и пара `entries` (Dr источник `amount`, Cr счет выплат `amount`); резерв
  получает `status=committed`, `transaction_id`, `counterparty_account_id`;
- статус `committed` - идемпотентный повтор: возвращается `COMMITTED`, эффекта нет (в том
  числе с другим ключом: состояние однозначно);
- статус `released` - `reserve_invalid_state` (failed_precondition);
- неизвестный `reserve_id` - `reserve_not_found` (not_found).

FR-4. `ReleaseReserve(idempotency_key, reserve_id, reason)`:
- резерв `active`: `reserved` уменьшается, `available` растет на сумму; статус `released`,
  `reason` сохраняется; проводок нет;
- статус `released` - идемпотентный повтор: возвращается `RELEASED` (reason повтора не
  меняет результат и в хэш ключа не входит);
- статус `committed` - `reserve_invalid_state`;
- неизвестный `reserve_id` - `reserve_not_found`.

FR-5. Идемпотентность ключей: ключ `Reserve` уникален в `reserves`, ключи `CommitReserve` и
`ReleaseReserve` - в `reserve_operations` (записываются и при повторе в терминальном
статусе: ключ, использованный на одном резерве, отклоняет ту же операцию над другим
резервом как `idempotency_key_reuse`); тело запроса фиксируется каноническим
`request_hash` (Reserve: account/amount/ref; commit/release: reserve_id, для release reason
исключен). Повтор ключа с тем же телом возвращает результат операции (`Replayed=true` в
слое хранения, для повтора в терминальном статусе - текущий статус резерва); повтор с
другим телом - `idempotency_key_reuse` (already_exists, 409); ключ коммита, уже занятый
проводкой `PostTransaction` в `transactions`, также отклоняется как `idempotency_key_reuse`
(без ретраев). Конкурентная вставка ключа (23505) повторяется согласно политике 2.3.

FR-6. Конкурентность: обновления `balances` только через CAS по `lock_version` с повторами
`RetryPolicy` (2.3); при гонке двух коммитов одного резерва побеждает один (ровно одна
проводка), проигравший после внутреннего повтора получает состояние `committed` из
состояния резерва. Потерянные эффекты невозможны: финализация резерва выполняется
условным UPDATE `WHERE status='active'` в той же транзакции, что балансы и проводки.

FR-7. Маппинг ошибок в gRPC: `insufficient_funds` и `reserve_invalid_state`
(failed_precondition), `reserve_not_allowed` (failed_precondition), `reserve_not_found`
(not_found), `idempotency_key_reuse` (already_exists), `invalid_reserve`, `asset_mismatch`
(invalid_argument), `account_not_found` (not_found), `conflict` (resource_exhausted),
прочее - internal без деталей.

## 4. Контракты

`LedgerService.Reserve/CommitReserve/ReleaseReserve` из `billing/ledger/v1` (CF-1):
`ReserveRequest{idempotency_key, account_id, amount: Money, ref_id}` и
`CommitReserveRequest`/`ReleaseReserveRequest{idempotency_key, reserve_id[, reason]}` ->
`{reserve_id, status}`; `ReserveStatus` enum: ACTIVE/COMMITTED/RELEASED. Лимиты входа
транспорта: ключ и идентификаторы до 128 символов, reason до 200, amount в пределах int64.
Принято с ограничением: в `CommitReserveRequest` нет поля счета-получателя, поэтому
получателем коммита однозначно является счет выплат с тем же `merchant_ref` и активом
(ADR-2.4); при появлении реального провайдера и уточнении контракта получатель выносится в
запрос (CF-2 не затрагивает gRPC).

## 5. Модель данных и миграции

`migrations/00007_reserves.sql`:

```sql
CREATE TABLE reserves (
    id                      uuid PRIMARY KEY,
    account_id              uuid NOT NULL REFERENCES accounts (id),
    amount                  bigint NOT NULL CHECK (amount > 0),
    ref_id                  text NOT NULL,
    status                  text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'committed', 'released')),
    idempotency_key         text NOT NULL UNIQUE,
    request_hash            text NOT NULL,
    reason                  text NOT NULL DEFAULT '',
    transaction_id          uuid REFERENCES transactions (id),
    counterparty_account_id uuid REFERENCES accounts (id),
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'committed' OR transaction_id IS NOT NULL)
);
CREATE INDEX reserves_account_idx ON reserves (account_id, created_at, id);
CREATE INDEX reserves_ref_idx ON reserves (ref_id);
CREATE INDEX reserves_active_idx ON reserves (status) WHERE status = 'active';

CREATE TABLE reserve_operations (
    id              bigserial PRIMARY KEY,
    reserve_id      uuid NOT NULL REFERENCES reserves (id),
    op              text NOT NULL CHECK (op IN ('commit', 'release')),
    idempotency_key text NOT NULL UNIQUE,
    request_hash    text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
```

GORM-модели `Reserve` и `ReserveOperation` повторяют DDL; `AutoMigrate` запрещен. Правка
`CBL-000` (раздел 6): в перечень таблиц леджера добавляются `reserves` и
`reserve_operations`. Статусы резерва экспортируются из `internal/domain`.

## 6. Конечные автоматы

```
(нет) --Reserve--> active --CommitReserve--> committed   (терминальный, есть проводка)
                     |--ReleaseReserve-----> released    (терминальный, без проводки)
```

Переходы только из `active`; повтор операции в терминальном статусе идемпотентен (возврат
текущего статуса), встречная операция отклоняется `reserve_invalid_state`.

## 7. Edge cases

Повтор `Reserve` тем же ключом (та же и другая сумма), конкурентные `Reserve` на один счет
(не перерасходовать), `Commit` после `Release` и наоборот, двойной конкурентный `Commit`
одним и другим ключом, повтор операции в терминальном статусе с новым ключом (ключ
фиксируется, захват другого резерва тем же ключом отклоняется), `Commit`/`Release`
неизвестного резерва, пустой `idempotency_key`, нулевая и отрицательная сумма, счет
`clearing`/`fee`/`suspense`/`payout`, мерчантский счет без `merchant_ref`, чужой актив,
несуществующий счет, переполнение `reserved` и `available`, коллизия ключа коммита с
ключом `PostTransaction`, отключенный счет выплат создается на лету, счет выплат с другой
шкалой decimals отклоняется, повтор `Release` с другим reason.

## 8. Нефункциональные требования

Дедлайн RPC - 1 с (CBL-003); горячие счета используют CAS без глобальных локов (T-LED-05),
число повторов ограничено `RetryPolicy`; `SELECT ... FOR UPDATE` не используется. Покрытие
unit+integration не ниже 80%; денежные сценарии только на реальном PostgreSQL
(testcontainers).

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Smoke | `go build ./...`, compose-стенд: healthz, миграции до версии 7 |
| Unit | `domain`: валидация суммы/ref, переходы статусов; transport: маппинг ошибок и статусов |
| Integration (storage) | удержание и балансы; replay и reuse ключей; отклонения (сумма, тип счета, актив, нехватка); commit (проводка, счет выплат, идемпотентность, состояние); release (возврат, reason, состояние); конкурентные reserve и commit; переполнение |
| Integration (gRPC, bufconn) | сквозной Reserve -> Commit -> Release через сервис с реальным PostgreSQL; коды ошибок |
| E2E (compose) | сценарий: seed счетов, Reserve -> Commit (проверка проводки и балансов в PG) и Reserve -> Release; подписи compose-стенда |
| Load | резервы не входят в профиль 2.9 (PostTransaction); при необходимости расширяется в 14.x |

## 10. План работ и коммит

Коммит `CBL-2.4: резервы Reserve, CommitReserve, ReleaseReserve`; спека входит в коммит.

## 11. Открытые вопросы и решения

- ADR-2.4: получатель коммита - счет выплат с тем же активом и `merchant_ref` (контракт
  CF-1 не передает получателя); счет создается идемпотентно при первом коммите, при
  несовпадении decimals с источником коммит отклоняется (`asset_mismatch`).
- ADR: `Reserve` доступен только мерчантским счетам с непустым `merchant_ref` (fail-closed):
  иначе средства разных мерчантов могли бы смешаться в общем счете выплат.
- ADR: `ReleaseReserve` не создает сторно-проводок, так как удержание не является проводкой;
  трасса операций - в `reserves`/`reserve_operations`; `CBL-000` (раздел 5) и `CBL-002`
  (GE-3) синхронизированы формулировкой "release удержания без проводок"; Reconciler и
  отчетность обязаны учитывать `reserves` (CBL-000, раздел 6).
- ADR: ключ коммита пишется и в `transactions.idempotency_key`; занятый там ключ - конфликт
  тела ключа (`idempotency_key_reuse`, 409), а не конкурентная гонка; ключи Reserve,
  Commit и Release живут в разных пространствах и один ключ может встречаться в каждом
  из них.
- Открытый вопрос: события `reserve.created/committed/released` в `ledger.events` будут
  публиковаться в 2.7 вместе с `transaction.posted` (outbox-запись для всех операций
  леджера централизуется там); backfill уже проведенных операций не предусмотрен.
  (Закрыт в 2.7: события пишутся в outbox в транзакции мутации.)
- Открытый вопрос: провижининг счетов мерчанта (сейчас тесты создают SQL) - задача 2.6
  (закрыт в 2.6: детерминированные счета и `EnsureAccount`).

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (14.10.2026). Блокеров нет; один major (согласование семантики
release с системными спеками) и один major по идемпотентности (ключ повтора в терминальном
статусе) закрыты, minor закрыты или зафиксированы.

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | коллизия ключа коммита с `transactions` дает 429 вместо 409; счет выплат без сверки decimals; `errReserveStateRace` в observer как unknown; пробелы тестов; `make smoke` без сборки | проверка `findTransaction` -> `idempotency_key_reuse`; сверка decimals получателя; cause `reserve_state_race`; добавлены тесты (типы счетов, unknown release, коллизия ключа, привязка ключа повтора, переполнение available, границы oversubscribe); в скрипт добавлен `go build ./...` |
| Архитектуры | major: CBL-000/`CBL-002`/T-05 описывают компенсацию как сторно-проводку, ADR-2.4 - release без проводок; decimals получателя; ключ повтора не пишется в `reserve_operations` (кросс-резервный эффект) | системные спеки синхронизированы: CBL-000 (раздел 5 + примечание о `reserves`), CBL-002 (GE-3), глобальный AGENTS (T-05); сверка decimals; запись ключа в терминальном повторе (`recordReserveOperation`) + тест привязки |
| С контекстом | major: тот же кросс-резервный эффект ключа повтора; дублирование retry-цикла и каркаса commit/release; `retryCause`; пробелы edge-тестов; шаг 7 не выполнен | запись ключа повтора; дублирование оставлено осознанно (T-LED-01, единая политика 2.3, вынос в generic-хелпер при пересмотре повторов в 14.4); cause добавлен; тесты добавлены; AGENTS/спека обновлены этим коммитом |
| Безопасности | major: `merchant_ref IS NULL` уводит коммит на общий счет выплат (fail-open); утечка asset/decimals счета в текст gRPC-ошибки; ключ коммита: 8 холостых повторов и 429; decimals получателя; нет лимита длины `asset` | fail-closed в `Reserve` (непустой `merchant_ref`) + тест; причины ошибок `asset_mismatch`/`reserve_not_allowed`/`invalid_reserve` больше не включаются в ответ; проверка коллизии ключа до захвата локов; сверка decimals; лимит `asset` 32 |
