# CBL-2.1. Схема БД леджера

| Поле | Значение |
| :-- | :-- |
| Эпик | E2, Ledger |
| Репозиторий | `cbl-ledger` |
| Размер | M |
| Волна и параллельная группа | W1-LEDGER |
| Зависит от | 1.2, 1.3 (пакеты cbl-kit), CF-1 |
| Версия контрактов | `cbl-protos@cf-1` (`billing/ledger/v1`, `billing/events/v1`) |
| Статус | done (локально, ожидает коммита) |

## 1. Контекст и цель

Схема леджера - фундамент задач 2.2 - 2.9 и всех денежных потоков платформы (`CBL-000`,
инварианты 2 - 5). Двойная запись, append-only проводки, оптимистичные блокировки балансов.

## 2. Границы

In scope: goose-миграции, GORM-модели, константы направлений и типов счетов, reference-копии
outbox/inbox из `cbl-kit`.
Out of scope: логика `PostTransaction` (2.2), резервы (2.4), consumer платежей (2.7).

## 3. Функциональные требования

FR-1. Миграции только вперед, goose-формат (`-- +goose Up` / `-- +goose Down`):

| Файл | Содержимое |
| :-- | :-- |
| `migrations/00001_ledger_init.sql` | `accounts`, `transactions`, `entries`, `balances`, триггер append-only |
| `migrations/00002_outbox.sql` | копия `cbl-kit/migrations/outbox.sql` |
| `migrations/00003_inbox.sql` | копия `cbl-kit/migrations/inbox.sql` |
| `migrations/00004_transaction_request_hash.sql` | колонка `request_hash` для контроля повтора ключа |
| `migrations/00005_balances_non_negative.sql` | триггер запрета отрицательного `available` для merchant и payout |

FR-2. `accounts`: `id uuid PRIMARY KEY`, `type text NOT NULL` (merchant, clearing, fee, payout,
suspense), `asset text NOT NULL`, `decimals int NOT NULL CHECK (decimals BETWEEN 0 AND 18)`,
`merchant_ref text NULL`, `created_at timestamptz NOT NULL DEFAULT now()`; уникальный индекс
`(type, asset, COALESCE(merchant_ref, ''))`: счета без merchant_ref тоже уникальны в рамках
типа и актива.

FR-3. `transactions`: `id uuid PRIMARY KEY`, `idempotency_key text NOT NULL UNIQUE`,
`ref_type text NOT NULL`, `ref_id text NOT NULL`, `request_hash text NOT NULL` (канонический
хэш тела, миграция 00004), `created_at timestamptz NOT NULL DEFAULT now()`.

FR-4. `entries` (append-only, без UPDATE и DELETE):
`id bigserial PRIMARY KEY`, `transaction_id uuid NOT NULL REFERENCES transactions(id)`,
`account_id uuid NOT NULL REFERENCES accounts(id)`, `direction text NOT NULL CHECK (direction IN
('debit','credit'))`, `amount bigint NOT NULL CHECK (amount > 0)`, `created_at timestamptz NOT
NULL DEFAULT now()`; индексы `(account_id, created_at, id)` и `(transaction_id)`.
Триггер `entries_append_only` поднимает исключение на UPDATE и DELETE.

FR-5. `balances`: `account_id uuid PRIMARY KEY REFERENCES accounts(id)`,
`available bigint NOT NULL DEFAULT 0`, `reserved bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0)`,
`lock_version bigint NOT NULL DEFAULT 0`, `updated_at timestamptz NOT NULL DEFAULT now()`;
обновления только через оптимистичную блокировку по `lock_version` (2.3). Отрицательный
`available` запрещен триггером `balances_non_negative` для счетов merchant и payout
(миграция 00005); транзитные счета (clearing, fee, suspense) могут уходить в минус (T-LED-03).

FR-6. GORM-модели повторяют DDL; `AutoMigrate` запрещен. Денежные поля только int64.
Доменные константы типов счетов и направлений экспортируются из `internal/domain`.

FR-7. Локальные зависимости: `go.mod` сервиса подключает `github.com/cbl/cbl-kit` и
`github.com/cbl/cbl-protos` через временные `replace`-директивы на сопоставленные каталоги
`../cbl-kit` и `../cbl-protos`; replace снимается после публикации модулей в CI (0.2).

## 4. Контракты

Таблица соответствия GORM-моделей контракту `billing/ledger/v1`:
`EntryInput/Entry` <- `entries`, `AccountBalance` <- `balances`, `Direction` enum <- колонка
`direction`. События `ledger.events` строятся из `transactions` и `entries` (задачи 2.2, 2.7).

## 5. Модель данных и миграции

См. FR-2 - FR-5. Все суммы в минимальных единицах актива, asset и decimals берутся из account.

## 6. Конечные автоматы

Не применимо; состояния платежей и выплат живут в своих сервисах.

## 7. Edge cases

Проверка сбалансированности на уровне приложения (2.2), запрет UPDATE/DELETE через триггер,
конкурентные апдейты баланса (2.3), пустые entries, отрицательные суммы, несуществующий account.

## 8. Нефункциональные требования

Применение миграций идемпотентно повторно (goose versioning). Индексы под горячие запросы
баланса и выписки. Проверка схемы на реальном PostgreSQL.

## 9. План тестов

| Уровень | Сценарии |
| :-- | :-- |
| Smoke | `go build ./...`, миграции применяются на чистой базе |
| Unit | константы и валидаторы моделей |
| Integration | testcontainers PostgreSQL: миграции вверх; триггер блокирует UPDATE/DELETE entries; CHECK reserved >= 0; UNIQUE idempotency_key; FK на несуществующий account |
| E2E | локальный compose леджера: health + применение миграций |
| Load | не применимо (2.9) |

## 10. План работ и коммит

Коммит `CBL-2.1: схема леджера с append-only entries и балансами`; спека в том же коммите.

## 11. Открытые вопросы

- Партиционирование `entries` по времени - отложено до бенчмарка 2.9.

## 12. Ревью (CBL-001, шаг 6)

Четыре независимых прохода (13 - 14.10.2026): без контекста, архитектуры, с контекстом,
безопасности (блокирующий).

| Роль | Ключевые замечания | Закрытие |
| :-- | :-- | :-- |
| Без контекста | уникальный индекс `accounts` строже FR-2; не покрыты `CHECK reserved >= 0` и FK-ограничения | FR-2 уточнена (COALESCE), тесты `reserved`/FK добавлены |
| Архитектуры | нет второго барьера БД для балансов; контроль суммы проводок только в коде | миграция 00005 (триггер non-negative для merchant/payout); constraint trigger суммы - кандидат в 2.3 (T-LED-02) |
| С контекстом | дублирование helper-обвязки между сервисами | `storagex.Migrate` вынесен в cbl-kit (T-KK6) |
| Безопасности | отрицательный `available` не запрещен на уровне БД | закрыто миграцией 00005 и тестом `TestIntegrationBalancesNonNegativeTrigger` |

