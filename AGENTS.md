# AGENTS.md - cbl-ledger

Двойная запись, резервы и балансы: источник истины о деньгах.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: PostTransaction, резервы, балансы, выписки, идемпотентные проводки.

Out of scope: другие сервисы платформы; взаимодействие только через контракты `cbl-protos`
(gRPC), Kafka и REST периметра (см. `CBL-000`, раздел 2).

## 2. Контракты

- REST: административный /healthz, /readyz
- gRPC: сервер LedgerService (PostTransaction, Reserve, CommitReserve, ReleaseReserve,
  GetBalance, ListEntries)
- Kafka продюсирует: ledger.events
- Kafka потребляет: payment.events
- Хранилище: PostgreSQL ledger (accounts, transactions, entries, balances, reserves,
  reserve_operations, transaction_balances, outbox, inbox)

Контракты заморожены в `cbl-protos` (CF-1 для gRPC, CF-2/CF-3 для событий), см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers)
make test-e2e     # сквозной сценарий резервов против поднятого compose-стенда (CBL-2.4)
make smoke        # compose-стенд: сборка, миграции до версии 8, e2e-сценарии резервов и выписки
make bench        # бенчмарки записи на testcontainers (CBL-2.9)
make vet          # go vet ./...
make lint         # golangci-lint в Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

Нагрузочный прогон k6 (compose-стенд): сид и команды - в шапке
`test/load/post_transaction.js`, сид-данные - `test/load/seed.sql` (только стенд нагрузки).

## 5. Порты и окружение

HTTP `8040`, gRPC `9040`, метрики `9240`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`, `DB_MAX_OPEN_CONNS` (по умолчанию 20),
`DB_MAX_IDLE_CONNS` (по умолчанию 10).

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> архитектура -> контракты -> тесты вперед -> код ->
  ревью 4 агентами (без контекста, архитектура, контекст, безопасность - блокирующий) ->
  AGENTS.md -> финальные прогоны -> линтеры -> коммит.
- Покрытие unit+integration не ниже 80%; денежные сценарии только на реальных PostgreSQL и
  Kafka (testcontainers); локальный e2e в docker-compose обязателен.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен.
- Docstring на русском в стиле godoc (первая строка с имени сущности, точка в конце);
  комментарии объясняют почему; ошибки через `%w`; панки в бизнес-коде запрещены.
- Коммит: `CBL-<эпик>.<номер>: <краткое сообщение>`, спека с тем же префиксом в том же коммите.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Базовые метрики: latency p95/p99, доля ошибок, outbox lag,
consumer lag; дашборды после задачи 0.7.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-LED-01 | Валидация, идемпотентность и оптимистичные повторы живут в `storage.Repository` | транзакционный контекст и атомарность в одном слое | usecase тонкий, границы слоев размыты | с 2.3 политика повторов вынесена в `RetryPolicy` и `RetryObserver`; бенчмарк 2.9: distributed 2070 записей/с, цель 500 TPS достигнута, решения по горячим счетам - раздел 3.2 спеки 2.9 |
| T-LED-02 | Сумма дебетов и кредитов проверяется приложением и барьером БД (деферред constraint trigger 00006) | гибкость приложения плюс второй барьер на коммите | ограничения барьера: прямым SQL в обход приложения возможно дописывание сбалансированной пары к чужой транзакции и смешение активов (вне модели угроз; балансы не меняются) | A/B-замер 2.9: +10.9 мс на 100 входов (около 11%); на типовых 2 - 4 входах в пределах шума |
| T-LED-03 | Отрицательный `available` разрешен для clearing, fee и suspense | поддержка потока Dr clearing / Cr merchant без префандинга | ошибка в политике возможна у транзитных счетов | контроль по типам счетов, мониторинг suspense в 2.6 |
| T-LED-04 | Пул соединений PostgreSQL ограничен (`DB_MAX_OPEN_CONNS=20`, потолок конфигурации 100) | защита `max_connections` от всплесков, предсказуемая деградация | при насыщении запросы ждут соединение | найдено нагрузкой 2.9 (ошибки Internal при 500 rps); значения тюнируются через env, запас 4x по замеру 2070 записей/с |
| T-LED-05 | Горячий счет остается на оптимистичном CAS до порога 200 записей/с | простая модель, нет блокировок строк на общих путях | при 24 писателях в один счет пропускная способность падает (168 записей/с, 14% отказов) | план защиты по порогу: сокращение операторов, батчевый коммиттер, шардирование счета (2.9, раздел 3.2) |
| T-LED-06 | Резерв - удержание: Reserve/Release меняют только available/reserved без проводок, Commit пишет сбалансированную пару Dr источник / Cr счет выплат | удержание не создает фиктивного движения в журнале, компенсация дешева (release) | балансы не выводятся из одних entries, нужен учет reserves (CBL-000, раздел 6); получатель коммита не в контракте CF-1 | получатель - счет выплат того же merchant_ref и актива (ADR-2.4, спека 2.4); события reserve.* - в 2.7; fail-closed для счетов мерчанта без merchant_ref |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-ledger-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-ledger-02 | Нет mTLS и authN на gRPC LedgerService: запись, резервы, чтение балансов и полной выписки (с ref_id) любого счета | порт 9040 не публиковать за пределы доверенной сети; prod-выкладка запрещена | mTLS и авторизация до prod (0.6, 1.4); неймспейсинг ключей идемпотентности по мерчанту - вместе с authN (оракул глобального ключа) | open |
| KI-ledger-03 | Сценарии e2e в `test/e2e/` отсутствуют | шаг 8 выполнен частично: integration, smoke compose и k6-нагрузка (2.9, `test/load/`) есть; сценариев e2e нет | ЗАКРЫТ 14.10.2026: e2e compose-сценарий резервов `test/e2e/reserve_flow.ps1` + `make smoke` (Reserve/Commit/Release через реальный gRPC и PostgreSQL) | closed |
| KI-ledger-04 | Race detector недоступен локально (нет CGO на Windows) | конкурентные тесты идут без -race | включить -race в CI (0.2) | open |
| KI-ledger-05 | Функция `balances_non_negative()` (миграция 00005) не фиксирует `search_path` | низкое: подмена требует прав CREATE в схеме раньше public; 00006 захардена (`public.entries`, search_path) | применить hardening отдельной миграцией при плановом изменении 00005 | open |
| KI-ledger-06 | Нет серверного deadline-интерцептора и ограничения очереди RPC: при насыщении пула запрос без клиентского дедлайна может ждать долго | availability-риск вне денежного контура; нагрузка 2.9 деградирует ростом латентности, не ошибками | timeout-интерцептор и MaxConcurrentStreams до prod (0.6/14.4); prod запрещен KI-ledger-02 | open |
| KI-ledger-07 | `transaction_balances` растет примерно 1:1 к `entries` (строка на затронутый счет), ретеншен и партиционирование не заданы | рост БД, удорожание бэкапов; чтения по PK от роста не страдают | партиционирование/ретеншен вместе с `entries` - задача 14.2; `created_at` уже пишется (2.5) | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-2.1 | 2026-10-14 | done, ревью закрыто (миграции 00001 - 00005, append-only триггер, non-negative) | `.specs/CBL-2.1-ledger-schema.md` | fb47705 |
| CBL-2.2 | 2026-10-14 | done, ревью закрыто (идемпотентность по хэшу тела, покрытие 90.7%) | `.specs/CBL-2.2-post-transaction.md` | a2d0606 |
| CBL-2.3 | 2026-10-14 | done, ревью закрыто (RetryPolicy + полный jitter, observer, миграция 00006, покрытие 91.7%) | `.specs/CBL-2.3-concurrency.md` | 0b623af |
| CBL-2.9 | 2026-10-14 | done, ревью закрыто (бенчмарки, k6 500/1000 rps, потолок пула БД, стратегия в ADR-2.9) | `.specs/CBL-2.9-benchmark-sharding.md` | b663447 |
| CBL-2.4 | 2026-10-14 | done, ревью закрыто (резервы с конечным автоматом, миграция 00007, e2e compose, покрытие 86.6%) | `.specs/CBL-2.4-reserves.md` | f3d4b9e |
| CBL-2.5 | 2026-10-14 | done, ревью закрыто (GetBalance/ListEntries, миграция 00008, снимки balances_after, покрытие 87.3%) | `.specs/CBL-2.5-balance-queries.md` | этот коммит |

## 11. Runbook

Заполняется первой продуктовой задачей сервиса. Типовые аварии: недоступность зависимости,
рост ошибок, лаг обработки; действия: проверить health/metrics, логи с request_id, состояние
Kafka consumer group и outbox lag, при необходимости перезапустить сервис.

Резервы (2.4):

1. Зависшие `active` резервы вне саги: `SELECT ref_id, account_id, amount FROM reserves
   WHERE status = 'active'` - сверять с Payout (10.x); освобождать через `ReleaseReserve`
   тем же ref, не правкой строк.
2. `conflict` (resource_exhausted) на Reserve/Commit: конкуренция на счете; проверить метрики
   повторов (cause `lock_conflict`, `reserve_state_race`), при систематике - план 2.9/14.6.
3. Проводки захвата искать с `ref_type = 'reserve'`; саммари - счет выплат того же
   `merchant_ref`; расхождения балансов без проводок (available/reserved) - это удержания,
   а не потерянные деньги (T-LED-06).
