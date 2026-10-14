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
- gRPC: сервер LedgerService
- Kafka продюсирует: ledger.events
- Kafka потребляет: payment.events
- Хранилище: PostgreSQL ledger

Контракты заморожены в `cbl-protos` (CF-1 для gRPC, CF-2/CF-3 для событий), см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint в Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

## 5. Порты и окружение

HTTP `8040`, gRPC `9040`, метрики `9240`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

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
| T-LED-01 | Валидация, идемпотентность и оптимистичные повторы живут в `storage.Repository` | транзакционный контекст и атомарность в одном слое | usecase тонкий, границы слоев размыты | с 2.3 политика повторов вынесена в `RetryPolicy` и `RetryObserver`; решение о шардировании и батчинге горячих счетов - по бенчмарку 2.9 |
| T-LED-02 | Сумма дебетов и кредитов проверяется приложением и барьером БД (деферред constraint trigger 00006) | гибкость приложения плюс второй барьер на коммите | ограничения барьера: прямым SQL в обход приложения возможно дописывание сбалансированной пары к чужой транзакции и смешение активов (вне модели угроз; балансы не меняются) |
| T-LED-03 | Отрицательный `available` разрешен для clearing, fee и suspense | поддержка потока Dr clearing / Cr merchant без префандинга | ошибка в политике возможна у транзитных счетов | контроль по типам счетов, мониторинг suspense в 2.6 |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-ledger-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-ledger-02 | Нет mTLS и authN на gRPC LedgerService | порт 9040 не публиковать за пределы доверенной сети; prod-выкладка запрещена | mTLS и авторизация до prod (0.6, 1.4) | open |
| KI-ledger-03 | Каталоги e2e и load пусты | шаг 8 выполнен частично: integration и smoke compose есть, сценариев e2e/нагрузки нет | e2e и k6 в 2.4 и 2.9 | open |
| KI-ledger-04 | Race detector недоступен локально (нет CGO на Windows) | конкурентные тесты идут без -race | включить -race в CI (0.2) | open |
| KI-ledger-05 | Функция `balances_non_negative()` (миграция 00005) не фиксирует `search_path` | низкое: подмена требует прав CREATE в схеме раньше public; 00006 захардена (`public.entries`, search_path) | применить hardening отдельной миграцией при плановом изменении 00005 | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-2.1 | 2026-10-14 | done, ревью закрыто (миграции 00001 - 00005, append-only триггер, non-negative) | `.specs/CBL-2.1-ledger-schema.md` | fb47705 |
| CBL-2.2 | 2026-10-14 | done, ревью закрыто (идемпотентность по хэшу тела, покрытие 90.7%) | `.specs/CBL-2.2-post-transaction.md` | a2d0606 |
| CBL-2.3 | 2026-10-14 | done, ревью закрыто (RetryPolicy + полный jitter, observer, миграция 00006, покрытие 91.7%) | `.specs/CBL-2.3-concurrency.md` | этот коммит |

## 11. Runbook

Заполняется первой продуктовой задачей сервиса. Типовые аварии: недоступность зависимости,
рост ошибок, лаг обработки; действия: проверить health/metrics, логи с request_id, состояние
Kafka consumer group и outbox lag, при необходимости перезапустить сервис.
