# Paypong

Асинхронный сервис платежей из тестового задания: FastAPI + Pydantic v2,
SQLAlchemy 2 async/asyncpg, PostgreSQL, FastStream 0.6/RabbitMQ, Alembic.
Один пакет, две точки запуска, один consumer. Деньги — Decimal/NUMERIC(18,2).

## Быстрый запуск

Нужен Docker с Compose v2. В корне проекта:

```sh
docker compose up --build -d --wait
docker compose logs -f api consumer
```

PostgreSQL и RabbitMQ имеют healthcheck. Одноразовый `migrate` выполняет
`alembic upgrade head` до запуска API/consumer. API: `http://localhost:8000`.
В Compose задан демонстрационный ключ `local-development-key-change-me`;
переопределите `API_KEY` в окружении или `.env` (образец `.env.example`).
Ключ минимум 16 символов. Swagger/OpenAPI отключены, `/health` тоже требует ключ.
БД и брокер наружу не опубликованы. Состояние хранится в именованных volumes.

```sh
curl -i http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-development-key-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"199.90","currency":"RUB","description":"Заказ 42","metadata":{"order_id":42},"webhook_url":"https://example.com/webhook"}'

curl http://localhost:8000/api/v1/payments/PAYMENT_UUID \
  -H 'X-API-Key: local-development-key-change-me'
```

В PowerShell используйте `curl.exe` и подходящее для оболочки экранирование JSON.
POST возвращает 202 с `payment_id`, `status`, `created_at` сразу после SQL commit.
GET возвращает также amount (JSON-строка), currency, description, metadata,
webhook_url, processed_at. Неизвестный UUID: 404; некорректный запрос: 422;
нет/неверный API key: 401; иной payload для прежнего ключа: 409.

Сумма строго положительная, максимум `9999999999999999.99`, до двух дробных
знаков (хвостовые нули допустимы). Валюта RUB/USD/EUR; описание до 2000 символов;
metadata — JSON-объект с конечными числами; webhook — HTTP(S), без credentials
и fragment. URL нормализуется Pydantic. Рекомендуется передавать сумму строкой.
Idempotency-Key обязателен: 1–255 непробельных символов. Один статический ключ
API даёт доступ ко всем платежам этого сервиса.

## Идемпотентность и доставка

Payment и исходный Outbox создаются одной транзакцией. Уникальный SQL-ключ и
`INSERT ON CONFLICT` защищают конкурентные запросы. Тот же ключ с тем же
нормализованным содержимым возвращает 202 с тем же ID и текущим статусом,
без нового события. Сравниваются все значимые поля, включая metadata/webhook;
порядок JSON-ключей и эквивалентные числа не влияют. Другой payload — 409.

Publisher работает фоновой задачей API: due-строки Outbox выбираются с
`FOR UPDATE SKIP LOCKED`, публикуются в durable `payments.new` как persistent
сообщения, помечаются только после broker confirm и проверки mandatory routing.
Транзакция удерживает соединение во время небольшого batch (по умолчанию 20,
timeout публикации 5 сек); для высокой нагрузки понадобятся отдельный publisher
и более короткие batches/lease. При недоступном RabbitMQ API сохраняет новые
платежи, а Outbox ждёт восстановления. Несколько publisher безопасны.

Consumer обрабатывает событие одним сценарием: резервирует попытку в БД,
эмулирует шлюз (асинхронные 2–5 сек, 90% succeeded/10% failed), сохраняет
результат, отправляет webhook и сохраняет доставку. Результат эмулятора
стабилен по payment_id: повтор неизвестного результата согласован с первым.
Очередь использует `x-single-active-consumer`, prefetch=1: одновременно работает
один обработчик даже при запуске нескольких экземпляров. Шлюз и HTTP вызываются
вне SQL-транзакций. Статусы платежа: pending/succeeded/failed; технический сбой
уведомления не меняет результат платежа. Бизнес failed также получает webhook.

Webhook POST имеет поля `notification_id`, `payment_id`, `status`, `amount`,
`currency`, `processed_at`, а также заголовок Idempotency-Key=notification_id.
Успешны только HTTP 2xx. Redirects не выполняются; timeout по умолчанию 10 сек.
Стабильный notification_id позволяет получателю удалять дубли.

## Retry, аварии и DLQ

Максимум **3 попытки всего**, первая + 2 повтора, задержки 1 и 2 секунды
(`RETRY_BASE_SECONDS` меняет базу). Счётчик хранится в payments. Техническая
ошибка атомарно сохраняет состояние и новый Outbox c `available_at`; publisher
позже отправляет его в ту же очередь. Задержка долговечна, без памяти процесса,
дополнительных consumers и RabbitMQ TTL/DLX. Исходное сообщение ACK только
после завершения либо SQL commit передачи ответственности в retry/DLQ.

После третьей ошибки создаётся Outbox в durable `payments.dlq`, с IDs,
attempt, stage и причиной. Состояние dead_lettered не меняет платёж на failed:
при неисправном шлюзе он может остаться pending, при сбое webhook сохраняет
ранее полученный terminal результат. Некорректные JSON/события сразу идут в DLQ.
Неисправность БД/передачи ответственности оставляет оригинал через NACK;
это транспортное восстановление, а не новая бизнес-попытка.

Окна аварии:

- После публикации до SQL commit Outbox может публиковаться повторно с тем же event_id.
- После резервирования attempts, но до завершения, redelivery считает попытку
  прерванной и планирует следующую/DLQ. Дубли уже запланированного retry — no-op.
- Сохранённый terminal результат пропускает шлюз и продолжает webhook.
- После принятия webhook до локального commit уведомление может повториться.
- После сохранения доставки до ACK повтор ничего не делает.

Гарантия at-least-once, не exactly-once. Настоящему шлюзу нужны собственные
идемпотентный ключ и запрос неизвестного результата. DLQ требует операционного
разбора причины; автоматического replay API в этом задании нет.

## Локальная разработка и проверки

Python 3.13+, uv. Зависимости зафиксированы в `uv.lock`:

```sh
uv sync --frozen
# PostgreSQL/RabbitMQ должны быть доступны по DATABASE_URL/RABBITMQ_URL
uv run alembic upgrade head
uv run uvicorn paypong.api:create_app --factory
uv run python -m paypong.worker
```

Settings читает `.env`; Alembic читает именно переменную окружения DATABASE_URL.
API_KEY обязателен при локальном старте (в Compose задан). Остальные настройки
и ограничения находятся в `paypong/config.py`.

```sh
uv run python scripts/ai/verify.py --project .
```

Обязательные команды заданы в `.ai/pipeline.json`: Ruff lint + format, unit,
HTTP-контракты и архитектурные границы. Verifier использует тот же интерпретатор,
не запускает shell и возвращает ошибку при пустых checks, отсутствующей команде,
timeout или ненулевом коде. Для полной проверки нужны отдельные тестовые сервисы:

```sh
docker run -d --name paypong-test-postgres -e POSTGRES_USER=paypong -e POSTGRES_PASSWORD=paypong -e POSTGRES_DB=paypong_test -p 127.0.0.1:55432:5432 postgres:17-alpine
docker run -d --name paypong-test-rabbit -e RABBITMQ_DEFAULT_USER=paypong -e RABBITMQ_DEFAULT_PASS=paypong -p 127.0.0.1:55672:5672 rabbitmq:4.1-alpine
export TEST_DATABASE_URL=postgresql+asyncpg://paypong:paypong@localhost:55432/paypong_test
export TEST_RABBITMQ_URL=amqp://paypong:paypong@localhost:55672/
uv run python scripts/ai/verify.py --project . --integration
```

В PowerShell задайте `$env:TEST_DATABASE_URL='...'` и `$env:TEST_RABBITMQ_URL='...'`.
Дождитесь готовности сервисов. Integration тесты очищают таблицы и очереди:
**только отдельная БД с `test` в имени и выделенный брокер/vhost**.
Отсутствие URL или skipped integration тест считается ошибкой, не успехом.
Проверяются реальные миграции/ограничения/конкурентность/откаты PostgreSQL,
FastStream publisher confirms/mandatory/persistence, API→consumer→webhook,
backoff/DLQ и окна сбоя. CI запускает тот же полный verifier с service containers.

## Архитектура и задача

`domain` — dataclasses; `application` — классы сценариев, DTO и Protocol-порты;
`infrastructure` — PostgreSQL/RabbitMQ/HTTP/шлюз; `presentation` — HTTP и один
consumer; `bootstrap` владеет ресурсами и lifecycle. На каждый транзакционный
этап новый UoW, явный commit и rollback при незавершённом выходе.

Спецификация, план, фактические проверки и статус: `docs/tasks/payment-service/`.
Остановка приложения: `docker compose down` (volumes сохраняются).

Дополнительный сквозной smoke с локальным HTTP-получателем webhook (после старта Compose):

```sh
docker run --rm --name paypong-smoke --network paypong_default -v "$PWD/scripts/ai/compose_smoke.py:/smoke.py:ro" python:3.13-slim python /smoke.py
```

Использованные контракты библиотек сверены с установленными версиями и
[FastStream acknowledgement](https://faststream.ag2.ai/latest/getting-started/acknowledgement/),
[RabbitBroker API](https://faststream.ag2.ai/latest/api/faststream/rabbit/RabbitBroker/),
[aio-pika publisher confirms](https://docs.aio-pika.com/rabbitmq-tutorial/7-publisher-confirms.html).
