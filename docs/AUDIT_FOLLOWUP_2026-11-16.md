# Перепроверка незакоммиченной реализации Core

> **Обновление после исправлений (2026-11-16).** Все пять регрессионных
> проверок R1–R5 из этого отчёта повторно выполнены командой
> `python3 docs/audits/2026-11-16-recheck/run_probes.py` и теперь проходят,
> включая fault injection после публикации `CURRENT`. Исправления находятся
> в текущем незакоммиченном рабочем дереве. Ниже сохранён исходный аудит
> **до исправлений**, поэтому формулировки «открытый дефект» и оценка
> «5 исправлений + 3 блока» относятся к тому состоянию. Полная аварийная
> матрица, инкрементальное восстановление, retention/compaction и длительная
> нагрузочная приёмка по-прежнему не закрыты.

> Актуальные результаты интеграционной проверки и вновь найденные ограничения:
> [post-fix audit](AUDIT_POSTFIX_2026-11-16.md).

Дата: 2026-11-16. Go 1.26.11, darwin/arm64, Apple M4 Pro.
База Core: `25267f5affb6f0e1de7a6572aa7217b85fcc9cad`; база Aggregator: `4f87725`.
Результаты относятся к рабочим деревьям, **не к опубликованному v0.1.1**.
Коммитов и публикации в этой проверке не было.

## Вердикт

Существенная часть исходных исправлений реализована и проходит существующие
тесты. Объявлять все замечания закрытыми нельзя: пять дополнительных проверок
воспроизводят дефекты хранения/совместимости. WAL v2 остаётся экспериментальным
opt-in. Дефект identity затрагивает и старый WAL v1 при обновлении реализации.

## Что действительно реализовано

| Область | Реализация и проверка | Ограничение |
| --- | --- | --- |
| Raw API | `NewRaw`, source-local cursor, live reader, offline read, raw payload/provenance; тесты неизвестных/failed/account сообщений и ACK | Raw — повторно сериализованный protobuf; canonical порядок не обещается |
| Подписки | Настраиваемый protobuf request, account filters, failed/vote tri-state, identity профиля | Совместимость legacy identity нарушена, см. R2 |
| Shutdown | Отдельный bounded drain context, deadline, сохранение независимой ошибки sink | Callback, игнорирующий context, нельзя принудительно завершить |
| Размеры и память | Совместимый предел v1, source/dispatch byte-budget, размер batch, backpressure | Это не ограничение всего RSS; для v2 есть R4 |
| ACK | Durable source/consumer cursor, journal binding, монотонность между процессами | Нет общей транзакции с внешней БД; commit consumer → ACK означает at-least-once |
| Dedupe | Опциональный bbolt индекс на source, bounded hot cache, late duplicate/conflict после eviction/restart | Полный rebuild при каждом старте; legacy decoded `Engine.seen` отдельно ограничен |
| Диагностика | Queue bytes/age, fsync histograms, consumer backlog/age | Consumer histogram — process-local выборки при опросах, не распределение возраста всех записей |
| WAL v1 | Batch preflight, rollback, prefix delivery, sparse seek cache | Повреждённый seek cache не всегда даёт fallback, см. R5 |
| WAL v2 | Сегменты, checksum, manifest, rotation, reopen, cross-segment cursor | Пять существующих целевых тестов проходят, но аварийная матрица неполна; R1/R3/R4 |
| Fork semantics | Processed/confirmed доступны как raw; program handlers требуют finalized | Полный canonical fork engine находится за границей raw Core |
| Aggregator replay | Отказ from_slot не зависит от успешности ReplayInfo, повторный live reconnect проверен | Capability запрета живёт в текущем клиенте; persistence/versioned capability ещё не закрыты |
| Aggregator archive | Bounded неблокирующий enqueue, drop/incomplete metrics | Долговременный manifest потерянных диапазонов и bounded stop при зависшем диске не доказаны |

Связь с исходными десятью шагами: 2, 5 и raw-часть 9 имеют реализацию и
регрессионные тесты; 1, 3, 4, 6, 7 и 8 закрыты лишь частично по причинам выше.
Шаг 10 (canonical V2) оценивается отдельно, а не как обязательная функция raw Core.

## Выполненные проверки

- Core: `go test -race -count=1 ./...`, `go vet ./...`, `go build ./...`,
  `git diff --check` — PASS; пять пакетов с тестами, один без тестов.
- Aggregator: те же команды — PASS; 53 пакета с тестами, 21 без тестов.
- Отдельный integration smoke module: первоначально FAIL — отсутствовали
  bbolt зависимости. Исправлены только его `go.mod`/`go.sum` через `go mod tidy`;
  повторные race/vet/build/diff — PASS, один пакет с тестами.
  Корневой `go test ./...` этот отдельный модуль не включает.
- `v2_p0_contract.py --check` — PASS, 1078 строк, runtime **UNVERIFIED**.
- `v2_p1_source_report.py --check`, `v2_p1_closure_oracle.py --check` — evidence
  PASS, runtime **BLOCKED**, 74 физических owner-filter контрпримера.
- `v2_ordered_bakeoff.py --check`, `v2_p2_report.py --check` — evidence
  consistent, acceptance **PARTIAL**; 409 occurrences, 246 `UNSUPPORTED_SCHEMA`.
- `PYTHONPATH=scripts python3 -m unittest scripts/test_v2_sdk_oracle_compare.py`
  — PASS (1 тест). Это проверка сохранённых evidence, не новый live oracle.
- Дополнительные пять audit regression expectations — **FAIL, все пять
  дефектов воспроизведены**. Это отдельные проверки сверх зелёного baseline.

Логи и воспроизводимый запуск: [evidence](audits/2026-11-16-recheck/).
Команда из корня Core:

```sh
python3 docs/audits/2026-11-16-recheck/run_probes.py
```

Скрипт использует Go overlay и временный файл; runtime исходники не меняются.
Ожидания сохранены как `.go.txt`, чтобы не подменять обычную CI suite.
Только R1 использует внедрённый I/O fault; R2–R5 воспроизводятся без инъекции.

Микрозамер `BenchmarkDecodeTransaction`, 3 × 1s: 6600–6744 ns/op,
3064 B/op, 38 allocs/op; fixture throughput 26.10–26.67 MB/s. Это маленькая
synthetic decode fixture, **не throughput WAL/gRPC и не production acceptance**.
Нового live/provider прогона, полного security-аудита и длительного load-теста
в этой перепроверке не проводилось.

## Подтверждённые незакрытые дефекты

### R1 — P1: rollback после публикации CURRENT ломает повторное открытие

Код: `internal/ingest/wal_v2.go`, `appendBatch`, `writeWALV2Atomic`.
Тест: `TestAuditPublishedManifestFailureMustRemainRecoverable`.

Сценарий: данные fsync → новый manifest → rename CURRENT → ошибка fsync
директории. `appendBatch` получает ошибку и обрезает данные до старого offset.
CURRENT уже может ссылаться на новый offset. Reopen возвращает
`active segment is shorter than its committed manifest offset`.
Это воспроизводимая потеря согласованности после ошибки с неопределённым
результатом commit; обычный happy-path тест её не покрывал.

Исправление: различить ошибку до публикации и неопределённый результат после
публикации; после возможного переключения CURRENT не обрезать соответствующий
data prefix. Сделать writer terminal до recovery, согласовать durable callback
с результатом commit. Добавить точки fault injection перед/после каждого fsync,
rename и публикации, включая rotation и initial creation.
Ожидание: ранее подтверждённые записи всегда читаются; неподтверждённый batch
может отсутствовать или повторно выдаваться, но manifest не ссылается на
самостоятельно удалённые данные.

### R2 — P1: неизменённая legacy подписка не открывает старый журнал

Код: `internal/ingest/config.go:Identity`, `store.go:OpenStoreObserved`.
Тест: `TestAuditLegacyIdentityRestart`.

В identity добавлено поле Subscription, поэтому hash изменился даже при nil
custom subscription и прежнем запросе. Старый WAL отвергается как несовместимый.
Старый hash в тесте вычислен точным алгоритмом из опубликованного HEAD.

Исправление: сохранить legacy identity для доказанно эквивалентного профиля
или добавить проверяемый versioned migration. Нельзя просто отключить проверку
identity. Зафиксировать настоящие v0.1.1 fixtures, включая restart/replay и
несовместимый custom profile. Ожидание: unchanged профиль продолжает историю;
изменённый scope по-прежнему требует явного нового журнала/миграции.

### R3 — P2: смена WALVersion незаметно переключает историю

Код: `internal/ingest/store.go:OpenStoreObserved`.
Тест: `TestAuditFormatSwitchMustNotHideHistory`.

При наличии `journal.wal` установка `WALVersion=2` создаёт `journal.v2` в том же
DataDir с новым journal ID. Старые данные физически остаются, но выбранный Store
больше не возвращает эту историю. Это не подтверждённая миграция.

Исправление: проверять существующий формат до создания/перестройки хранилищ;
отказывать при неявном переключении. Явная миграция должна сравнить
count/order/payload и определить судьбу ACK/journal ID. Отдельно проверить
переключение назад. Ожидание: история либо сохранена проверенной миграцией,
либо запуск остановлен с понятной инструкцией, без скрытого нового baseline.

### R4 — P2: допустимый transport payload не помещается в допустимый v2 segment

Код: `internal/ingest/config.go:Validate`, `wal_v2.go:appendBatch`.
Тест: `TestAuditSegmentCapacityMustMatchAdmission`.

Конфигурация MaxMessageBytes=3 MiB, segment=2 MiB, batch=1 MiB проходит
Validate. Payload меньше 3 MiB отвергается writer: frame 3145030 bytes при
доступных 2096952 bytes. Проверка только BatchBytes не покрывает отдельную
крупную запись и header/envelope overhead.

Исправление: связать admission limit, single-record/batch policy и usable
segment capacity, учитывая envelope/header. Либо валидировать достаточный
размер сегмента до подключения, либо явно поддержать oversized-record segment.
Тесты: boundary -1/0/+1, public NewRaw и CLI, одиночный record и полный batch.
Ожидание: принятая конфигурация гарантирует запись любого допустимого сообщения.

### R5 — P2: повреждённый optional sparse index блокирует чтение исправного WAL

Код: `internal/ingest/wal.go:loadWALIndexCache`, `WALReader.ReadAfter`.
Тест: `TestAuditCorruptSparseHintMustFallBack`.

Изменение offset в `.sidx` на один байт проходит size/mtime/structure проверки.
Reader использует offset и возвращает `invalid WAL frame`, хотя WAL исправен.
Документированного fallback на последовательное чтение нет.

Исправление: проверять привязку index entry к frame/sequence; при невалидном
hint отбросить индекс и повторить чтение из проверенной позиции. Для legacy
frames без sequence нельзя доверять номеру из неподтверждённого индекса.
Ожидание: отсутствующий, испорченный или устаревший cache не меняет результат
чтения; реальная коррупция WAL по-прежнему останавливает чтение.

## Сколько работы остаётся

Все пять конкретных дефектов R1–R5 закрыты в текущем working tree и проходят
свои regression probes. Также добавлены class-specific replay frontiers для
всех поддерживаемых `SubscribeRequest` update-классов. Это не доказательство
отсутствия других ошибок: полная аварийная матрица и нагрузка могут выявить
новые.

Три крупных блока доведения raw Core остаются открытыми:

1. Инкрементальное восстановление: WAL-bound checkpoint/index frontier,
   bounded seek; убрать обязательный полный rebuild dedupe/индексов при старте.
2. Retention/compaction: slowest supported consumer ACK, expired cursor,
   сохранение dedupe guarantees. Отдельный best-effort cleanup старых v2
   manifest generations теперь есть; он не удаляет WAL-сегменты и не заменяет
   retention. Сами WAL-данные по-прежнему растут с историей.
3. Сквозная приёмка: Raw Run → v2 WAL → reader → consumer commit/ACK → restart;
   kill/short write/ENOSPC/fsync/rename fault matrix; длительная нагрузка с
   крупными сообщениями, несколькими sources и медленным consumer/disk;
   p95/p99, RSS, disk growth, recovery time. После этого — согласование
   документации, проверка pinned consumer и подготовка релиза без публикации.
   Отдельно проверить реальный replay/retention для каждого поддерживаемого
   subscription profile у целевых провайдеров: локальные frontiers не
   подтверждают, что сервер хранит и возвращает историю.

Порядок внутри хранения также должен учитывать замеченные при чтении кода
риски: recovery изменяет active tail до полной проверки sealed history;
initial-create crash может оставить каталог без CURRENT; v2 reader открывается
с полным scan. Эти сценарии пока не воспроизведены отдельными тестами здесь и
включены в recovery/acceptance блоки, а не объявлены закрытыми.

## Что не входит в обязательное raw ядро

Полная интерпретация CPI, инструкции отдельных программ, token accounting,
canonical bank admission, RPC repair и atomic state writer — отдельные слои.
Публичное raw ядро должно сохранять их входы/качество и не требовать декодеров.

Для Aggregator отдельно остаются account/config completeness (P1 BLOCKED),
ordered economic effects (P2 PARTIAL), целевой dual-FINALIZED admission/repair
и single writer. Числа 74 контрпримера и 246 unsupported occurrences — это
результаты fixtures, **не число отдельных задач или недостающих декодеров**.
Превращать их в остаток обязательной работы над raw Core неправильно.
