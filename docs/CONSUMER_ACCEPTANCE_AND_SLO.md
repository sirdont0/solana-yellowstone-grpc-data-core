# Приёмка Raw Core и контракт downstream-потребителя: тесты, SLI и SLO

Статус: **проектный контракт приёмки, не подтверждение production SLO**. Дата: 2026-11-19. Применяется к публичному `dev` дерева `solana-yellowstone-grpc-data-core`. Численные **целевые примеры** ниже являются предварительными бюджетами для согласования с конкретным потребителем; измеренные значения выделены отдельно. Этот документ не повышает статусы Aggregator V2 P1 `BLOCKED` и P2 `PARTIAL` и не объявляет каноническую полноту.

Связанные локальные документы: [согласованная область Core](PROJECT_SCOPE.md), [source contract](SOURCE_CONTRACT.md), [провайдеры](PROVIDER_MATRIX.md), [release gates Core](RELEASE_GATES.md), [архитектура](ARCHITECTURE.md), [raw quick start](RAW_QUICKSTART.md), [WAL v2](WAL_V2_CONTRACT.md), [технический долг](TECH_DEBT.md), [аудит 17.11](AUDIT_VERIFICATION_2026-11-17.md), [локальные измерения](audits/2026-11-17-verification/CONTINUATION.md), [roadmap Aggregator V2](AGGREGATOR_V2_ROADMAP.ru.md).

## 1. Граница ответственности и этапы допуска

Core — универсальный источник **наблюдений Yellowstone**, не декодер экономических событий Pump.fun и не канонический Solana ledger. `NewRaw` сохраняет выбранные `SubscribeUpdate` в WAL без программного декодера. Фильтр задаёт потребитель; наборы `transactions`, `transactions_status`, `accounts`, `slots`, `blocks_meta`, `blocks`, `entry`, `block_footer` и другие допустимые поля определены текущим `proto/solanacore-geyser.proto`. Наличие поля в protobuf **не доказывает** поддержку конкретным провайдером: это проверяется отдельно. Сохранённый `RawObservation.Payload` — protobuf, повторно сериализованный после gRPC-декодирования, а не исходный HTTP/2/network frame.

Приёмка разделяется на три рубежа:

| Рубеж | Что разрешено утверждать | Что запрещено выводить |
| --- | --- | --- |
| **P — provisional observation** | Получено и разобрано обновление данного соединения; при `Delivery=Provisional` handler может получить его до `fsync`. | Что оно переживёт crash, принадлежит окончательной цепочке или закрывает банк. |
| **D — durable source-local observation** | Запись включена в проверенный durable prefix WAL конкретного `(source, identity, journal_id)`; `Durable` handler вызывается после успешного `fsync` WAL v1 или принятого commit-протокола v2. | Что провайдер отдал все совпадающие обновления, что нет пропусков по аккаунтам или что две подписки синхронизированы. |
| **C — consumer/application admission** | Потребитель атомарно сохранил собственные эффекты и ключ идемпотентности; затем ACK сдвигает его курсор. Более сильный статус `canonical` допускается только отдельным проверенным oracle/commit-протоколом приложения. | Что Core ACK сам по себе делает эффекты exactly-once или что `finalized` подтверждает полноту фильтрованной истории. |

**Базовый профиль для испытания A/B-устойчивости:** минимум два Yellowstone-провайдера из реально независимых `failure_domain`, каждый со своей подпиской, reconnect loop и WAL; `finalized` для допуска декодированных обработчиков; стабильный `YLABWAL1` (`WALVersion=1`) по умолчанию, `WALVersion=2` только opt-in в эксперименте. Сам публичный Core поддерживает и один источник, но при одном источнике нельзя заявлять A/B-устойчивость. Несколько соединений с одним backend считаются одним failure domain. Сопоставление A/B — независимое свидетельство и диагностика расхождений, **не общий упорядоченный stream**. Для raw `processed`/`confirmed` допустимы как предварительные, fork-aware наблюдения; автоматического продвижения их в C/canonical нет.

Конфигурация источника должна закреплять `source ID`, `failure_domain`, endpoint, `commitment`, полную subscription protobuf identity, `filter_version`, WAL version и выбранные политики ошибок. Секреты передаются через env/secret manager. Замена фильтра/endpoint/identity не должна молча переиспользовать WAL и ACK; для новой идентичности требуется отдельный data root либо явная проверенная миграция и пере-регистрация consumer.

## 2. Реальный публичный контракт и обязанности consumer

В `raw.go` доступны `RawObservation`, `RawCursor`, `RegisterConsumer`, `UnregisterConsumer`, `ConsumerCursor`, `OpenRawReaderFromConsumer`, `RawReader.Next(ctx, limit)`, `RawReader.Close`, `Ack` и `ConsumerLag`. Курсор несёт `(source, identity, journal_id, sequence)` плюс `consumer_epoch` для зарегистрированного потребителя. `sequence` монотонен **внутри одного WAL**, не между провайдерами; нулевая последовательность — начальная позиция. Повторная регистрация существующего consumer не обнуляет его ACK; явное снятие регистрации и последующая регистрация создают новую эпоху. `ErrCursorExpired`, `ErrCursorRegression` и `ErrConsumerUnregistered` являются состояниями, требующими явного восстановления или вмешательства, а не разрешением пропустить данные. В одном `RawReader` допускается один активный `Next`; `Close` может прервать ожидание.

Оболочка содержит `Kind`, `Type`, `Source`, `Generation`, `Commitment`, `FilterVersion`, `Identity`, `Slot`, необязательные `BankID`/`BankIDPresent`, `TxIndex`, `Signature`, `Account`, `WriteVersion`, `Failed`/`StatusPresent`, `FeeLamports`/`FeePresent`, `ReceivedAt`, `SourceCreatedAt`, `Digest`, `Quality` и `Payload`. Необязательные поля нельзя трактовать как гарантированно заполненные для каждого вида update. `bank_id` и `write_version` являются source-local и не используются для глобальной идентичности между backend. Lifecycle/quality markers также занимают номера последовательности и входят в lag.

Алгоритм корректного downstream-потребления:

1. Зарегистрировать **стабильное** имя consumer для каждого source, явно выбрать `ConsumerFromBeginning` или `ConsumerFromHead`. Последний вариант намеренно пропускает существующий журнал и разрешён только при документированном новом baseline.
2. Открыть reader с durable cursor, читать ограниченными страницами (`Next(ctx, 128)` — работающий пример, не нормативный размер).
3. В **одной транзакции приложения** сохранить запись/эффект и уникальный ключ `(source, identity, journal_id, sequence)` с проверкой digest. При повторе того же ключа и payload — no-op; при отличающемся payload — блокирующий конфликт. Для экономических эффектов нужен ещё доменный ключ, предотвращающий двойной эффект одинаковой транзакции из A/B, без уничтожения самих source-local свидетельств.
4. Только **после commit+durability** приложения вызвать `Ack(consumer, observation.Cursor)`. Если crash случился между commit и ACK, Core повторно доставит запись, а база приложения должна распознать повтор. Для внешних RPC, очередей, торговли и других side effects нужен собственный transactional outbox/idempotency protocol; ACK Core не разделяет транзакцию с ними.
5. Сохранять provenance и времена `event_time` (если извлечено из payload), `source_created_at`, `received_at`, `wal_durable_at` (если измеряется отдельно), `consumer_known_at`, `backfill_discovered_at`. Для ML replay запрещено выдавать восстановленные позже события за известные модели в момент исходного market-time.

`ConsumerLag` даёт `head_sequence`, `ack_sequence`, `pending_records`, возраст **самой старой** ожидающей записи и histogram выборок этого возраста на каждый вызов API. Это не histogram задержки каждой записи и не счётчик отсутствующих в Solana транзакций. ACK не даёт права удалять WAL: ACK-aware retention и compaction пока отсутствуют. Предварительный контракт retention должен учитывать самого медленного зарегистрированного consumer, replay/recovery и обязательный исторический срок; при истечении истории нужен явный `ErrCursorExpired`/resync, не silent skip.

Для обработчиков `Engine` API `Delivery=Provisional` и `Delivery=Durable` задаёт момент вызова `EventSink`; это отдельный путь от durable raw reader. Ошибки хранилища, identity conflict и sink останавливают engine. `SkipAndFlag` относится к **целой ошибочной транзакции декодера** и оставляет диагностический след; он не делает пропущенный эффект безопасным для строгой торговой модели. Встроенный cross-source dedupe декодированных finalized событий не означает canonical admission. `NewRaw` этот decoded-event путь обходит.

## 3. Три разные полноты и правила расхождений

Нужно хранить раздельные статусы:

| Измерение | Доказательство, необходимое для повышения статуса | При отсутствии доказательства |
| --- | --- | --- |
| `transport/source_continuity` | Durable WAL, валидные frame/CRC/sequence, identity, сведения о reconnect и попытке `from_slot`, отсутствие неизвестных участков внутри заявленного наблюдаемого диапазона. | `UNKNOWN`/`BROKEN`, даже если A и B совпали. |
| `transaction_coverage` | Для каждого проверяемого **произведённого** finalized bank: независимый полный список транзакций/индексов и проверка применённых фильтров, ошибок/fee, ALT/CPI occurrence и границ истории. | `UNVERIFIED`; отсутствие сообщений в sparse program subscription не является slot gap. |
| `account_config_coverage` | Независимая последовательность account writes и конфигурационных состояний, включая owner/close/recreate, Token-2022 TLV, metadata/config updates, pre/post baseline. | `UNVERIFIED` даже после успешного `getBlock`: транзакционный RPC не восстанавливает автоматически каждое изменение account stream. |
| `ordered_protocol_effects` | Детерминированный порядок `(finalized bank, tx index, instruction/CPI occurrence)`, атомарные multi-account/multi-mint эффекты, сравнение с независимыми fixture/oracle. | Не допускать exact state или финансовый сигнал, зависящий от неизвестной истории. |

`SubscribeReplayInfo` и `from_slot` — механизмы запроса/оценки replay у сервера, **не обещание исторического покрытия**: наличие метода в proto не гарантирует, что endpoint хранит нужный диапазон или типы обновлений. Reconnect cursor Core выбирает консервативно по запрошенным классам сообщений; если класс ещё не наблюдался, нельзя продвигать общий курсор по другому классу. `block_meta`, `block_footer`, ping, завершение слота и сам факт `finalized` не доказывают полной выдачи фильтрованных транзакций и account/config. Проверять `getBlocks`/`getBlock` по finalized можно как внешний транзакционный свидетель; skipped slots отличать от пропущенных produced banks. Для account/config нужен дополнительный источник истории/baseline. RPC repair ограничивать бюджетами времени, запросов, 403/429 и pruning; если он не завершён, оставлять gap open.

Для A/B строить **две** source-local последовательности и отдельный comparison ledger. Совпадающие finalized `(slot, tx_index, signature, нормализованный payload)` увеличивают confidence для этой транзакции; одинаковые данные двух зависимых endpoint не считаются независимыми доказательствами. `missing_on_A`, `missing_on_B`, late arrival, payload conflict, bank/root conflict, unsupported filter и provider outage — разные категории. Расхождение не разрешать правилом «большинство из двух». При конфликте source-local записи сохраняются; downstream admission затормаживается для затронутой области до проверки независимым oracle или явной политики деградации. В будущем `processed` provisional ветви следует хранить fork-aware и уметь rollback; это отдельный критерий приёмки, сейчас не production baseline.

## 4. SLI: определение, знаменатель и способ измерения

Все latency SLI считают по source, subscription identity, commitment, виду записи, размеру payload, профилю нагрузки и статусу (normal/reconnect/replay/backfill). Смешивать provisional с durable или live с repair в одном percentile нельзя. `p50/p95/p99` вычислять из **per-record** выборок с монотонными часами на одном хосте; межхостовые времена использовать только после оценки NTP offset/uncertainty. Отдельно публиковать count, max, sample interval, missing timestamps и censored/timeout observations. Percentile без объёма выборки и диапазона времени не принимается.

| SLI | Формула/смысл | Источник измерения |
| --- | --- | --- |
| Ingress throughput | `accepted_updates / wall_seconds`, `sum(encoded_payload_bytes) / wall_seconds` за устойчивое окно; дополнительно offered provider rate/size и backlog. | Tap на приёме gRPC, WAL counters, capture fixture. |
| `receive→durable` | `t_fsync_batch_commit - t_after_Recv` для каждой записи; для v2 берётся время после committed-manifest publication, не только segment fsync. | Явные timestamp probes (часть сейчас не публичная). |
| `receive→provisional` | `t_handler_entry - t_after_Recv`; лишь для `Delivery=Provisional`. | Handler instrumentation. |
| `durable→consumer_commit` | `t_app_db_commit - t_wal_durable`; измерять отдельно `commit→ACK` и `head→ACK` lag. | Instrumented consumer + Core cursor. |
| `provider_created→receive` | `ReceivedAt - SourceCreatedAt`, если provider timestamp есть и часы согласованы; иначе `N/A`. | `RawObservation`; не интерпретировать как чистую сеть. |
| Queue pressure | source writer oldest age, pending, reserved bytes/limit; dispatch oldest wait/depth/bytes; duration и count `fsync`. | `Engine.Health()`; гистограммы writer-age и fsync attempts — диагностические, не прямой receive→durable SLI. |
| Consumer lag | `head_sequence - ack_sequence`, oldest pending age; доля времени lag > budget. | `ConsumerLag`; marker records входят в числитель. |
| Recovery time | `t_first_normal_durable_after_recovery - t_fault_detected`; отдельно `t_reconnect`, `t_replay_end`, `t_consumer_caught_up`. | Fault injector + trace. |
| Coverage/discrepancy | Доля подтверждённых matching transactions/accounts среди **oracle-expected** в заданном диапазоне; `unknown` отдельно; A/B mismatch counts. | Сопоставление с независимым bank/account witness, не оценка только по двум streams. |
| Durability/integrity | `corrupt_frames`, sequence gaps, missing ACKed records, conflicting duplicate payload, recovery divergence; целевое значение для допущенного durable prefix — 0. | Строгий WAL verify, restart/replay/DB compare. |
| Resource cost | CPU cores, wall CPU, RSS peak/steady, Go heap, GC pause/CPU, allocs/update, WAL bytes/update и GB/day, SSD write IOPS/bandwidth. | OS profiler/Go pprof/metrics, тестовый host log. |

Публичный `Health()` уже предоставляет queue byte usage, queue age/wait, WAL sync duration/count, counters и continuity quality; `ConsumerLag()` предоставляет source-local lag. Отдельный end-to-end latency recorder, per-record fsync timestamps, exporter histogram и oracle coverage scorer **не являются заявленной готовой функцией Core**. Их нужно реализовать в стенде/потребителе либо добавить как отдельную контролируемую инструментализацию до подписания SLO.

## 5. Предварительные бюджеты для проектирования — НЕ измеренные обещания

Следующая таблица — **пример стартовых acceptance targets**, предполагающий один выделенный NVMe host, Linux amd64, 1–4 KiB typical raw payload, 2 независимых источника и bounded in-process consumer без тяжёлого декодирования. Значения **не получены из live provider** и подлежат пересмотру после 15–60 минут репрезентативного capture и обсуждения с downstream командой. При необходимости больших block/account payload вводятся отдельные классы; нельзя экстраполировать эти числа на 95 MiB messages.

| Сценарий/SLI | Предварительный ориентир (assumption) | Критерий отказа |
| --- | --- | --- |
| 1× captured peak, steady 30 мин | 100% expected fixture records durably accepted, без растущего backlog; `receive→durable` p95 ≤ 100 ms, p99 ≤ 250 ms | Любая потёрянная accepted запись или устойчивый рост backlog. |
| 3× peak, 10 мин burst + drain | Полный source-local replay/verification; drain ≤ 2× длительности burst; p99 ≤ 1 s в burst | Необъяснённая потеря, CRC/sequence mismatch, terminal deadlock; latency over budget требует capacity decision. |
| 10× peak, 2 мин overload | Controlled backpressure/fail-closed, bounded queues and recovery; **нет обязательного** real-time p99 при перегрузе | Silent drop или неподконтрольное увеличение RSS/диска. |
| Consumer normal catch-up | oldest pending ≤ 5 s p95 по 1-min scrape series; после краткого outage догоняет быстрее входа ≥ 2× | Неспособность догнать или ACK без app commit. |
| Planned graceful stop | 0 потерянных fsynced records; error/`ErrDrainTimeout` при превышении заданного 15 s default drain | Успешное завершение с неподтверждённой потерей. |
| One-source outage | Surviving source continues independently; affected source marked degraded; no false canonical promotion | Общий стоп без технической причины либо silently merged A/B cursor. |
| Crash/restart | 0 исчезнувших ранее durable/ACKed records; no duplicate app effect; deterministic WAL replay | Любое silent truncation committed prefix или duplicate effect. |
| 24 h soak, profile 1× | 0 integrity errors; no unbounded RAM/disk queue growth; measured p95/p99 and WAL GB/day | Утечки/рост очереди, не соответствующий входу, скрытые continuity breaks. |

Примерные 100/250 ms — **переговорные performance targets**, не свойства Yellowstone или `fsync`. Для trading path budget следует разделить на provider/transport, Core WAL, consumer decode, bank admission, strategy и execution; `finalized` сам по себе добавляет consensus delay, поэтому сравнение с ultra-low-latency `processed` путём некорректно без общей временной базы. Для ML/offline можно утвердить более мягкие latency SLO при более строгой полноте и воспроизводимости. Если workload не выдерживается, фиксировать capacity envelope вместо подмены correctness критерия.

## 6. Реально измеренное в дереве `dev` (не production SLO)

Локальный WAL-only эксперимент 17.11 с 1 KiB payload, batch до 64 записей, 2 MiB segments, без реального Yellowstone, disk dedupe и downstream DB: 1,000/s offered → **998.9/s** achieved, p95/p99 append **37.07/74.32 ms**; 3,000/s → **1,955.4/s**, **39.05/74.97 ms**, max schedule lag **16.03 s**; 10,000/s → **1,920.8/s**, **44.97/74.95 ms**, max schedule lag **126.18 s**. Всего 420,000 записей проверены после reopen без mismatch. `append` latency здесь не включает schedule lag; поэтому малый p99 **не доказывает** обработку 3×/10× в real time. В этом fixture sustained 3×/10× throughput **не пройден**. См. [полный отчёт](audits/2026-11-17-verification/CONTINUATION.md) и [сырой лог](audits/2026-11-17-verification/load-30s.txt).

Отдельный 10,000-record microbenchmark открытия WAL v2 + dedupe с прогретыми seek indexes: full rebuild **138.97 ms**, verified recovery cache **57.63 ms** mean; результаты не включают исторический callback, Engine startup или большой архив. Проверка 17.11 включала ограниченные crash/rotation сценарии, race/unit/static analysis, но **не** полный I/O fault matrix, 24h soak или live provider. Это доказательства локальных путей, не выполнение таблицы бюджетов §5.

## 7. Воспроизводимые тесты и матрица отказов

Каждый прогон сохраняет manifest: git commit, Go/OS/kernel/CPU/RAM/SSD/FS, WAL version, dedupe mode, source/filter identity (без secret), provider name/failure domain и его версии если доступна, sizes/rate distribution, timestamps, fault schedule, параметр seed, counters до/после, raw WAL digests, oracle result и точные команды. Фиксировать раздельно `offered`, `accepted`, `durable`, `consumer committed`, `acked`, `oracle expected` и `unknown` — без смешения знаменателей. Использовать детерминированные fixtures с известными protobuf update bytes и ожидаемыми digest/sequence/positions; повторять минимум 3 seed/run для latency, bootstrap и race-sensitive paths.

| ID | Проверка/инъекция | Обязательное наблюдаемое поведение |
| --- | --- | --- |
| F01 | v1 WAL fixture: transaction, failed transaction+fee, account, slot, block-meta, block, entry, footer, ping/marker, unknown-compatible variant | Все выбранные виды сохраняются по одному source, protobuf variant согласован с `Kind`, порядок/CRC/sequence совпадает; неподдержанный variant fail/diagnostic согласно контракту. |
| F02 | Перестановки `created_at`, одинаковые slot/signature, разные `tx_index`, отсутствие optional bank ID, account write version | Raw не теряет дубликаты свидетельств без законного dedupe, source-local order сохраняется; никакого глобального sort по timestamp. |
| F03 | Full WAL replay дважды, consumer DB recreate, crash между DB commit и ACK | Один application effect на idempotency key, redelivery без дубля, ACK монотонен, digest conflict блокирует. |
| F04 | Параллельные Engine/process ACK, re-register/unregister и старый epoch, stale cursor, journal recreation | Lock/epoch fencing, явные `ErrCursorExpired`/`ErrCursorRegression`/`ErrConsumerUnregistered`, ни одного backward/foreign ACK. |
| F05 | Два независимых source: одинаковая finalized tx, только A, только B, conflicting payload/bank, delayed messages | Обе исходные цепочки хранятся; comparison ledger маркирует расхождение; consumer не объявляет `canonical` без oracle. |
| F06 | Disconnect одного провайдера, обоих, DNS/TLS failure, auth failure, idle timeout, ping/pong issue, `from_slot` rejection, retention exhausted | Индивидуальные reconnect/quality markers; попытки replay с доказанным source frontier; отказ не превращается в clean continuity. `AllowLiveFallbackOnReplayRejection` только с явным gap. |
| F07 | Sparse program filter с отсутствующими слотами, skipped slot, observed block_meta без tx, incomplete bank, late tx | Sparse отсутствие не считается slot gap; bank closure/transaction coverage остаются UNVERIFIED без full-bank witness. |
| F08 | Account owner/close/recreate, Token-2022 TLV/config evolution во время disconnect, новый baseline | Account/config gap остаётся open до независимого account witness; RPC `getBlock` один не повышает полноту. |
| F09 | Обрыв питания/process kill до append, после append, до/после fsync, в v2 до/после manifest/`CURRENT`, во время rotation | Только verified committed prefix durable; uncertain v2 commit reconciled/fail-closed; ни одного ACKed record не исчезает. |
| F10 | Torn active tail, CRC corruption внутри committed prefix, missing sealed segment, stale/corrupt index/checkpoint | Допустимый tail восстанавливается с marker; повреждённый committed prefix/segment блокирует admission; кеш никогда не источник истины. |
| F11 | Disk full, `ENOSPC`, short write, fsync/rename/dir-sync error, read-only FS, lock failure, oversized frame | Sticky error, no false durable callback, bounded allocation, safe restart; fail closed при неразрешённой неопределённости. |
| F12 | Slow/stalled consumer, slow handler, queue saturation, shutdown timeout, multi-source skew | Bounded memory/byte queues, измеренный oldest age и drain; отказ без silent drop; ACK lag и pressure видны. |
| F13 | Replay 1×/3×/10× captured input с теми же digest и random chunking; 24h soak | Проверка count/order/source identity, p50/p95/p99, CPU/RSS/GC, WAL volume, catch-up и стабильности очередей. |
| F14 | `processed` provisional fork A/B, dead slot, rollback, later finalized copy (отдельный future profile) | Provisional state может быть отозван; finalized admission не наследует неподтверждённый provisional факт. Пока профиль не принят — disabled для exact consumers. |
| F15 | Time-aware ML replay с delayed reconnect/backfill | `known_at` исходного live сигнала не переписывается историческим временем; offline parity и online decision parity оцениваются раздельно, без lookahead. |

**Две parity-проверки обязательны и различаются по смыслу.** `Source replay parity`: последовательность durable source WAL, digest, type, cursor и application idempotent output совпадают после повторного запуска. `Canonical/economic parity`: downstream reducers дают одинаковый state на **независимом полнобанковом и account/config oracle** и на live/admitted path. Первый PASS не подразумевает второй. Для Aggregator V2 дополнительно проверять атомарность multi-mint effects, правильный CPI order, ценовые/holder инварианты, миграции пулов, eviction/rehydration и offline ML windows. Это отдельная downstream приёмка, не задача универсального Core.

### Рабочие команды локального baseline

Из корня репозитория (без необходимости provider credentials):

```bash
GOWORK=off go test ./...
GOWORK=off go test -race -count=1 ./...
GOWORK=off go vet ./...
go test ./internal/ingest -run '^$' -bench '^BenchmarkWALV2StoreRecovery$' -benchtime=3x -count=1
CORE_WAL_LOAD=1 CORE_WAL_LOAD_SECONDS=30 go test ./internal/ingest -run '^TestWALV2SyntheticLoad$' -count=1 -v
```

Последняя команда намеренно предлагает **30 секунд на каждый** из 1×/3×/10× профилей, но перегруженные профили дренируются дольше и требуют свободного диска. Запускать один эксперимент на выделенной машине, без конкурирующих тестов. Он измеряет WAL-only, не public Engine SLO. Для provider acceptance использовать команду из [raw quick start](RAW_QUICKSTART.md) с собственными переменными `YELLOWSTONE_ENDPOINT`, `YELLOWSTONE_TOKEN`, `PROGRAM_ID` и отдельным test data root; `corectl verify/inspect/replay` запускаются на остановленном capture. `verify` проверяет WAL/variant/identity, **не** покрытие Solana.

Рекомендуемый live-протокол: 15 минут warmup + ≥ 60 минут нормальной нагрузки (включая естественные пики); один controlled disconnect A, один B и один обеих; тест 1×/3×/10× из предварительно записанного репрезентативного trace; отдельный 24h soak; затем cold restart и сравнение source WAL/consumer DB/oracle. Для каждого провайдера отдельно исследовать `SubscribeReplayInfo`, фактический минимум replay, выбранные фильтры, rate limit, schema/version, failed transactions и account history. Невозможность получить независимый witness отмечать `UNVERIFIED`, не заменять предположением.

## 8. Test gates downstream и реакция потребителя

Ниже `T0–T5` — **проверки данного consumer/test профиля**. Это не номера релизных `G0–G6` публичного Core: официальная карта релиза и границы продукта находятся в [RELEASE_GATES.md](RELEASE_GATES.md).

| Gate | PASS только если | При FAIL/UNKNOWN |
| --- | --- | --- |
| **T0 — API/identity** | Подписки валидированы, identity и journal fencing, source-local records/ACK и idempotent DB тесты проходят. | Остановить consumer, устранить несовместимость/ошибку; не сбрасывать курсор скрытно. |
| **T1 — Raw durability** | v1 fault matrix, integrity/replay parity, no false durable callback и bounded failure под 1×; параметры и артефакты опубликованы. | Не использовать Core как гарантированный durable input. |
| **T2 — Provider compatibility** | Для профиля A/B протестированы ≥2 независимых endpoint; replay/filter/schema behavior и gap policy зафиксированы по каждому. | Источник `DEGRADED`/`UNVERIFIED`, без автоматического «сведения» A/B. |
| **T3 — Capacity/SLO** | Репрезентативные нагрузочные тесты и 24h soak измерены, согласованные percentile и capacity выдержаны. | Уменьшить фильтры, ускорить consumer/storage, масштабировать или задать честный capacity limit. |
| **T4 — Canonical downstream** | Full-bank + account/config oracle, ordered effects, rehydration, repair и ML lookahead тесты доказаны отдельно. | Exact trading/ML state и live execution admission запрещены; raw аналитика может продолжаться с quality. |
| **T5 — WAL v2 production** | Все требования [WAL v2 contract](WAL_V2_CONTRACT.md), включая fault matrix, migration, retention/cursor и sustained load, подписаны. | Оставить стабильный v1; v2 — opt-in experiment. |

В downstream API/датасете каждое поле, зависящее от истории, должно нести или наследовать `quality`, source/provenance, commitment и доступные границы покрытия. При `CONTINUITY_BROKEN`, `UNVERIFIED`, конфликте A/B или неподтверждённой account/config истории строгие consumers должны **не принимать новый exact state**. Допустимы: quarantine, накопление raw evidence, bounded repair, новый явно маркированный baseline, либо degraded аналитика с недвусмысленным quality flag. Отсутствие известных проблем не превращать в `EXACT`. Формулировка `SENT`/`ACKED` где-либо в приложении означает лишь соответствующий локальный handoff/commit, не внешнюю покупку или canonical settlement.

## 9. Upstream-основания и ограничения источников

- [Solana RPC commitment](https://solana.com/docs/rpc): документирует уровни `processed`, `confirmed`, `finalized`; подтверждение консенсуса отличается от полноты Yellowstone-подписки.
- [Solana `getBlocks`](https://solana.com/docs/rpc/http/getblocks) и [RPC `getBlock`](https://solana.com/docs/rpc/http/getblock): способы получить подтверждённые произведённые слоты и содержимое блока; `processed` для этих методов не поддерживается. Это транзакционный witness, а не автоматическая полная история account writes.
- [Solana indexing/Geyser](https://solana.com/docs/payments/accept-payments/indexing): Geyser как validator streaming interface и Yellowstone как распространённая реализация.
- [Upstream rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc) и [upstream `geyser.proto`](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/geyser.proto): типы подписок, `from_slot`, `SubscribeReplayInfo`, ping и protobuf variants. Core использует собственный vendored/generated proto; проверять drift перед подключением нового provider.
- [Yellowstone upstream TypeScript reconnect example](https://github.com/rpcpool/yellowstone-grpc/blob/master/examples/typescript/README.md): пример reconnect/replay, который не заменяет source-specific coverage acceptance.

Любые provider-specific ограничения replay, retention, `from_slot`, block filter и токенов доступа нужно подтверждать на **фактическом endpoint** и записывать в протокол. Внешняя документация показывает API-возможность, но не заключает SLA с оператором Core.
