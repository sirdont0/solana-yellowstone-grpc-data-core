# Public Core — roadmap, задачи и зависимости

**Срез:** 2026-11-19 · **Репозиторий:** `solana-yellowstone-grpc-data-core` · **Статус:** C2 software durability layer и G2 локально проверены; C3-01 WAL v2 schema/format compatibility локально проверена; следующий этап — C3-02 incremental recovery. G3 provider acceptance остаётся открытым. Это roadmap **публичного универсального ядра**, а не календарный план Aggregator V2. Даты и проценты готовности не подменяют тестовые evidence.

## Приоритет и статусы

`P0` блокирует корректность или совместимость; `P1` блокирует operational acceptance; `P2` — улучшение после основных инвариантов. Статус задачи: `DONE-DOCS` (зафиксирована спецификация), `IMPLEMENTED-UNACCEPTED` (код есть, локальные проверки выполнены, но operational/provider gate не закрыт), `PARTIAL`, `OPEN`, `DEFERRED`. Доказательства с commit и tests приводятся в [RELEASE_GATES.md](RELEASE_GATES.md) и архивах аудита. Любой новый defect получает контрпример, owner и отдельный gate.

## Этапы

| Этап | Цель | Зависимости | Выходной артефакт |
| --- | --- | --- | --- |
| **C0 — Contract freeze** | Границы продукта, source contract, 4 ADR, provider и test specs | Текущая реализация и аудит | Единая точка навигации, `G0` |
| **C1 — Protocol compatibility** | Pinned protobuf, Alpenglow, реальная матрица provider capabilities | C0, credentials для live verification | Descriptor/fixtures + PV report, `G1`/`G3` |
| **C2 — Source durability** | Закрыть recovery/cursor/backpressure/fault semantics текущего raw API | C0; частично параллельно C1 | Source-local integrity и idempotency evidence, `G2` |
| **C3 — WAL v2 acceptance** | Incremental crash recovery, authoritative retention, disk budget и migration | C2 | Полный fault/load matrix и v2 release gate, `G4` |
| **C4 — Operational acceptance** | Реальная нагрузка и ресурсные границы для утверждённого provider profile | C1–C3 для включённых возможностей | Reproducible 1×/3×/10×, soak, SLO report, `G5` |
| **C5 — Release** | Версионирование, совместимость, CI/security/licensing | Все gates нужного уровня | Релиз с честным перечнем гарантий, `G6` |

Часть C1 и C2 допускает параллельную реализацию. **C2/G2 software contract locally verified** по process-crash, fault-injection, consumer ACK/idempotency и retained-byte queue evidence; это разрешает начать C3. `G3` не waived: provider replay, independent Yellowstone B и canonical coverage остаются внешними blockers и не превращаются в SLO. Физическая power-loss/media acceptance и production performance относятся к G5. WAL v1 может выпускаться отдельно от экспериментального v2 при соответствующем статусе возможностей.

## Реестр задач

| ID / приоритет | Задача | Статус на 2026-11-19 | Acceptance / зависимость |
| --- | --- | --- | --- |
| `CORE-C0-01` P0 | Зафиксировать границу Core и 4 ADR | `DONE-DOCS` | [PROJECT_SCOPE.md](PROJECT_SCOPE.md), `docs/ADR/` |
| `CORE-C0-02` P0 | Зафиксировать source/raw consumer contract и качества | `DONE-DOCS` | [SOURCE_CONTRACT.md](SOURCE_CONTRACT.md), API audit |
| `CORE-C0-03` P0 | Сформировать проверяемый downstream consumer/SLO контракт | `DONE-DOCS` | [CONSUMER_ACCEPTANCE_AND_SLO.md](CONSUMER_ACCEPTANCE_AND_SLO.md); реализация тестов отдельно |
| `CORE-C0-04` P0 | Матрица provider/commitment/filter и case IDs | `DONE-DOCS` | [PROVIDER_MATRIX.md](PROVIDER_MATRIX.md); live результаты отдельно |
| `CORE-C1-01` P0 | Зафиксировать upstream Yellowstone `.proto`, hash/descriptor, обновить generated bindings штатным скриптом | `IMPLEMENTED-UNACCEPTED` | Pin commit `be9bdc5…`, оба blob SHA, два descriptor hash, 11 structural wire kinds, `/geyser.Geyser/*` path; [C1 evidence](audits/2026-11-19-c1/PROTOCOL_COMPAT.md) |
| `CORE-C1-02` P0 | Исправить drift локального Alpenglow footer (bytes 6–8 против typed certificates 9–11 в upstream 14.0.1), проверить bank/certificate identity | `IMPLEMENTED-UNACCEPTED` | Old/new wire fixtures, optional certificates, differing certificate slot, WAL v1/v2 replay; provider/semantic validation ещё OPEN |
| `CORE-C1-03` P0 | Probe PublicNode и independent B на фактическом аккаунте/фильтрах | `PARTIAL / G3 NOT_ACCEPTED` | Ранее: dual-session PublicNode 5 мин, 1 391 block-meta, bounded TX-RPC parity 52 865 signatures. Новая [матрица G3](audits/2026-11-19-g3/REPORT.md): семь authenticated unary probes PASS; live finalized slots, transactions, transaction statuses, accounts, blocks_meta и block_footer PASS отдельно; 6 попыток from_slot overlap: 4 InvalidArgument, 2 Unavailable, 0 successful replay. B независимого Yellowstone нет; bounded public RPC `getBlocks/getBlock` probe PASS for 21 finalized slots and one full block; this is not a Yellowstone B or account/config-history witness. Combined filters, limits, retention, natural EOF and long capture OPEN. `G3` не закрыт. |
| `CORE-C2-01` P0 | Стабилизировать v1 WAL/read/replay и consumer ACK/cursor error paths | `LOCALLY_VERIFIED` | WAL v1/v2 lost ACK replay; 8 real SIGKILL consumer/DB scenarios; WAL v1 partial-frame, real fsync-complete-before-return and append-returned SIGKILL cases; four cursor temp-fsync/rename/dirsync SIGKILL boundaries; injected fsync/ENOSPC and cursor error paths. Power loss is not simulated and is deferred to G5 hardware acceptance. См. [consumer crash](audits/2026-11-19-c2/CRASH_MATRIX.md), [WAL crash](audits/2026-11-19-c2/WAL_V1_PROCESS_CRASH.md), [cursor crash](audits/2026-11-19-c2/CURSOR_PROCESS_CRASH.md). |
| `CORE-C2-02` P0 | Интеграционная проверка multi-source evidence, source-local ordering и quality | `LOCALLY_VERIFIED / LOCAL TESTS PASS` | Fake A/B reorder, disconnect, overlap dedupe, source/global session_end partial-bank handling and no false exact promotion PASS; independent live completeness oracle remains G3. См. [partial bank](audits/2026-11-19-c2/PARTIAL_BANK.md) |
| `CORE-C2-03` P1 | Доказать bounded queues, slow-sink drain, source writer p99 и fail-closed overload | `PARTIAL / BYTE BOUNDS PASS; P99/SOAK G5 OPEN` | Source writer and shared post-WAL dispatch queue byte budgets verified under blocked, context-aware sink; two durable updates replay after `ErrDrainTimeout`. Hard RSS, live writer p99, 1×/3×/10× profile and soak remain G5. См. [sink pressure](audits/2026-11-19-c2/SINK_PRESSURE.md), [Engine dispatch](audits/2026-11-19-c2/ENGINE_DISPATCH_PRESSURE.md). |
| `CORE-C2-04` P0 | Зафиксировать source-local `partial_bank` и `replay_from_slot` при disconnect, сверить поведение с upstream reconnect tests | `LOCALLY_VERIFIED / PROVIDER REPLAY OPEN` | Детерминированный mid-bank disconnect + from_slot=505 overlap + source-local partial PASS; исправлен session_end с открытым банком для WAL-scoped и global adapter. Live upstream replay и explicit raw partial_bank marker remain G3/provider scope. См. [evidence](audits/2026-11-19-c2/PARTIAL_BANK.md). |
| `CORE-C3-01` P0 | Завершить v2 binary-envelope/schema compatibility policy | `LOCALLY_VERIFIED` | Frozen v2.0 byte layout; independent physical/envelope versions; pinned descriptor digests; strict unknown-field policy; persistent DataDir format marker; immutable byte fixtures; legacy reopen and v1→v2 migration tests. См. [C3 compatibility audit](audits/2026-11-19-c3/WAL_V2_COMPATIBILITY.md). Остальной `G4` остаётся открыт. |
| `CORE-C3-02` P0 | Crash-time incremental recovery + exact disk dedupe frontier | `PARTIAL` | O(unrecovered tail) целевой путь с полным fallback и неизменной семантикой |
| `CORE-C3-03` P0 | Authoritative recovery baseline, ACK-aware retention/compaction | `OPEN` | Удаление реальных sealed segments без нарушения cursor/recovery; `G4` |
| `CORE-C3-04` P0 | Полная fault injection матрица WAL v2, ENOSPC и uncertain commits | `PARTIAL` | Восстановление после каждого crash-window и отсутствие false durable callback |
| `CORE-C4-01` P1 | Provider/end-to-end нагрузочные профили, 1×/3×/10× и 24h soak | `OPEN` | SLO profiles, reproducible metrics, admitted/rejected events accounting; `G5` |
| `CORE-C4-02` P1 | Recovery time/catch-up/retention budget и resource alarm semantics | `OPEN` | Recovery objective и saturation policy по профилю с fault evidence |
| `CORE-C5-01` P1 | Release, compatibility/security/licensing/checklist | `OPEN` | CI, race/vet, dependency & secrets scan, migration notes, `G6` |
| `CORE-FUTURE-01` P2 | Rust ingest A/B benchmark или sidecar | `DEFERRED` | Только после measured Go bottleneck и parity на идентичных fixtures |

## Технический долг и критерии закрытия

**TD-01 (recovery):** verified v2 cache полезен после clean close, но общий crash-path может повторно сканировать/хешировать всю историю или rebuild dedupe. Закрывает `CORE-C3-02`: сравнение ускоренного reopen и полного replay на одних fixtures при изменённых/corrupt индексах и разрывах питания.

**TD-02 (retention):** архивный журнал ещё нельзя удалять по одному consumer ACK или disposable cache. Закрывает `CORE-C3-03`: manifest-bound authoritative baseline, минимальный зарегистрированный ACK, expiration errors, crashtests удаления.

**TD-03 (provider):** `from_slot`, `SubscribeReplayInfo`, разрешённые фильтры и ограничения различаются. Закрывают `CORE-C1-03` и повторяемая matrix на целевых endpoint.

Кандидаты и ограничения сведены в [provider research](audits/2026-11-19-g3/PROVIDER_CANDIDATES.md): Alchemy документирует `from_slot`, но его официальные страницы расходятся по replay window (6 000 vs 432 000 slots); QuickNode документирует около 3 000 slots; Sodae предлагает trial 5 GB/7 дней без публично документированного replay; Tatum и Chainstack публикуют Yellowstone endpoints. Ни один B credential или source failure domain пока не проверен, поэтому `CORE-C1-03/G3` **не** меняет статус.

**TD-04 (protocol):** локальные protobuf и Alpenglow обновлены и закреплены на upstream 14.0.1; `CORE-C1-01/02` прошли локальную проверку схемы/проводных полей. Остаются фактические provider capabilities, реальная сертификатная семантика и контроль будущего schema drift; см. `CORE-C1-03`, `G3` и [протокол проверки](audits/2026-11-19-c1/PROTOCOL_COMPAT.md).

**TD-05 (performance):** синтетические WAL benches не доказывают provider/network/sink SLO или bounded RSS. Закрывают `CORE-C2-03`, `CORE-C4-01/02`.

**TD-06 (cross-source exactness):** Core intentional-only evidence, без canonical block/account completeness. **Не закрывается добавлением магического `exact` в Core.** Downstream consumer строит независимый acceptance; см. [SOURCE_CONTRACT.md](SOURCE_CONTRACT.md) и [AGGREGATOR_V2_ROADMAP.md](AGGREGATOR_V2_ROADMAP.md).

## Связь с Aggregator V2

Существующие фазы Aggregator V2 `P0–P8` не перенумеровывать. На момент предыдущей проверки `V2 P0 = PASS` для аудита контрактов, `V2 P1 = BLOCKED` по source/account coverage, `V2 P2 = PARTIAL` по decoder/effect coverage. Новые `CORE-C*` — **другие идентификаторы**. Consumer может тестировать синтетический state до канонического admission, но не выпускать exact trading/ML result без своего доказательства покрытия. Обоснование Go-first, lifecycle и границы Rust описаны в [AGGREGATOR_V2_ROADMAP.md](AGGREGATOR_V2_ROADMAP.md).

## Порядок работ над задачей

Для каждого ID: (1) сформулировать инвариант и тест, который до изменения не проходит; (2) ограничить scope/API и сохраняемую совместимость; (3) внедрить по одному слою; (4) прогнать fault/replay tests; (5) приложить metric fixtures и документировать ограничения; (6) изменить статус **только после реального evidence**. Результаты старых аудитов хранятся неизменно как история.
