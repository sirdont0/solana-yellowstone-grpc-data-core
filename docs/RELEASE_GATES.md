# Core acceptance и release gates

**Дата:** 2026-11-19 · **Текущий `dev`: experimental**. Этот документ разделяет четыре разных факта: код существует (`IMPLEMENTED`), локально проверен (`LOCALLY_VERIFIED`), проверен на выбранном provider/runtime (`PROVIDER_VERIFIED`) и принят для заявленного эксплуатационного профиля (`ACCEPTED`). Статус старого аудита не повышается автоматически из-за новой документации.

## Нормативные правила

- Для каждой завершённой задачи нужны проверяемый артефакт (test/fixture/report), SHA/версия кода, входной профиль, точный результат и известные ограничения. Непроведённый тест = `NOT_RUN`, не `PASS`.
- Quality/continuity loss, corruption, stale cursor, source identity conflict или невосстановленная история MUST приводить к явной ошибке/degraded mode; запрет на ложный `EXACT` — безусловный gate.
- Нагрузочный SLO задаётся для конкретного hardware/provider/filter/retention/consumer профиля по [CONSUMER_ACCEPTANCE_AND_SLO.md](CONSUMER_ACCEPTANCE_AND_SLO.md), с методикой, percentiles и запретом молчаливой потери. Числа из синтетического теста не являются обещанием live throughput.
- `finalized` и WAL fsync не доказывают canonical completeness. Core release не зависит от реализации экономического reducer, но обязан честно выдавать provenance, quality и failure semantics.

## Gates для публичного Core

| Gate | Входные условия | Проверка / артефакт | Статус на дату создания |
| --- | --- | --- | --- |
| **G0 — Spec** | Согласованные границы/ADR, source contract, provider matrix, testing/SLO | Cross-links, API/code review, нет противоречивых гарантий | **Документируется**, ещё не закрыт проверкой |
| **G1 — Protocol** | Pinned upstream `.proto` + descriptor digest, миграция/generated code | Wire fixtures со старыми/новыми версиями; required update kinds, Alpenglow footer, unknown fields | **LOCALLY_VERIFIED**: schema/structural-wire/WAL replay tests; [C1 evidence](audits/2026-11-19-c1/PROTOCOL_COMPAT.md). Нет live provider proof |
| **G2 — Durable v1 API** | Source identity, WAL v1, ACK, replay, fail-closed | Unit/race/crash/fault/consumer idempotence, source-local parity, bounded retained-byte queues | **LOCALLY_VERIFIED (software contract)**: 8 SIGKILL consumer/DB cases (v1/v2); WAL v1 partial-frame and real-fsync-before-return SIGKILL cases; four cursor temp-fsync/rename/dirsync SIGKILL boundaries; injected fsync/ENOSPC errors; source-local partial-bank repair; bounded source + shared dispatch queues with durable replay after `ErrDrainTimeout`. Physical media exhaustion, power-loss/controller-cache behavior and deployment RSS/SLO are explicitly excluded from G2 and remain G5 hardware-profile acceptance. A sink that ignores its context remains a consumer contract violation. См. [C2 integration](audits/2026-11-19-c2/INTEGRATION_REPORT.md), [WAL crash](audits/2026-11-19-c2/WAL_V1_PROCESS_CRASH.md), [cursor crash](audits/2026-11-19-c2/CURSOR_PROCESS_CRASH.md) и [dispatch pressure](audits/2026-11-19-c2/ENGINE_DISPATCH_PRESSURE.md). |
| **G3 — Provider** | Проверенный primary и, если используется, secondary | PV-01…PV-10 на реальных endpoint; retained evidence и degraded handling | **PARTIAL / NOT_ACCEPTED**: bounded dual PublicNode + independent RPC TX parity PASS; new 7 authenticated PublicNode probes show 6 failed from_slot overlap attempts (4 InvalidArgument, 2 Unavailable); second independent Yellowstone B NOT_CONFIGURED. A separate public JSON-RPC `getBlocks/getBlock` bounded probe PASS demonstrates a possible transaction/block repair primitive, but not sustained backfill, account/config history or attested provider failure-domain independence. Combined stream/quotas/retention and long coverage OPEN. См. [G3 matrix](audits/2026-11-19-g3/REPORT.md) and [provider candidates](audits/2026-11-19-g3/PROVIDER_CANDIDATES.md). |
| **G4 — WAL v2 opt-in** | Schema freeze, durable committed manifest, recovery, retention decision | Crash matrix + index/checkpoint parity + actual retention/delete/restart + disk bounds | **PARTIAL**: C3-01 schema/format freeze LOCALLY_VERIFIED; incremental crash recovery, ACK-aware retention/compaction, full fault matrix and disk budget remain open; v2 stays experimental |
| **G5 — Operational SLO** | Утверждённая измеряемая нагрузка, hardware и пороги, 1× baseline | 1× sustained, 3×/10× burst/overload policy, 24h soak, p50/p95/p99, RSS, CPU, disk, backlog/recovery | **OPEN** |
| **G6 — Release** | Нужный набор G0–G5 по заявляемым возможностям | Clean build, race, vet, secrets/license audit, reproducible release notes и rollback/migration | **OPEN** для production claim |

`G4` обязателен для утверждений о production WAL v2 и удаления истории, но **не должен блокировать** исправления/исторические релизы v1, если v2 остаётся явно экспериментальным. Отдельный релиз возможен как «experimental source release» с честными ограничениями; надпись `production-grade` требует закрытия соответствующего SLO/provider профиля.

## Правила измерения и отказов

`G2` is a software durability/API gate: it proves process-crash recovery,
error propagation, consumer commit/ACK behavior, source-local ordering, and
configured retained-byte bounds on the tested OS/filesystem APIs. It does not
claim that a specific disk/controller preserves acknowledged writes through
power loss. Physical media exhaustion, volatile device-cache behavior, hard
RSS, throughput and latency are deployment-profile questions for `G5`, where
the actual host, filesystem, storage controller and power-loss protection must
be identified and tested. A process `SIGKILL` must never be described as a
power-loss simulation.

Базовые тесты используют записанные protobuf updates с зафиксированными фильтрами и SHA fixtures, отдельные профили 1× наблюдаемой нагрузки и 3×/10× пикового входа. Для недопустимой нагрузки приемлем явно зарегистрированный backpressure/terminal fail-closed в пределах заранее заданного resource budget; **неприемлемо** молча отбросить данные или повысить quality. Версия decoder и seed/timing fault injection должны быть воспроизводимы.

Проверяются crash до/после WAL `fsync`, atomic manifest/CURRENT, partial write, `ENOSPC`, повреждение sealed segment, stale/corrupt index/snapshot, отмена с медленным sink, reconnect с replay-отказом, A/B disagreement, stale ACK, consumer commit-before-ACK и новый журнал с тем же sequence. Неуспех проверки должен оставлять чёткий контрпример и blocker в [TECH_DEBT.md](TECH_DEBT.md) / [ROADMAP.md](ROADMAP.md).

## Свидетельства и отчётность

Каждый отчёт должен содержать `core_commit`, `go_version`, `provider_profile`, `subscription_hash`, `wal_version`, `fixture_hashes`, `host_resources`, `run_duration`, `offered/accepted/persisted/read records`, `bytes`, `p50/p95/p99`, ошибки/quality transitions, recovery/catch-up и список `PASS/FAIL/NOT_RUN`. Отдельно указывать время события цепочки, получение от провайдера, durable и downstream known time: backfilled события нельзя относить к прежней доступности live модели.

Исторический baseline: [AUDIT_VERIFICATION_2026-11-17.md](AUDIT_VERIFICATION_2026-11-17.md), [RECOVERY_CHECKPOINT.md](RECOVERY_CHECKPOINT.md), [WAL_V2_CONTRACT.md](WAL_V2_CONTRACT.md). Конкретные владельцы/зависимости — [ROADMAP.md](ROADMAP.md).
