# CORE-C2-02 / CORE-C2-04 — partial bank, reconnect, source-local ordering

**Дата:** 2026-11-19. **Статус:** `LOCAL_REGRESSION_FIXED`; aggregate `G2` is `LOCALLY_VERIFIED` for the software contract. Проверки ниже — только на детерминированном loopback fake Geyser, без live provider и без independent transaction/account inclusion oracle.

## Проверяемый сценарий

Файл: `internal/ingest/c2_partial_bank_reconnect_test.go`.

`TestC2PartialBankReconnectOmissionRemainsUnverified`:

1. Fake source A, generation 1: `block_meta(510)`, `block_footer(510)`, `transaction(510,index=2)`, затем принудительный gRPC `Unavailable`.
2. Generation 2: требование `from_slot=505` (durable transaction frontier 510 минус overlap 5), повтор `transaction(510,index=2)`, затем `block_meta(511)` и `transaction(511,index=3)`. Сервер **намеренно не доставляет** ещё одну возможную транзакцию слота 510; тест не делает вывода о её отсутствии в блоке.
3. Проверяются WAL source-local append order, durable `disconnect`/`continuity_broken`, dedupe overlap, `partial_bank` issue на slot 510, sticky `CONTINUITY_BROKEN` A и независимое здоровье B.
4. Source B сообщает matching transaction slot 511 до `block_meta`; это другой receive order. Anchor A/B совпадает, однако closure/observed set comparison и все exact admission поля остаются не подтверждёнными.

**Результат:** `PASS`. Это подтверждает fail-closed coordinator quality на известном обрыве. Обнаружить невыданную провайдером транзакцию без внешнего full-block witness тест не может по определению; `from_slot` является запросом, а не доказательством полноты replay. Source-local `partial_bank` сейчас выводится из сохранённого disconnect при replay coordinator, отдельный durable `partial_bank` WAL marker Runner не пишет.

## Воспроизводимый defect и исправление

`TestC2SourceLocalSessionEndMarksOpenBankPartial`:

1. A и B подключены независимо.
2. A получает `block_meta(700)` и `block_footer(700)`; upstream bank-closure witness отсутствует.
3. Для WAL owner A передаётся source-empty `session_end` (как при graceful close/replay boundary).

**До исправления (FAIL):** A становился `Connected=false`, но `PartialBanks=0`, bank 700 оставался `Partial=false`, общий quality — `OBSERVING_UNVERIFIED`, `partial_bank` issue отсутствовал. B оставался исправным и подключённым. Прямого false `EXACT` не было, но открытый банк не маркировался частичным.

**Исправлено:** `ObserveWALRecord` для source-empty `session_end` вызывает `markOpenPartial` только для владельца WAL. Общий `ObserveRecord` для global `session_end` маркирует открытые банки всех своих источников. Новый `TestC2GlobalSessionEndDoesNotHideOpenBanks` покрывает этот путь. Все три C2 bank regression теста прошли с `-race`; независимый B не затрагивается source-local событием. Отдельная durable `partial_bank` запись для raw consumers, не пересобирающих coordinator state, остаётся открытым проектным решением.

## Команды и результаты

```text
go test ./internal/ingest -run '^TestC2PartialBankReconnectOmissionRemainsUnverified$' -count=1 -v
PASS

go test ./internal/ingest -run '^TestMultiSource' -count=1
PASS

go test ./internal/ingest -run 'TestC2' -count=1 -v
HISTORICAL BEFORE FIX: FAIL TestC2SourceLocalSessionEndMarksOpenBankPartial

GOWORK=off go test -race -count=1 -timeout=60s -run 'TestC2GlobalSessionEndDoesNotHideOpenBanks|TestC2SourceLocalSessionEndMarksOpenBankPartial|TestC2PartialBankReconnectOmissionRemainsUnverified' ./internal/ingest
PASS AFTER FIX
```

## Ограничения для release

- `CORE-C2-02`: есть deterministic A/B reorder, source-local WAL order и quality evidence; полный multi-source provider/replay/race matrix остаётся открытым.
- `CORE-C2-04`: локальный session_end blocker исправлен; поведение реального upstream `from_slot`/footer ordering и replay omission остаётся непроверенным.
- `G2`: `LOCALLY_VERIFIED` только для source-local software semantics; это не утверждает canonical tx inclusion, account/config history или ordered effects. Для admission необходим внешний свидетель полного банка/аккаунтов и отдельная downstream policy (`G3`/consumer acceptance).
