# CORE-C2-01 — начало durable consumer recovery regression

**Дата:** 2026-11-19. **Статус:** `IMPLEMENTED-UNACCEPTED`; `G2` не закрыт.

Начата параллельная фаза C2 после bounded C1 provider evidence. При этом `CORE-C1-03` остаётся `PARTIAL`, `G3` — `NOT_ACCEPTED`: пять минут двух PublicNode-сессий не заменяют проверку `from_slot` и второго независимого Yellowstone-провайдера.

Добавлен `TestArchiveReplayAcrossLostAckAndDurableAck` в `raw_archive_cursor_epoch_test.go`. Проверяются оба формата WAL v1/v2 по публичному `ReadRawArchive` и `Engine.Ack`:

1. WAL с несколькими записями прочитан consumer с зарегистрированным epoch.
2. После имитированной границы application commit перед Core ACK durable cursor остаётся прежним; повторный archive read доставляет те же записи и курсоры.
3. После durable ACK следующая выборка начинается строго после ACK; старый ACK возвращает `ErrCursorRegression`.

Это **детерминированный тест границы ACK**, а не настоящий kill/power-loss process crash или гарантия exactly-once внешнего side effect. Application-level idempotence отдельно проверяется в `examples/rawconsumer/database_test.go`. Следующая работа по `CORE-C2-01`: process-crash матрица вокруг `fsync`/ACK и consumer DB commit, ошибки записи cursor, durable reopening и bounded API failure semantics. До её завершения `G2` остаётся `PARTIAL`.
