# ADR-0003 — Политика WAL v1/v2

**Статус:** принято · **Дата:** 2026-11-19.

## Контекст

`YLABWAL1` — действующий single-file формат с JSON envelope и base64 protobuf payload. Экспериментальный `WALVersion=2` использует сегменты, CRC32C, отдельный binary raw payload, JSON metadata, checksum/manifest и индексы. У v2 уже есть тесты ротации и восстановления и verified recovery cache после clean close, но остаются открыты общая incremental crash recovery, контракт authoritative retention, полная crash/fault матрица и sustained-load acceptance.

## Решение

`WALVersion=0/1` остаётся значением по умолчанию. `WALVersion=2` включается только явно; переход v1→v2 выполняется копированием в новый root через проверяемую миграцию. Автоматического in-place переключения и повторного использования старых consumer ACK в новом journal нет. Удаление WAL-сегментов разрешается только после отдельного authoritative recovery/retention контракта, защиты минимального поддерживаемого consumer ACK и тестов удаления/краш-восстановления. Snapshot/cache и sparse index сами по себе не позволяют удалять историю.

## Проверка

Тестировать совместимость чтения/миграции, old/new journal ID, crash до/после fsync/manifest/CURRENT, torn active tail, sealed corruption, ENOSPC, cursor expiration и retention при медленном потребителе. Решение о смене default приниматься только после прохождения [WAL_V2_CONTRACT.md](../WAL_V2_CONTRACT.md) и [RELEASE_GATES.md](../RELEASE_GATES.md), с измеренным профилем диска/latency.
