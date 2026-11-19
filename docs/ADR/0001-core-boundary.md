# ADR-0001 — Универсальная граница Core

**Статус:** принято · **Дата:** 2026-11-19 · **Область:** публичный Data Core.

## Контекст

Pump.fun агрегатор потребляет on-chain данные, но публичному Yellowstone Core должны подходить произвольные программы, аккаунты, блоки и слоты. Встраивание экономических reducer в транспорт усложнило бы поддержку протокола и вынудило бы ядро обещать полноту данных, которой само подключение не доказывает.

## Решение

Core предоставляет source configuration/identity, gRPC ingest, reconnect, WAL, raw observations, локальные курсоры/ACK, replay, явное качество и наблюдаемость. Handler/CPI decoder остаётся опциональным API. Canonical bank admission, прикладные reducers, account/config bootstrap, RPC/SQD repair, торговые и ML компоненты располагаются за границей библиотеки.

## Следствия и проверка

`NewRaw` не зависит от Pump.fun decoder и позволяет сохранять подписанные типы Yellowstone updates. Тесты Core проверяют source-local инварианты. Полнота бизнес-стейта тестируется в потребителе. Общие форматы наблюдений и ошибки задаёт [SOURCE_CONTRACT.md](../SOURCE_CONTRACT.md); границы релиза — [RELEASE_GATES.md](../RELEASE_GATES.md).
