# ADR-0002 — Независимые потоки и межисточниковое согласование

**Статус:** принято · **Дата:** 2026-11-19.

## Контекст

Провайдеры A/B могут по-разному задерживать, упорядочивать и восстанавливать одинаковые Yellowstone updates. Два подключения одного оператора могут зависеть от одного backend и не представляют независимые failure domains. Разница между временем получения событий не является ошибкой Solana или признаком пропуска.

## Решение

Каждый источник имеет свой `Source.ID`, подписку/`FilterVersion`, `FailureDomain`, generation и WAL. Последовательность `(source, identity, journal_id, sequence)` упорядочена **только внутри источника**. Core хранит provenance и evidence; `MultiSourceCoordinator` сравнивает наблюдаемое, но не выпускает сертификат canonical completeness. Межисточниковое объединение выполняет отдельный consumer по подтверждённому blockhash/parent/hash цепочки, индексу транзакции и signature с независимым inclusion witness; `bank_id` и `write_version` из разных backend не сравниваются напрямую.

У filtered subscriptions последовательность слотов может быть разреженной. `block_meta`, `block_footer`, `finalized` и совпадение наблюдаемых транзакций A/B не доказывают отсутствия пропусков. При неизвестной истории consumer обязан выставить `UNKNOWN`/degraded state, отложить exact admission или создать документированный новый baseline; он не восстанавливает несуществующее свидетельство.

## Проверка

В тестах A/B меняется порядок получения, один провайдер задерживается/обрывается, один выдаёт конфликтующие сведения, оба используют общий backend и затем независимый. Результат Core — два корректных WAL, provenance, quality flags и отсутствие ложного `EXACT`. Результат consumer после внешней проверки — один детерминированный ordered effects log либо явный отказ в canonical admission. См. [SOURCE_CONTRACT.md](../SOURCE_CONTRACT.md) и [CONSUMER_ACCEPTANCE_AND_SLO.md](../CONSUMER_ACCEPTANCE_AND_SLO.md).
