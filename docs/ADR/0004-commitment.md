# ADR-0004 — Finalized как основной профиль и отдельный low-latency режим

**Статус:** принято · **Дата:** 2026-11-19.

## Контекст

Для экономических агрегаторов особенно важны стабильный порядок и возможность точного replay. Обработка `processed` требует отслеживать конкурирующие банки, abandoned/dead branches и откат speculative state. Фактические задержки до `finalized` зависят от цепочки, валидатора, провайдера и сети; цифра финализации консенсуса не является SLO конечного consumer.

## Решение

Базовый профиль downstream state использует `finalized` и отдельную проверку полноты. Текущий decoded handler API допускает только `finalized`. Raw API может записывать `processed` и `confirmed` как source-local evidence, но Core не объединяет их в канонический state. Прототип низколатентного `PROVISIONAL` consumer возможен отдельно, с явными `apply/revert` или версионированной заменой state и тестами forks; он не входит в текущие гарантии Core. `Provisional`/`Durable` в `engine.go` обозначают момент **доставки handler относительно fsync**, а не chain commitment.

## Проверка

Сравнить provider-observed latency, fsync latency и consumer-known latency на одинаковой выборке. Для `finalized` выполнить replay parity и качество покрытия; для будущего speculative режима — fork/dead slot/rollback и отсутствие двойных экономических эффектов. Ни одна версия не утверждает фиксированные «150 мс end-to-end» без измерений. См. [CONSUMER_ACCEPTANCE_AND_SLO.md](../CONSUMER_ACCEPTANCE_AND_SLO.md).
