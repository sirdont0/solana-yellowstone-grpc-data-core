# Upstream-источники и кандидаты для переиспользования

**Срез исследования:** 2026-11-19. Ссылки ниже — источники и кандидаты для изучения. Их наличие **не означает**, что код импортирован, тестирован в Core или совместим с нужным provider. При реализации фиксировать конкретный Git SHA/tag, descriptor checksum, лицензию файлов и тестовые fixtures; ссылки на `master` сами по себе не являются immutable pin.

## Протокол и нормативные основания

| Источник | Назначение | Что закрепить в нашем проекте |
| --- | --- | --- |
| [Yellowstone gRPC — repository](https://github.com/rpcpool/yellowstone-grpc) | Базовый протокол, сервер/клиенты, примеры | Точный релиз proto/client и backend; не копировать AGPL-only код без legal review |
| [Upstream `geyser.proto`](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/geyser.proto) | `SubscribeRequest/Update`, `SubscribeReplayInfo`, `from_slot`, footer, bank metadata | Wire fixtures на старых/новых версиях, неймспейс и совместимость service path |
| [Yellowstone changelog](https://github.com/rpcpool/yellowstone-grpc/blob/master/CHANGELOG.md) | История reconnect и protobuf | Проверить изменения Alpenglow сертификатов и `ReconnectStream`, включая partial-bank behaviour |
| [Yellowstone licensing](https://github.com/rpcpool/yellowstone-grpc/blob/master/LICENSING.md) | Границы AGPL-3.0-only и Apache-2.0 по subtrees | Проверять **лицензию конкретного исходного файла** перед заимствованием |
| [Solana Alpenglow](https://solana.com/uk/upgrades/alpenglow) | Новая consensus/finality модель, block footer и certificates | Не трактовать footer как полноту банка; измерить provider latency самостоятельно |
| [Solana `getBlocks`](https://solana.com/docs/rpc/http/getblocks) · [`getBlock`](https://solana.com/docs/rpc/http/getblock) | Независимые RPC-свидетели произведённых блоков и tx inclusion | Ограничение: не восстановят автоматически полный account/config history |
| [Solana indexing / Geyser](https://solana.com/docs/payments/accept-payments/indexing) | Экосистема индексирования и потоки Geyser | Подтверждать применимость к заявленному filter/commitment профилю |
| [Go GC Guide](https://go.dev/doc/gc-guide) | Методы профилирования Go/GC | Бенчмарки на одинаковом input до возможного Rust port |

### Проверенная точка риска Alpenglow

**Риск устранён локально в C1 (2026-11-19):** прежняя схема `proto/solanacore-geyser.proto` представляла сертификаты `SubscribeUpdateBlockFooter` как `bytes` в полях 6–8, а upstream `yellowstone-grpc-proto 14.0.1` использует типизированные сообщения полей 9–11 и резервирует 6–8. Теперь схема/Go bindings обновлены по commit `be9bdc5b53b5b08c31a4da8c6dd7b68c0b2d4e2f`, что проверено old/new wire fixtures, descriptor pins и WAL v1/v2 replay. **Остаётся реальная provider acceptance и семантическая проверка сертификатов**, включая их отдельный slot/block identity. Нельзя трактовать `block_footer` или сертификат сам по себе как полный bank witness. См. [PROTO_NOTICE.md](../PROTO_NOTICE.md) и [C1 evidence](audits/2026-11-19-c1/PROTOCOL_COMPAT.md).

По upstream Yellowstone Rust client `ReconnectStream` 17.0.0 (30 октября 2026) возвращает явный `Disconnected` с `partial_banks`/`replay_from_slot`. Для Go Core это полезная **модель тестирования**, а не требование подключить Rust-клиент: reconnect остаётся source-local, а успешный replay не доказывает account completeness. Точное поведение зависит от backend и проверяется по [PROVIDER_MATRIX.md](PROVIDER_MATRIX.md).

## Проекты для исследования, без принятого решения о внедрении

| Проект | Идеи и возможная роль | Условия применения |
| --- | --- | --- |
| [Yellowstone Block Machine](https://github.com/rpcpool/yellowstone-block-machine) | Bank-aware buffering, incomplete/dead banks, deterministic ordering, fixture design | Проверить assumptions о commitment, bank lifecycle и лицензии каждого файла; не считать drop-in A/B reconciler |
| [Richat](https://github.com/lamports-dev/richat) | Geyser stream/fan-out архитектура и транспортные компромиссы | Измерить и проверить реально нужный функционал перед интеграцией |
| [Carbon](https://github.com/sevenlabs-hq/carbon) | Декодирование инструкций и удобная модель downstream program decoders | Использовать как reference для отдельного decoder модуля, не переносить экономическую логику в raw Core |
| [Shipstern](https://github.com/solana-rpc/shipstern) | Наблюдение за bank/commitment и архитектура RPC/indexing | Проверить актуальность, исходные допущения и лицензию |
| [Solana Microscope](https://github.com/solana-foundation/solana-microscope) | Инструменты понимания/проверки структуры транзакций | Тестовые fixtures и независимый debugging, не completeness oracle автоматически |
| [Light RPC](https://github.com/sodae-io/light-rpc) | Разделение streaming и query, indexing/storage | Только downstream идея; адаптация после сравнения лицензии и интерфейса |
| [Pump public docs / IDLs](https://github.com/pump-fun/pump-public-docs/tree/main/idl) | Официальные IDL для Pump.fun/PumpSwap | Исключительно downstream decoders; pin version и test against on-chain fixtures |

## Правила заимствования и проверки

Собственный код Core распространяется под MIT; адаптированные Yellowstone protobuf/bindings находятся под Apache-2.0 по [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md). Общая upstream-лицензия Yellowstone отличается от лицензии отдельных proto/client subtrees. Не копировать файлы из других репозиториев по одному названию лицензии проекта: проверить конкретный путь, attribution, downstream distribution requirements и соответствие host API.

Прежде чем принять dependency: записать лицензию и pinned revision; описать требуемый контракт и test fixture; измерить CPU/RSS/latency/complexity на идентичном capture; сделать negative/restart tests; указать условия отката. Для идеи Go→Rust потребитель остаётся привязан к версионированному observation protocol, а не к Rust-specific in-memory структуре. Ранее согласованное направление Go-first сохраняется.
