# Source / Raw Consumer Contract

**Статус:** согласованная спецификация публичного интерфейса; описание уже реализованного отделено от будущих гарантий.
**Профиль:** `finalized` для базового raw capture и downstream deterministic state.
**Дата:** 2026-11-19.

Нормативные слова **MUST**, **MUST NOT**, **SHOULD** обозначают требование для следующей принимаемой версии; они не означают, что каждая возможность уже реализована. Текущее поведение подтверждается `engine.go`, `raw.go`, `internal/ingest/*` и [ARCHITECTURE.md](ARCHITECTURE.md). При расхождении с кодом создаётся задача в [ROADMAP.md](ROADMAP.md), а не молчаливое обещание.

## 1. Уровни гарантий

| Уровень | Кто выдаёт | Что разрешено утверждать | Чего он не доказывает |
| --- | --- | --- | --- |
| `SOURCE_OBSERVED` | Yellowstone receive path | Провайдер прислал наблюдение с provenance | Источник увидел все события |
| `SOURCE_DURABLE` | Core после успешного WAL commit | Запись сохранена в локальном журнале; её можно прочитать по локальному курсору | Данные есть в других источниках или каноничны |
| `SOURCE_CORROBORATED` | Внешний consumer/evidence | Совпали конкретные наблюдаемые идентичности/anchors A/B | Полное включение в банк |
| `CANONICAL_ADMITTED` | Downstream admission с независимым witness | Проверена достаточная для указанного поля/среза каноническая история | Полнота всех аккаунтов/цен без отдельных проверок |
| `APPLICATION_COMMITTED` | Транзакция consumer DB | Применённый эффект приложения зафиксирован с idempotency key | Удалённый вызов или Core ACK был атомарен с этой транзакцией |

`CANONICAL_ADMITTED` и `APPLICATION_COMMITTED` — **логические состояния следующего слоя, они не являются текущими enum/API Core**. Нельзя переводить `SOURCE_DURABLE` или совпадение двух источников непосредственно в `EXACT`.

## 2. Источники, идентичность и последовательность

`Source.ID` MUST быть стабильным в пределах жизненного цикла журнала; `FailureDomain` MUST описывать реальную общую инфраструктуру, без предположения о независимости только по числу endpoint. `FilterVersion` описывает логическую подписку; полный нормализованный Yellowstone filter, commitment и endpoint входят в source identity. Изменение подписки/совместимости требует нового data root или явной миграции, иначе WAL должен отклонить запись.

Физический курсор `RawCursor` — `Source`, `Identity`, `JournalID`, `Sequence`; `ConsumerEpoch` дополнительно защищает регистрацию ACK от устаревших подтверждений. `Sequence` растёт внутри одного source WAL. Из последовательности A нельзя вывести порядок B. Новое создание WAL MUST менять `JournalID`; старые курсоры MUST завершаться явной ошибкой (`ErrCursorExpired`), а не перепрыгивать на новый журнал.

`Generation` обозначает сеанс подключения и меняется при reconnect. Source-local `(slot, bank_id)` различает варианты банка только там, где provider доставил `bank_id`; отсутствие поля MUST сохраняться (`BankIDPresent=false`) и не заменяться выдуманным нулевым «общим» банком. `bank_id` и `write_version` разных валидаторов не являются общесетевыми идентификаторами.

## 3. Raw envelope и точки доставки

Реальный публичный `RawObservation` содержит `Version`, `Cursor`, `Type`, `Source`, `Generation`, `Commitment`, `FilterVersion`, `Identity`, `Kind`, `Slot`, при наличии `BankID`/`BankIDPresent`, `TxIndex`, `Signature`, `Account`, `WriteVersion`, `ReceivedAt`, `SourceCreatedAt`, `Digest`, `Quality`, `Payload` и отдельные признаки статуса/fee. Поля с `omitempty` могут отсутствовать; downstream MUST проверять `Kind` и признаки присутствия до интерпретации нуля. `Payload` — Yellowstone protobuf update, повторно сериализованный после gRPC decode, **не исходный wire frame TCP/gRPC**.

`NewRaw`/`RawReader.Next(ctx, limit)` выдают **уже fsync-нутые** source-local записи и lifecycle markers. Для decoded handlers есть `Provisional` до fsync и `Durable` после fsync. Эти флаги определяют durability момента dispatch; chain commitment задаётся `Source.Commitment`. В обычном handler path допускается `finalized`; `processed`/`confirmed` доступны raw API как несогласованные локальные наблюдения.

## 4. Consumer API и транзакционная граница

Рекомендуемая последовательность для каждого `(consumer, source)`:

1. `RegisterConsumer` с явным start policy (новый пользователь выбирает позицию осознанно), затем `OpenRawReaderFromConsumer` или `OpenRawReader` с ранее проверенным курсором.
2. Чтение bounded batch через `Next(ctx, limit)`; запись каждого наблюдения под idempotency key `(source, identity, journal_id, sequence)` в **одной транзакции consumer DB** вместе с его эффектами. При повторе с тем же ключом и другим digest — ошибка и остановка.
3. После коммита приложения выполнить `Ack(consumer, cursor)` для соответствующего source. `Ack` монотонен, fsync-нут, защищён consumer epoch; он **не** является частью транзакции внешней БД.
4. При crash между application commit и ACK повторно доставленная запись MUST быть no-op на уровне idempotency table. Новая generation/reconnect не даёт права пропускать историю. Медленный consumer отслеживает `ConsumerLag`, возраст старейшей записи и backpressure.

`ReadRaw` предназначен для offline catch-up. При открытом live `Run` следует использовать `OpenRawReader`. `ErrCursorExpired`, `ErrCursorRegression`, `ErrConsumerUnregistered`, ошибка integrity WAL, несовместимая subscription identity и зафиксированная `continuity_broken` не переводятся в «успешную синхронизацию».

Существующий runnable пример — [`examples/rawconsumer`](../examples/rawconsumer/). Он использует bbolt consumer DB и idempotent запись до отдельного Core ACK. Для внешнего side effect (например, HTTP/trade) нужен собственный transactional outbox либо idempotent downstream protocol.

## 5. A/B, банк, порядок и полнота

Вход от A и B записывается независимо, включая источники с одинаковым `failure_domain`. Равенство `slot` или source-local `bank_id` недостаточно для объединения. Для finalized-canonical применения consumer должен подтвердить chain anchor/parent и идентичность блока, получить независимое подтверждение **включения** требуемых транзакций, затем упорядочить их по транзакционному и внутритранзакционному порядку. Signature без позиции не является достаточным ключом полного порядка экономических эффектов.

`MultiSourceCoordinator` уже хранит `OBSERVING_UNVERIFIED`, `CONTINUITY_BROKEN`, `CONFLICT` и evidence snapshots; он специально не продвигает `TxInclusionVerifiedThrough`, `AccountHistoryVerifiedThrough` или ordered effects admission в готовое состояние без внешнего oracle. `block_footer` может прийти до остальных обновлений банка и должен трактоваться как свидетельство, **не barrier всех updates**. У filtered потока `101→103→109` допустимо при отсутствии совпадающих транзакций в пропущенных слотах.

Для прикладного агрегатора контроль покрытия MUST разделять хотя бы `TX_INCLUSION`, `ACCOUNT_HISTORY`, `CONFIG_HISTORY`, `PRICE` и `ORDERED_EFFECTS` с отдельными диапазонами, источниками доказательства и degrade reasons. `getBlock` может помочь проверить/восстановить tx inclusion, но не автоматически все account writes. RPC account reads с `minContextSlot` сами по себе не создают атомарный snapshot нескольких аккаунтов на одном банке. Восстановление, не закрывшее историю, создаёт явный новый coverage epoch/baseline.

## 6. Ошибки, ресурсные пределы и наблюдаемость

Core MUST fail closed либо сохранить durable quality/continuity marker при необратимом WAL/dedupe конфликте, повреждении committed данных, неполной восстановленной истории или несовместимости подписки. Истёкший provider `from_slot` — факт неудачной попытки recovery, а не основание скрытно перейти на head; явный `AllowLiveFallbackOnReplayRejection` допустим только с отмеченным пробелом и политикой consumer.

Минимальные SLI: updates/s и MiB/s по источнику и виду update; receive→durable p50/p95/p99; oldest queued age; dispatch backlog; WAL bytes/day и fsync duration; consumer ACK lag; reconnects и replay overlap; RAM/RSS, CPU, GC; delayed/conflicting observations; количество и диапазоны continuity issues. Квантильные определения, нагрузочные профили, тесты восстановления и acceptance — в [CONSUMER_ACCEPTANCE_AND_SLO.md](CONSUMER_ACCEPTANCE_AND_SLO.md).

## 7. Совместимость

Любое изменение protobuf, raw envelope, cursor, digest/key semantics, commitment/filter identity или WAL layout должно проходить schema/fixture compatibility tests. Возможности `from_slot`, `SubscribeReplayInfo`, блоков и аккаунтов проверяются отдельно для каждого provider/commitment/filter profile; факт наличия метода в `.proto` не означает поддержку сервером. Смотри [PROVIDER_MATRIX.md](PROVIDER_MATRIX.md).
