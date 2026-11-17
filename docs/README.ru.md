# Solana Yellowstone gRPC Data Core — Русский

[← Главная](../README.md) · [🇬🇧 English](README.en.md) · [🇷🇺 Русский](README.ru.md) · [Архитектура](ARCHITECTURE.md) · [План Aggregator V2](AGGREGATOR_V2_ROADMAP.ru.md) · [Технический долг](TECH_DEBT.md) · [Участие](../CONTRIBUTING.md) · [Безопасность](../SECURITY.md)

## Обзор

Solana Yellowstone gRPC Data Core — встраиваемая Go-библиотека для получения и обработки наблюдаемых транзакций Solana и CPI-инструкций напрямую внутри приложения. Она рассчитана на агрегаторы, торговых ботов, индексаторы и аналитические сервисы. Сетевой и журналирующий фундамент не зависит от конкретной экономической модели токена.

**Что реализовано:** независимо переподключающиеся Yellowstone-источники, информация об источниках и разрывах непрерывности, ограниченные очереди, WAL с CRC и пакетным fsync, декодирование транзакций с разрешением ALT, точные позиции внешних инструкций и CPI, базовая дедупликация между источниками, реестр обработчиков и повторная обработка сохранённого WAL.

**Чего в ядре нет:** подсчёта холдеров, расчёта цен и ликвидности, отправки торговых операций, ML-признаков и автоматического RPC/SQD-восстановления. Эти функции остаются на прикладном уровне Aggregator V2 и других приложений.

**Статус:** экспериментальная версия v0. Даже при finalized события остаются наблюдениями; ядро пока не доказывает полноту банков и истории аккаунтов/конфигурации. У событий качество OBSERVING_UNVERIFIED либо CONTINUITY_BROKEN.

## Быстрый старт

Нужны Go 1.26.11+ и доступ к Yellowstone gRPC (для PublicNode по умолчанию необходим токен провайдера).

    git clone https://github.com/sirdont0/solana-yellowstone-grpc-data-core.git
    cd solana-yellowstone-grpc-data-core
    cp .env.example .env
    cp config.example.json config.json

В **.env** укажи endpoint, токен и заголовок авторизации первого провайдера. Если токен не нужен — оставь его пустым. Для второго провайдера заполни поля B и установи source_count = 2 в config.json. Два подключения к одному провайдеру не становятся независимыми источниками полноты.

Загрузи переменные среды и запусти пример обработки Pump.fun CreateEvent:

    set -a
    . ./.env
    set +a
    go test ./...
    go run ./cmd/pumpfun-create

Программа выводит JSON-события и сохраняет WAL в run/primary/journal.wal. Адрес PublicNode в примере не гарантирует доступ, поддержку фильтров или глубину replay у провайдера. Остановка — Ctrl+C. Повторная обработка уже записанного локального WAL с теми же настройками источника и фильтра:

    CORE_REPLAY_ONLY=1 go run ./cmd/pumpfun-create

Go не читает .env автоматически: переменные нужно загрузить в shell, передать через Docker/другой менеджер процессов либо настроить окружение вручную. Загружай `.env` через `source`/`.` только из доверенного файла: shell выполняет его содержимое.

### Настройки

**.env** — адреса провайдеров и чувствительные данные: CORE_PROVIDER_A_ENDPOINT, CORE_PROVIDER_A_TOKEN, CORE_PROVIDER_A_TOKEN_HEADER и необязательные аналоги CORE_PROVIDER_B_*. CORE_CONFIG задаёт путь к JSON-конфигу; CORE_REPLAY_ONLY=1 включает offline replay.

**config.json** — параметры исполнения без секретов:

| Поле | По умолчанию | Описание |
|---|---|---|
| source_count | 1 | Один или два независимо переподключаемых gRPC-потока |
| commitment | finalized | processed, confirmed либо finalized |
| delivery | provisional | provisional либо durable |
| filter_version | pumpfun-create-v1 | Версия подписки для идентификации данных |
| data_dir | ./run | Каталог WAL |
| queue_capacity | 4096 | Размер ограниченной очереди событий |
| dedupe_limit | 2000000 | Лимит дедупликации между источниками |
| ping_interval_seconds | 15 | Интервал keepalive |
| idle_timeout_seconds | 45 | Таймаут без входящих сообщений |

На каждого настроенного провайдера открывается одна подписка Yellowstone. Пример CLI принимает один или два источника; встраиваемый Go API — до 16. source_count не создаёт независимые валидаторы из одного endpoint. Размер очереди меняй после измерений задержек sink. Токены и пароли в JSON писать нельзя: неизвестные поля, включая token, отвергаются.

Для совместимости пока поддерживаются старые переменные CORE_ENDPOINT/CORE_TOKEN и CORE_ENDPOINT_B/CORE_TOKEN_B. В новых конфигурациях используй CORE_PROVIDER_A_* и CORE_PROVIDER_B_*.

## Интеграция

Библиотека импортируется в Go-приложение; обработчики и sink вызываются **в том же процессе**, без дополнительного gRPC между ядром и агрегатором.

Репозиторий публичный. Используй экспериментальный `v0.1.1`: он включает
WAL-hardening и исправления аудита от 15.11.2026. В старом `v0.1.0` этих
исправлений нет. `GOPRIVATE` не требуется:

    go get github.com/sirdont0/solana-yellowstone-grpc-data-core@v0.1.1

При локальной разработке рядом с Aggregator V2 можно использовать соседнюю
папку и временный replace:

    go mod edit -replace=github.com/sirdont0/solana-yellowstone-grpc-data-core=../solana-yellowstone-grpc-data-core

Для воспроизводимых сборок после публикации закрепляй версию по тегу или
коммиту, а локальный replace удаляй, когда он больше не нужен. Следующий
фрагмент показывает API внутри приложения; переменные `endpoint`, `token`,
`consumer` и `ctx` необходимо определить самостоятельно. Это не готовая
самостоятельная Go-программа:

    import (
        "context"
        core "github.com/sirdont0/solana-yellowstone-grpc-data-core"
        "github.com/sirdont0/solana-yellowstone-grpc-data-core/examples/pumpfuncreate"
    )

    engine, err := core.New(core.Config{
        Sources: []core.Source{{
            ID: "primary", Endpoint: endpoint, Token: token,
            Commitment: "finalized", Programs: []string{pumpfuncreate.ProgramID},
        }},
        DataDir: "./run",
        Delivery: core.Provisional,
    }, func(ctx context.Context, event core.Event) error {
        return consumer.Accept(ctx, event)
    })
    if err != nil { return err }
    if err := engine.Register(pumpfuncreate.New()); err != nil { return err }
    return engine.Run(ctx)

Для своей программы реализуй интерфейс обработчика:

    type Handler interface {
        ProgramIDs() []string
        Decode(*core.Transaction, core.Instruction) ([]core.Emission, error)
    }

Регистрируй обработчики до вызова Run. Instruction.Inner == -1 означает внешнюю инструкцию; индекс внутренней CPI отсчитывается от нуля внутри внешней. Данные инструкции считаются неизменяемыми; если их нужно сохранить, копируй их. Неизвестные дискриминаторы и версии должны оставаться неподдерживаемыми, пока не проверены реальные фикстуры.

Демонстрационный обработчик лежит в [examples/pumpfuncreate](../examples/pumpfuncreate/create.go). Он разбирает подтверждённый префикс Pump.fun CreateEvent, а неизвестный хвост отмечает отдельно.

### Доставка и качество данных

**Provisional** отдаёт событие после получения, ещё до fsync WAL: минимальная задержка, но при аварии событие может потеряться. **Durable** отдаёт событие после fsync журнала соответствующего источника, с дополнительной задержкой.

В каждом событии есть источник, поколение подключения, commitment, слот, подпись транзакции, CPI-позиция, ID события, отметка долговечности и качество. После разрыва связи качество может стать CONTINUITY_BROKEN. Разреженные слоты после фильтрации (например, 101 → 103 → 109) сами по себе нормальны.

Replay проверяет CRC и вызывает те же обработчики, что live. Порядок сохраняется внутри WAL одного источника, но исходный порядок между провайдерами не восстанавливается. При отсутствии WAL нужного источника или несовпадении идентичности источника/фильтра replay завершается с ошибкой. Доказательство полноты банков и RPC recovery пока вынесены в техдолг.

### Raw-only ingestion и курсоры потребителей

Используй `core.NewRaw(config)`, если приложению нужны разные типы Yellowstone
updates до выбора конкретного декодера программы. `Source.Subscription` принимает
копию `proto.SubscribeRequest`: фильтры транзакций, transaction status,
аккаунтов, слотов, block-meta, блоков, entry и block-footer. Ядро само задаёт commitment,
`from_slot` и ping, а нормализованную подписку включает в WAL identity.
Поля `Vote` и `Failed` в transaction filter — tri-state указатели: `nil` не
ограничивает результат, `false` исключает соответствующий класс, `true`
оставляет только его. Старый Pump-oriented фильтр по умолчанию исключает
vote-транзакции и failed-транзакции.

Для custom subscription reconnect использует минимальный наблюдаемый slot
frontier среди всех явно запрошенных классов updates. Если хотя бы один класс
ещё не дал slot, ядро не выводит его позицию из другого класса: используется
только явно заданный начальный `FromSlot` либо подключение начинается с текущей
вершины провайдера, а continuity break остаётся видимым. Это не доказывает
retention у провайдера или полноту исторического replay. `SubscribeReplayInfo`
носит справочный характер; replay нужно проверять для каждого endpoint/profile.

`NewRaw` сохраняет updates без ALT/CPI-декодирования, поэтому отсутствие CPI
не мешает захвату исходных данных. `OpenRawReader(source, cursor)` читает только
уже fsync-нутые записи пакетами с ограниченным размером через
`Next(ctx, limit)`. `ReadRaw` предназначен для offline catch-up после остановки
движка. `RawObservation` содержит protobuf payload, повторно сериализованный
после gRPC-декодирования, receive time, provider `created_at` при наличии,
source/slot/index/signature и признак наличия bank ID. Если провайдер прислал
transaction metadata, также доступны статус ошибки и комиссия. Это наблюдения
внутри одного источника, а не канонический порядок транзакций и не исходные
сетевые gRPC-фреймы.

Сохраняй прогресс через `Ack(consumer, observation.Cursor)` только после commit
собственной транзакции потребителя. ACK-файл атомарно заменяется и fsync-ится;
курсор монотонный и отдельный для каждого consumer/source. Если приложение
успело сделать commit, но упало до ACK, запись придёт повторно: гарантия
at-least-once. Курсор привязан к subscription identity и случайному WAL journal
ID. Если WAL удалили и создали заново, старый курсор вернёт
`ErrCursorExpired`. Автоматическое удаление/компакция WAL по ACK пока не сделаны.
Для долгой raw-записи можно включить `Config.DedupeDisk = true` (или
`YLAB_DEDUPE_DISK=true` в lab CLI). Тогда точная source-local дедупликация
использует индекс bbolt, восстановленный из WAL, а `DedupeLimit` задаёт размер
горячего RAM-кэша. Поздние дубликаты и конфликты распознаются после вытеснения
и перезапуска. Но каждый старт требует полного чтения WAL и временного места
для новой базы. Режим по умолчанию и отдельный предел legacy cross-source
decoded-event dedupe пока не изменены.
`ConsumerLag(consumer, source)` возвращает количество записей WAL между
подтверждённым курсором и fsync-нутой вершиной журнала. В счёт входят также
служебные и quality-маркеры; это не число канонических транзакций. Если есть
backlog, метод также возвращает время и возраст самой старой неподтверждённой
записи. `AgeBuckets` накапливает один sample возраста старейшей записи при
каждом вызове для пары consumer/source: `<=1s`, `<=5s`, `<=10s`, `<=30s`,
`<=1m`, `<=5m`, `<=1h`, `>1h`. Это распределение наблюдений при опросе, а не
сканирование возраста каждой записи в WAL. История sample хранится в памяти
процесса, ограничена 1024 активными парами consumer/source и сбрасывается при
перезапуске; наименее недавно опрашиваемая пара вытесняется первой.
В `Health()` доступны `QueueOldestWait`, `QueueLastWait`, `QueueMaxWait` и
`QueuePending` для общей очереди dispatch после резервирования byte-budget.
Эти величины не учитывают очередь source-WAL и повторяются для каждого source.
На поддерживаемых Unix-системах межпроцессный lock защищает проверку и запись
ACK от гонок между разными Engine. После штатного закрытия WAL сохраняется
вспомогательный `.sidx` для быстрого offline-чтения; устаревший индекс
игнорируется, а целостность самих WAL-фреймов проверяется при чтении.

Program handlers разрешены только для `finalized`. `processed` и `confirmed`
можно собирать через `NewRaw`; конкурирующие форки остаются локальными для
источника и не превращаются в общий конфликт finalized bank/dedupe. Значения
по умолчанию: 128 MiB на очередь source writer, 256 MiB на общую dispatch
очередь и 16 MiB на fsync batch. Это бюджет удерживаемых payload, а не жёсткий
лимит RSS процесса. `MaxMessageMiB` ограничен 95 MiB, чтобы base64 JSON envelope
помещался во фрейм WAL v1 размером 128 MiB. При graceful shutdown sink получает
до 15 секунд на drain; после таймаута возвращается `ErrDrainTimeout` и
отменяется delivery context. `Health()` показывает использование/лимиты source
и dispatch byte budgets, а также последнюю, суммарную и максимальную длительность
WAL sync и число sync. Гистограммы задержек и возраст старейшей очереди пока
остаются будущей работой.

## Структура репозитория

- [engine.go](../engine.go), [transaction.go](../transaction.go), [replay.go](../replay.go) — публичный API.
- [internal/ingest](../internal/ingest) — клиент Yellowstone, очереди, WAL, reconnect и сведения об источниках.
- [internal/sourcecfg](../internal/sourcecfg) — настройки источников.
- [proto](../proto) — изолированные protobuf-схемы и Go-биндинги.
- [examples/pumpfuncreate](../examples/pumpfuncreate) — заменяемый пример обработчика.
- [cmd/pumpfun-create](../cmd/pumpfun-create) — запускаемый пример.
- [scripts/regenerate-proto.sh](../scripts/regenerate-proto.sh) — повторяемая генерация proto с сохранением оригинальных gRPC-путей.

## Основа проекта, лицензии и разработка

Используются proto-схемы [rpcpool/yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc) (подкаталог yellowstone-grpc-proto — Apache-2.0), библиотеки [grpc/grpc-go](https://github.com/grpc/grpc-go), [protocolbuffers/protobuf-go](https://github.com/protocolbuffers/protobuf-go) и [mr-tron/base58](https://github.com/mr-tron/base58). Полноценный Yellowstone Geyser-плагин валидатора в этот репозиторий не входит.

Собственный код и документация ядра распространяются по MIT. Адаптированные proto-схемы и сгенерированные биндинги сохраняют Apache-2.0 и атрибуцию исходного проекта: [сторонние лицензии](../THIRD_PARTY_NOTICES.md). В других частях исходного проекта Yellowstone действует иная базовая лицензия.

Проверки перед коммитом:

    gofmt -w ./cmd/pumpfun-create/*.go
    go test ./...
    go test -race ./...
    go vet ./...

При обновлении proto запускай [scripts/regenerate-proto.sh](../scripts/regenerate-proto.sh) и проверяй совместное подключение со старым protobuf-клиентом агрегатора. Подробности — в [архитектурном контракте](ARCHITECTURE.md) и [техдолге](TECH_DEBT.md). Это независимый проект, не официальный SDK Solana, Triton One или Pump.fun.

## Ошибки, восстановление и диагностика

`Config.DecodeErrorPolicy` по умолчанию равен `FailClosed`. Явный `SkipAndFlag`
пропускает целую нечитаемую транзакцию, сохраняет raw и диагностический маркер,
фиксирует `CONTINUITY_BROKEN`. Для обработчика доступен
`RegisterWithPolicy(handler, SkipAndFlag)`. Ошибки sink/storage и конфликты
целостности всегда фатальны. Частичные эффекты после ошибки декодера не выходят.

`ReplayWithOptions(ctx, ReplayOptions{TolerantTail:true})` читает целый префикс
оборванного WAL без изменения файла. До выдачи событий проверяются все журналы;
неверные CRC/identity и активный writer остаются ошибками. Отсутствие записи CPI
не равно известному пустому списку CPI.

Настройки API: `ReplayOverlap` (0 выбирает 64 слота), `MaxReconnects` (0 — без
лимита), `FromSlot`. В JSON примера: `replay_overlap`, `max_reconnects`,
`from_slot`, `decode_error_policy`, `handler_error_policy` (значения
`fail_closed` / `skip_and_flag`) и `tolerant_replay_tail`.

`Health()`, `Diagnostics()` и `Evidence()` возвращают независимые снимки.
`Source.FailureDomain` задаётся приложением; неизвестные домены считаются одним.
`Logger` принимает `*slog.Logger`. Диагностика не доказывает полноту банков.
