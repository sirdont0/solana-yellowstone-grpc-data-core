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

Репозиторий публичный. Для воспроизводимой зависимости используй
опубликованный экспериментальный релиз; `GOPRIVATE` не требуется:

    go get github.com/sirdont0/solana-yellowstone-grpc-data-core@v0.1.0

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
