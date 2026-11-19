# Yellowstone provider compatibility matrix

**Статус на 2026-11-19:** PublicNode прошёл ограниченные authenticated live probes и dual-session сравнения; полного PV-01…PV-10 acceptance нет. В семи новых authenticated sessions живые finalized update kinds наблюдались отдельно, но 6 попыток `from_slot` reopen/overlap завершились ошибками (4 `InvalidArgument`, 2 `Unavailable`). Второй независимый Yellowstone-провайдер не настроен. Подробности: [provider probe](audits/2026-11-19-c1/PROVIDER_PROBE.md), [dual coverage](audits/2026-11-19-c1/DUAL_PUBLICNODE_COVERAGE.md), [новая G3 matrix](audits/2026-11-19-g3/REPORT.md). Наличие метода в protobuf или рекламируемая возможность не означает работоспособность на выбранном endpoint.

Проверяется конкретная комбинация `(provider endpoint, backend/version, commitment, subscription filters, auth tier, date)`. Каждый вариант получает отдельную строку evidence, включая источник и ограничения. Изменение фильтра создаёт новый профиль, даже если hostname не изменился. Токены, полные private endpoints и приватные WAL не коммитятся.

## Профили для первой проверки

| Поле | `primary` — PublicNode | `secondary` — независимый provider |
| --- | --- | --- |
| Endpoint / auth tier | Заполняется оператором | Выбрать и записать отдельно |
| `Source.ID` | `primary` | `secondary` |
| `FailureDomain` | `publicnode` до доказанного уточнения | Фактический оператор/backend; не совпадает с A, если доказана независимость |
| Commitment | `finalized` baseline | `finalized` baseline |
| FilterVersion | Версионированный единый логический набор фильтров | Эквивалентный логический набор фильтров |
| Protocol build/descriptor hash | **НЕ ПРОВЕРЕНО** | **НЕ ПРОВЕРЕНО** |
| Реальная runtime проверка | **PARTIAL**: dual client/RPC parity, отдельные live finalized kinds; 6 replay-overlap attempts FAILED; natural EOF, combined stream/quotas OPEN | **НЕ ПРОВЕРЕНО / NOT_CONFIGURED** |

Кандидаты и их различия теперь сведены в [provider candidate research](audits/2026-11-19-g3/PROVIDER_CANDIDATES.md). Для replay наиболее перспективен [Alchemy Historical Replay](https://www.alchemy.com/docs/reference/yellowstone-grpc-historical-replay): официально документирует `from_slot`, но у Alchemy есть конфликт официальных страниц по replay window (432 000 против 6 000 slots), поэтому фактическая граница UNKNOWN до письменного уточнения и live probe. [QuickNode](https://www.quicknode.com/guides/solana-development/tooling/solana-grpc/solana-grpc) документирует `fromSlot` до 3 000 slots, но требует Scale/Business. [Sodae](https://docs.sodae.io/yellowstone/overview) даёт trial 5 GB/7 дней с approval, однако его публичные Yellowstone docs не подтверждают replay. Ни один кандидат не настроен: локальных credentials для B не найдено.

Дополнительно bounded public Solana JSON-RPC `getBlocks/getBlock` probe PASS; он подтверждает retrieval полного transaction list одного finalized block, но не является Yellowstone B и не восстанавливает полную account/config history. Подробности и redacted JSON — в G3 audit. This RPC repair belongs to the downstream consumer, not this Core library.

## Обязательные capability probes

Во время каждой проверки в таблицу ниже заносятся `PASS`/`FAIL`/`UNSUPPORTED`/`UNKNOWN`, точная дата, версия protobuf и путь к обезличенному протоколу теста. `UNKNOWN` никогда не превращается в `PASS` по аналогии с другим провайдером.

| ID | Возможность / негативный сценарий | Что именно нужно доказать |
| --- | --- | --- |
| PV-01 | TLS/auth/version | Установить защищённое соединение, зафиксировать `GetVersion`/capabilities и реакцию на ошибочный auth без утечки токена |
| PV-02 | Указанные update kinds | Точный набор: transactions, transaction status, accounts, slots, blocks_meta, blocks, entry, block_footer — по отдельности и совместно в пределах лимитов |
| PV-03 | Протокол / schema | Принятый запрос и корректно декодируемые ответы, включая optional presence, bank identity, актуальные block-footer сертификаты, их собственный slot/identity и неизвестные поля |
| PV-04 | `SubscribeReplayInfo` | Поддерживается ли RPC, `first_available`, статус и смысл результата именно для этого backend |
| PV-05 | `from_slot` | Работает ли на **данном commitment** и **каждом требуемом виде update**; фактическая граница replay, overlap и точные ошибки |
| PV-06 | Reconnect / retention exceeded | Принудительный обрыв, восстановление, повтор/пропуск, отказ в replay, отметка неизвестного покрытия |
| PV-07 | Bank/slot evidence | Есть ли `bank_id`, `parent_slot`, `blockhash`, footer и какие сообщения могут приходить раньше transactions; самостоятельного доказательства полной истории здесь нет |
| PV-08 | Реальные лимиты | Max message size, subscriptions/filters, update bytes/s, 403/429/RESOURCE_EXHAUSTED, разрывы при перегрузке |
| PV-09 | Независимость A/B | Общий оператор, proxy, upstream cluster, география, ограничения и общий failure domain задокументированы без необоснованных гарантий |
| PV-10 | Доказательство данных | Сверка выборочных block/tx anchors с внешним RPC/архивом; несовпадения, unknown ranges и результат repair фиксируются отдельно |

## Процедура проверки

1. Зафиксировать Git SHA Core, версию `go`, checksum локального `SubscribeRequest`, подписку, redacted endpoint, auth tier, дату UTC, длительность и измеренные rates. Выполнить `go run ./cmd/corectl capture --config ./rawcore.json --dry-run` для проверки локальных параметров. Это не проверяет провайдера.
2. Подключиться к реальному endpoint с секретами из окружения и записать короткий raw capture; прервать/восстановить с overlap. Отдельно пробовать каждый фильтр/commitment, поскольку сервер может принимать один набор и отклонять другой.
3. По завершении выполнить `go run ./cmd/corectl verify --data-dir ./run --source primary` и `inspect` / `replay` по [RAW_QUICKSTART.md](RAW_QUICKSTART.md). Проверка целостности WAL не считается подтверждением покрытия Solana.
4. Сопоставить полученные идентификаторы и контрольные блоки с независимым источником, зафиксировать реальные `from_slot` результаты, ограничения, gaps и timestamp разницу. Повторить для B и при двух одновременных потоках.
5. Сохранить обезличенный JSON результат с sha256 fixtures в `docs/audits/YYYY-MM-DD-provider/`, не включая токены, приватные payload и адреса пользовательских кошельков. Срок действия capability finding задаётся профилем; любое обновление backend требует повторного smoke/proto проверки.

## Решение при несовместимости

Provider profile, не поддерживающий обязательный filter или replay, может использоваться только с явным ограничением/quality degradation. Перенос с данного источника на live head при неудачном `from_slot` — отдельное разрешённое действие, которое MUST создавать coverage break. Два провайдера с общим backend не считаются независимым доказательством полноты.

См. [SOURCE_CONTRACT.md](SOURCE_CONTRACT.md), [RELEASE_GATES.md](RELEASE_GATES.md) и [CONSUMER_ACCEPTANCE_AND_SLO.md](CONSUMER_ACCEPTANCE_AND_SLO.md).
