# Критический аудит Claude: иллюстрации RKB, VibePublish и границы со Street Story

Дата: 19 октября 2026. Статус: **реестр предложений для рассмотрения, без реализации**.

По прямому запросу владельца проведены две независимые read-only консультации через существующий backend DevCoveer `claude_consult` (Claude Code / Claude Agent SDK). Платформа подтвердила фактическую модель **`claude-opus-5-5` в обеих консультациях**, без замены модели. Сам консультант сообщает Agent SDK и не утверждает, что видит интерактивный CLI; это ограничение сохранено в его исходных ответах ниже.

Claude получил два явно подготовленных пакета публичного кода с исходными номерами строк и обезличенными результатами приёмки; инструменты чтения/записи файлов, сети и внешних систем у него были отключены (`provided_only`). Это критический анализ предоставленного материала, а не самостоятельное обследование всего сервера. Код и live-состояние в рамках аудита не менялись. Ни один пункт ниже не является поручением автоматически внедрять его.

Область: поиск → evidence → figure/crop → приватный Telegram-архив → реальные байты, долговременные GET/PUT, grants/ACL, cache, SQLite/disk/RAM, worker/leases/FloodWait и соседство со Street Story. Универсальная фотоидентификация, голос и редакторское качество Street Story полностью не аудировались.

Исходный код: RKB `481241fa36db30e3f0e6c4c4b88fb3ce84964676`, VibePublish `d1f880b24788f609398a660ba53dd8c4731b7aec` (schema 11). Это проверенные runtime-версии, не предположение, что production равен новейшему main. [Фактическая приёмка](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/13c53548c0088f94716ed986b9a1f8a348b7a2c8/docs/reports/illustration-archive-live-acceptance-20261019.md) отдельно различает 151/151 метаданных, восемь свежих image-byte downloads и четыре PDF.

## Как пользоваться реестром

Сохранены **все 60 пунктов Claude**: 35 RKB и 25 VibePublish. Пересекающиеся замечания имеют собственные ID и не потеряны при объединении. Нумерация RKB исходная: A35 отсутствует, A36 присутствует. P0 в предоставленном материале не выявлено; это не гарантия отсутствия P0 во всём продукте. Приоритеты и уверенность принадлежат консультанту.

«Доказан по коду» означает свойство рассмотренной реализации, а не воспроизведённый production-инцидент. «Кодовый риск», «гипотеза» и «улучшение» не должны превращаться в утверждение о текущем отказе. Сначала безопасный воспроизводимый тест/замер, затем отдельное решение о доработке. Ни один пункт не требует повторно публиковать 151 изображение или переимпортировать книги.

## Критическая сверка Codex после получения ответов

Следующие уточнения имеют приоритет над буквальным прочтением исходных ответов Claude; исходные предложения сохранены полностью для прозрачности.

| Пункт | Уточнение по дополнительному чтению и live-фактам |
| --- | --- |
| RKB-A13 | Признак незавершённого поиска уже существует: реальные ответы содержали `main_state=starting/pending`, `main_job_id`, degraded mode и затем `ready`. Добавлять второй флаг без необходимости не нужно. Остаётся проверить поведение потребителей и холодную latency. |
| RKB-A31 | Гипотеза о deferred BEGIN не подтверждается: [BgeQueue.connect](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L54-L64) явно выполняет `BEGIN IMMEDIATE`. Не заводить исправление отсутствующей транзакции. |
| RKB-A33 | WAL уже включён [конструктором SQLiteCorpus](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/sqlite_corpus.py#L75); read-only PRAGMA подтвердил WAL у corpus, BGE queue и Vibe. Замеры contention полезны, но «включить отсутствующий WAL» не требуется. |
| VP-A18 | Гипотеза о воскрешении старого grant при replay опровергается дополнительным кодом: [_replay](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L435-L456) вызывает [private_operation](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L384-L404), который повторно требует сохранённый grant ID. Существующие revoke/regrant regressions пройдены. |
| RKB-A08, VP-A09 | Source, display и provider SHA имеют разные роли. Нельзя исправлять отсутствие provider-якоря угадыванием через другой SHA или переписыванием immutable origin. Любая доработка сохраняет явный proof format и fail-closed семантику. |
| RKB-A10 | Нельзя просто требовать `requesting_actor == grant.owner`: это может запретить законное ACL-scoped чтение общего источника. Проверять нужно фактическую привязку документа/entry/tenant и права источника, а не заменять ACL равенством пользователей. |
| RKB-A36 | Отсутствие crop pointer не означает потерю исходной книги: оригиналы и приватный Telegram-архив сохранены. Риск — доступность конкретной crop-rendition и зависимость её штатной доставки от Vibe/Telegram. |
| VP-A01, VP-A11 | Повтор того же keyed GET сохраняет operation/receipt и не равен новой BLOB-копии. Риск роста cache относится к новым операциям, разным ключам и GET без ключа. Нельзя устранять его ослаблением grant/cache fences. |
| VP-A07, VP-A12 | Влияние общего Telegram lane на Street Story условно: продукты используют независимые scoped connections. Внутреннюю fairness/общий event loop всё равно нужно измерить. Не объявлять остановку Street Story наблюдавшимся фактом. |
| VP-A25 | После ответов завершился новый CI **6/6 success**, [run 39230004084](https://github.com/onedayonemasterpiece/vibepublish/actions/runs/39230004084); [PR88](https://github.com/onedayonemasterpiece/vibepublish/pull/88) слит `03236559bacc0c9bd06ce3086b1ef7fc9d94366b`. Лимит только полного verify-job увеличен 15→20 минут; тесты, другие jobs и продуктовые timeouts сохранены. Старый cancelled run не скрывается и не считается успешным. |

Архитектурные ограничения для всех предложений: BGE inference выполняется **только на Kaggle**, без локального BGE fallback; существующий E5 сохраняется. Same-host связь — имеющийся loopback/Unix, dedicated scoped principal, без owner bearer, browser OAuth или нового gateway. UNKNOWN сначала наблюдается; новые GET допустимы только при доказанном безопасном состоянии. Не ослаблять ACL, source/revision fences и provider-byte checks. Квоты, мониторинг и кэш — предложения, не повод автоматически добавлять сервисы, увеличивать оплачиваемый диск, проводить VACUUM или копировать живую БД.

## Рекомендуемый порядок рассмотрения

1. **Диск и память:** RKB-A01/A02/A03/A19, VP-A01/A02. Сначала измерить общий запас, WAL/cache/RSS и определить допустимый ресурсный бюджет; не проводить разрушительный эксперимент на prod.
2. **Восстановление чтения:** RKB-A04/A05/A06/A07 и VP-A11/A13/A24. Разобрать terminal transient, lost reply, cooldown и TTL при сохранении известных operation IDs.
3. **Конкуренция и задержки:** RKB-A18/A15, VP-A03/A04/A05/A06/A07/A12/A17. Staging/fake нагрузка, длительность write transaction, heartbeat/lane wait, fairness.
4. **Доказательства и права:** RKB-A08/A10, VP-A08/A09/A10/A21. Сначала воспроизводимость, затем изменения без расширения прав и переоценки immutable proof.
5. **Качество поиска и покрытие:** RKB-A13/A14/A29/A32, VP-A22. Отдельные eval/byte sampling; полнота индекса не заменяет качество retrieval. Остальные P3 — по подтверждённой потребности.

## Индекс всех предложений

Распределение исходных приоритетов: P1: 6, P2: 24, P3: 30.

| ID | Приоритет | Статус/уверенность по Claude |
| --- | --- | --- |
| [RKB-A01](#rkb-a01) | P1 | доказан (измерение) + кодовый риск · уверенность высокая |
| [RKB-A02](#rkb-a02) | P1 | кодовый риск · уверенность высокая |
| [RKB-A03](#rkb-a03) | P1 | кодовый риск · уверенность средне-высокая |
| [RKB-A04](#rkb-a04) | P1 | кодовый риск · уверенность высокая |
| [RKB-A13](#rkb-a13) | P1 | доказан (наблюдение) · уверенность высокая (факт) / средняя (наличие флага) |
| [VP-A01](#vp-a01) | P1 | кодовый риск · уверенность: высокая (код), средняя (масштаб) |
| [RKB-A05](#rkb-a05) | P2 | кодовый риск · уверенность высокая |
| [RKB-A06](#rkb-a06) | P2 | кодовый риск · уверенность высокая |
| [RKB-A08](#rkb-a08) | P2 | кодовый риск · уверенность средняя |
| [RKB-A10](#rkb-a10) | P2 | кодовый риск (defense-in-depth) · уверенность средняя |
| [RKB-A11](#rkb-a11) | P2 | гипотеза · уверенность средняя |
| [RKB-A12](#rkb-a12) | P2 | гипотеза · уверенность низко-средняя |
| [RKB-A14](#rkb-a14) | P2 | доказан (на 5 запросах, не массово) · уверенность средняя |
| [RKB-A15](#rkb-a15) | P2 | кодовый риск · уверенность средняя |
| [RKB-A16](#rkb-a16) | P2 | гипотеза · уверенность низкая |
| [RKB-A18](#rkb-a18) | P2 | кодовый риск · уверенность средняя |
| [RKB-A19](#rkb-a19) | P2 | кодовый риск · уверенность высокая |
| [RKB-A32](#rkb-a32) | P2 | улучшение · уверенность высокая |
| [RKB-A36](#rkb-a36) | P2 | доказан (состояние) + улучшение · уверенность средняя |
| [VP-A02](#vp-a02) | P2 | кодовый риск · уверенность: средняя |
| [VP-A03](#vp-a03) | P2 | кодовый риск · уверенность: высокая |
| [VP-A04](#vp-a04) | P2 | кодовый риск · уверенность: высокая |
| [VP-A05](#vp-a05) | P2 | кодовый риск · уверенность: средняя |
| [VP-A06](#vp-a06) | P2 | доказан (по коду) · уверенность: высокая |
| [VP-A07](#vp-a07) | P2 | кодовый риск (влияние на соседей — гипотеза) · уверенность: средняя |
| [VP-A08](#vp-a08) | P2 | доказан (поведение по коду, by design) · уверенность: высокая |
| [VP-A09](#vp-a09) | P2 | кодовый риск · уверенность: средняя |
| [VP-A10](#vp-a10) | P2 | кодовый риск · уверенность: средняя |
| [VP-A11](#vp-a11) | P2 | кодовый риск · уверенность: высокая |
| [VP-A12](#vp-a12) | P2 | кодовый риск · уверенность: средняя |
| [RKB-A07](#rkb-a07) | P3 | кодовый риск · уверенность средняя |
| [RKB-A09](#rkb-a09) | P3 | кодовый риск · уверенность высокая |
| [RKB-A17](#rkb-a17) | P3 | кодовый риск · уверенность средняя |
| [RKB-A20](#rkb-a20) | P3 | кодовый риск · уверенность средняя |
| [RKB-A21](#rkb-a21) | P3 | гипотеза · уверенность низкая |
| [RKB-A22](#rkb-a22) | P3 | улучшение |
| [RKB-A23](#rkb-a23) | P3 | кодовый риск (только OAuth-путь, вероятно legacy) · уверенность средняя |
| [RKB-A24](#rkb-a24) | P3 | кодовый риск |
| [RKB-A25](#rkb-a25) | P3 | кодовый риск |
| [RKB-A26](#rkb-a26) | P3 | улучшение (наблюдаемость) |
| [RKB-A27](#rkb-a27) | P3 | улучшение |
| [RKB-A28](#rkb-a28) | P3 | кодовый риск |
| [RKB-A29](#rkb-a29) | P3 | кодовый риск |
| [RKB-A30](#rkb-a30) | P3 | кодовый риск |
| [RKB-A31](#rkb-a31) | P3 | гипотеза |
| [RKB-A33](#rkb-a33) | P3 | гипотеза |
| [RKB-A34](#rkb-a34) | P3 | улучшение (покрытие) |
| [VP-A13](#vp-a13) | P3 | улучшение · уверенность: высокая |
| [VP-A14](#vp-a14) | P3 | кодовый риск · уверенность: низкая–средняя |
| [VP-A15](#vp-a15) | P3 | кодовый риск · уверенность: низкая |
| [VP-A16](#vp-a16) | P3 | гипотеза · уверенность: низкая |
| [VP-A17](#vp-a17) | P3 | кодовый риск (производительность) · уверенность: средняя |
| [VP-A18](#vp-a18) | P3 | гипотеза · уверенность: низкая |
| [VP-A19](#vp-a19) | P3 | улучшение · уверенность: высокая |
| [VP-A20](#vp-a20) | P3 | гипотеза · уверенность: низкая |
| [VP-A21](#vp-a21) | P3 | кодовый риск (хрупкость) · уверенность: средняя |
| [VP-A22](#vp-a22) | P3 | улучшение (покрытие) · уверенность: высокая |
| [VP-A23](#vp-a23) | P3 | кодовый риск · уверенность: низкая |
| [VP-A24](#vp-a24) | P3 | улучшение · уверенность: высокая |
| [VP-A25](#vp-a25) | P3 | улучшение · уверенность: высокая |

## Полный текст Claude — часть RKB

**Модель:** платформа сообщает Opus 5.5 (`claude-opus-5-5`), запуск через Claude Agent SDK. Принадлежность к конкретной поверхности Claude Code из окружения не видна, поэтому я её не утверждаю.

### Критический аудит RKB: иллюстрации, архив, кэш, mirror, BGE, SQLite, same-host

**Источник ссылок:** `onedayonemasterpiece/regional-knowledge-base@481241fa36db30e3f0e6c4c4b88fb3ce84964676`. Ниже это сокращено до `RKB@481241fa <файл>:<строка>`, где файл лежит в `src/regional_knowledge/`. Ссылки указывают на строки из пакета. Перед публикацией permalink их стоит сверить с репозиторием.

**Режим:** только чтение. Предложения ниже являются советом, а не поручением.

**Статусы:**
- **доказан**: следует из приёмочных фактов пакета;
- **кодовый риск**: виден в коде, в проде не наблюдался;
- **гипотеза**: нужен эксперимент или код вне пакета;
- **улучшение**: необязательно.

**Важная граница доказанного.** Полный каталожный аудит 151/151 сверил метаданные: tenant, topic4, origin.ref, provider `download_sha256`. Он не скачивал байты. Байтовая доставка доказана только для 8 свежих PNG и 4 PDF. Успешная приёмка не гарантирует массового качества поиска и массовой доступности.

---

#### Findings

##### Реестр

###### P1

<a id="rkb-a01"></a>

**RKB-A01 · P1 · доказан (измерение) + кодовый риск · уверенность высокая**
- **Сценарий.** Свободно около 2.36 GiB, резерв для новых admission в mirror 2 GiB. Запас до порога около 0.36 GiB. Vibe SQLite около 4.54 GB делит файловую систему с архивом и кэшем (комментарий RKB@481241fa [illustration_mirror.py:232-234](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L232-L234)). Рост WAL, загрузка временных assets или одна выдача оригинала быстро съедают запас.
- **Обоснование.** Порог есть только в [illustration_mirror.py:236-237](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L236-L237). У других писателей его нет: см. A02 и A03.
- **Ущерб.** Сначала mirror уйдёт в `MirrorAdmissionDeferred`, что безопасно. Дальше возможен ENOSPC у Vibe SQLite и durable receipts. Поведение Vibe при ENOSPC из пакета не проверить. Масштаб: весь хост.
- **Доработка.** План ёмкости вне кода: расширение тома или перенос по решению владельца. Сырые копии БД не делать. Ввести мониторинг free bytes и размера WAL с порогом выше 2 GiB.
- **Проверка.** Метрика `free_bytes` на томе state. Алерт срабатывает до достижения резерва. В логах есть события `MirrorAdmissionDeferred`.
- **Порядок.** Первым. От этого зависят A02, A03, A19.

<a id="rkb-a02"></a>

**RKB-A02 · P1 · кодовый риск · уверенность высокая**
- **Сценарий.** Новый импорт источника, затем `SourceArchive.tick` выполняет PUT полного PDF/DjVu в Vibe assets. Проверки резерва диска здесь нет, в отличие от mirror.
- **Обоснование.** [source_archive.py:199-205](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L199-L205) (`get_bytes` и `POST /v1/assets` без проверки `disk_usage`). Для сравнения: [illustration_mirror.py:236](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L236).
- **Ущерб.** Самая крупная запись на том при запасе около 0.36 GiB. Политика «держать источник до проверенного SHA» соблюдается, но Vibe может упасть по ENOSPC.
- **Доработка.** Тот же guard «только новые PUT», что в mirror, с порогом с учётом размера источника. Уже начатые operation продолжают reconcile.
- **Проверка.** Unit-тест: при `free < reserve + size` PUT не происходит, существующий `operation_id` продолжает опрашиваться.
- **Порядок.** После A01. Порог согласовать с A25.

<a id="rkb-a03"></a>

**RKB-A03 · P1 · кодовый риск · уверенность средне-высокая**
- **Сценарий.** `download_source` скачивает оригинал во временный `.partial`, устанавливает его в кэш, затем копирует его по `path`. Пиковое потребление: partial плюс копия, после установки файл кэша плюс копия. `install` проверяет только бюджет кэша (512 MiB), но не свободное место на ФС.
- **Обоснование.** [source_archive.py:157-160](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L157-L160) (copyfile); [original_cache.py:127-134](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L127-L134), `141-150`.
- **Ущерб.** При почти полном диске одна выдача оригинала может толкнуть ФС ниже резерва mirror или вызвать ENOSPC.
- **Доработка.** Проверять `statvfs` перед loader и перед copy, с общим резервом. Отказ fail-closed с отдельным кодом ошибки. Рассмотреть отдачу из кэша без копии, если вызывающий это допускает.
- **Проверка.** Тест с подменённым `disk_usage`. Метрика: отказы с кодом low-disk, без ENOSPC в логах.
- **Порядок.** После A01.

<a id="rkb-a04"></a>

**RKB-A04 · P1 · кодовый риск · уверенность высокая**
- **Сценарий.** Все активные crop имеют `crop_object_id = null`, это доказанный факт. Значит, каждое чтение crop идёт через `telegram_archive`. Допустим, provider-GET один раз завершился terminal `blocked` с транзиентным кодом (`telegram_transport_unhealthy`/`provider_read_deadline`). Тогда scope в `archive_reads` навсегда держит этот operation. Каждый следующий вызов повторяет тот же `request_key`, получает тот же неуспешный receipt и `archive_source_unavailable`. Пути восстановления нет, кроме ручного вмешательства в кэш-БД.
- **Обоснование.** [source_archive.py:77-90](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L77-L90) (scope retire только на 404/410 в `:100-102`). Тест `test_terminal_provider_failure_is_not_replaced_by_new_get` фиксирует такое поведение намеренно. В mirror для тех же кодов есть контролируемый retry: [illustration_mirror.py:167-171](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L167-L171), `264-283`.
- **Ущерб.** Конкретная иллюстрация или оригинал после одного транзиентного сбоя Telegram недоступны неопределённо долго. Масштаб: любая из 151 записи.
- **Доработка.** Перенести политику mirror. Для `blocked` с кодом из `_TRANSIENT_READ_CODES` после cooldown выполнять compare-and-replace scope на детерминированный ключ, производный от старого operation (по образцу [illustration_mirror.py:269](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L269)). Denied, failed и неизвестные коды не трогать.
- **Проверка.** Fake-provider тесты: transient → один новый keyed GET после cooldown; denied → без retry; lost reply нового GET → тот же ключ.
- **Порядок.** До A06, чтобы общая логика cooldown была одна.

<a id="rkb-a13"></a>

**RKB-A13 · P1 · доказан (наблюдение) · уверенность высокая (факт) / средняя (наличие флага)**
- **Сценарий.** При холодном старте BGE (несколько минут) первые lexical-результаты были неадекватны. Ready наступил по тому же job ID. Агент-потребитель может принять ранний lexical-ответ за окончательный.
- **Обоснование.** Окно холодного старта до 900 с: [bge_queue.py:83-85](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L83-L85). Job ID стабилен: [bge_queue.py:115-120](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L115-L120). Код выдачи поиска и признак degraded в пакете отсутствуют.
- **Ущерб.** Неверные или пропущенные доказательства в ответах. Масштаб: каждый первый запрос после простоя.
- **Доработка.** Проверить, а при отсутствии добавить явный машиночитаемый признак `semantic_pending` с job ID и рекомендацией повторить. Lexical-результаты при этом помечаются как предварительные.
- **Проверка.** Контрактный тест ответа поиска в состоянии `starting`. Метрика: доля ответов, отданных потребителю до ready.
- **Порядок.** Независимо.

###### P2

<a id="rkb-a05"></a>

**RKB-A05 · P2 · кодовый риск · уверенность высокая**
- **Сценарий.** Lost reply при admission первого readback GET. Процесс падает или соединение рвётся после `call(get)`, но до записи `read_operation_id`. Следующий tick делает новый GET без ключа и получает дубликат provider-чтения, то есть лишнюю загрузку Telegram и риск FloodWait.
- **Обоснование.** [illustration_mirror.py:249-254](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L249-L254) и [source_archive.py:267-272](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L267-L272): GET без `request_key`. Для сравнения: keyed retry [illustration_mirror.py:269](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L269) и keyed archive read [source_archive.py:79-80](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L79-L80).
- **Ущерб.** Публикационного side-effect нет ([illustration_mirror.py:167](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L167)), но нагрузка на provider растёт.
- **Доработка.** Детерминированный `request_key`, например `rkb:mirror:read:<put_operation>` или `rkb:source:read:<operation>`.
- **Проверка.** Тест «потерянный ответ GET → тот же operation_id».
- **Порядок.** Независимо.

<a id="rkb-a06"></a>

**RKB-A06 · P2 · кодовый риск · уверенность высокая**
- **Сценарий.** Элемент в FloodWait-cooldown (`MirrorReadTransient`) при опросе receipt ловит любое другое исключение, например сетевую ошибку. Общий `except` перезаписывает `mirror_error_type` именем класса. На следующем tick `_read_retry_due` видит, что тип не transient, и сразу разрешает новый GET. Cooldown обойдён.
- **Обоснование.** [illustration_mirror.py:174-176](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L174-L176), `295-296`, `264-266`.
- **Ущерб.** Усиление FloodWait при деградации Telegram. Именно этот режим cooldown и должен был гасить.
- **Доработка.** Хранить маркер cooldown в отдельном поле или не перезаписывать `MirrorReadTransient` в общем обработчике.
- **Проверка.** Тест: transient → сетевое исключение → следующий tick не выполняет GET до истечения cooldown.
- **Порядок.** Вместе с A04 (общая политика).

<a id="rkb-a08"></a>

**RKB-A08 · P2 · кодовый риск · уверенность средняя**
- **Сценарий.** Чтение crop через архив сверяет байты только с `provider_crop_sha256`, и только если он сохранён. Если поле null и provider evidence без sha, `archive_payload` возвращает собственный sha данных. Независимой проверки целостности тогда нет: display/source sha для origin `telegram_archive` не проверяются.
- **Обоснование.** [illustrations.py:123-130](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L123-L130); [source_archive.py:106-110](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L106-L110).
- **Ущерб.** Сейчас не проявлено: 151/151 сверены с provider sha. Риск относится к будущим и legacy-записям.
- **Доработка.** Fail-closed, если нет ни сохранённого provider sha, ни evidence sha. Опционально сверять с `display_crop_sha256`, если historically они совпадали.
- **Проверка.** Unit-тест с null-якорями; read-only выборка по каталогу, есть ли записи без `provider_crop_sha256`.
- **Порядок.** Независимо.

<a id="rkb-a10"></a>

**RKB-A10 · P2 · кодовый риск (defense-in-depth) · уверенность средняя**
- **Сценарий.** Путь crop использует единственный service grant из env и не проверяет, что владелец документа совпадает с `client.owner`. Путь оригиналов такую проверку делает. В пакете 121 service-owned и 30 legacy-owner записей. Корректность держится только на RLS RKB и scope на стороне Vibe.
- **Обоснование.** [illustrations.py:117-120](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L117-L120). Для сравнения: [source_archive.py:135](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L135).
- **Ущерб.** Ошибка RLS или подмена `vibepublish_entry_ref` может дать чтение чужого entry через сервисную идентичность. Наблюдений нет.
- **Доработка.** Явная проверка привязки владельца, tenant и thread entry к grant до GET (по образцу проверок `thread_ref` в mirror).
- **Проверка.** Негативный тест: entry чужого owner → отказ до provider-вызова.
- **Порядок.** Независимо.

<a id="rkb-a11"></a>

**RKB-A11 · P2 · гипотеза · уверенность средняя**
- **Сценарий 1.** Vibe хранит идемпотентность `request_key` меньше, чем RKB хранит scope (`archive_reads` живёт «до 404/410»). Replay ключа создаст новый operation, сработает `archive_read_operation_conflict`, и scope застрянет навсегда.
- **Сценарий 2.** У asset истекла идемпотентность `Idempotency-Key`. Replay PUT получит новый `asset_id`, payload `media_store put` изменится, и PUT зациклится на `idempotency_conflict`. Это fail-safe, но «вечно».
- **Обоснование.** [source_archive.py:81-82](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L81-L82); [illustration_mirror.py:153-160](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L153-L160), `239-240`; [source_archive.py:203-205](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L203-L205), `241`.
- **Ущерб.** Застревание без дубликатов публикаций.
- **Доработка.** Сначала эксперимент. Затем явный retire scope при доказанном «ключ неизвестен» (не при сетевой неопределённости).
- **Проверка.** Read-only сверка кода и политики retention Vibe; тест на fake provider.
- **Порядок.** До любых изменений A04.

<a id="rkb-a12"></a>

**RKB-A12 · P2 · гипотеза · уверенность низко-средняя**
- **Сценарий.** PUT источника `blocked/failed` вызывает `retry_failed` с ключом `rkb:source:retry:<operation>`. Если и retry завершится неуспехом, то следующий вызов с тем же ключом и новым `expected_revision` даст конфликт идемпотентности. Получится цикл раз в 30 с без продвижения.
- **Обоснование.** [source_archive.py:255-264](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L255-L264), `166`.
- **Доработка.** Ключ, зависящий от ревизии, и лимит или алерт на повторный провал. Только после проверки семантики Vibe.
- **Проверка.** Fake-provider тест двойного провала.
- **Порядок.** После проверки Vibe.

<a id="rkb-a14"></a>

**RKB-A14 · P2 · доказан (на 5 запросах, не массово) · уверенность средняя**
- **Сценарий.** Целевые доказательства на rank 2–3, нерелевантные выше были отклонены вручную. BGE 2959/2959 ready означает полноту индекса, а не качество.
- **Обоснование.** Приёмочный отчёт `regional-knowledge-base@13c53548 docs/reports/illustration-archive-live-acceptance-20261019.md`. Код ранжирования в пакете отсутствует.
- **Ущерб.** Потребитель, берущий top-1, ошибётся.
- **Доработка.** Небольшой размеченный eval-набор без приватного контента в публичном репозитории. Метрики recall@k и MRR. Контракт «смотреть top-k» для потребителя.
- **Проверка.** Повторяемый offline eval; порогов без замера не задаю.
- **Порядок.** После A13.

<a id="rkb-a15"></a>

**RKB-A15 · P2 · кодовый риск · уверенность средняя**
- **Сценарий.** Глобальный лимит 256 незавершённых jobs общий для `document` (индексатор) и `query`. Бэклог индексации или один актор отклоняет пользовательские запросы с `BGE queue full`.
- **Обоснование.** [bge_queue.py:121-122](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L121-L122). Приоритет claim в пакете не виден (строки 127–208 отсутствуют).
- **Доработка.** Раздельные квоты или резерв под query, per-actor лимит.
- **Проверка.** Нагрузочный тест на staging/fake: при заполнении document-jobs query принимаются.
- **Порядок.** Независимо.

<a id="rkb-a16"></a>

**RKB-A16 · P2 · гипотеза · уверенность низкая**
- **Сценарий.** Run в `failed` истекает через 60 с, jobs возвращаются в pending, `_ensure` создаёт новый run. При устойчивой ошибке провайдера возможен цикл холодных стартов без экспоненциального backoff.
- **Обоснование.** [bge_queue.py:80](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L80), `86-90`, `97-101`. Код `_new_run` и launch отсутствует.
- **Доработка.** Backoff или circuit breaker на подряд идущие failed.
- **Проверка.** Чтение кода launch; fake-тест последовательных failed.
- **Порядок.** Независимо.

<a id="rkb-a18"></a>

**RKB-A18 · P2 · кодовый риск · уверенность средняя**
- **Сценарий 1.** `OriginalCache.access` держит exclusive flock полосы (64 полосы) на всё время `loader`. Это опрос до 60×0.5 с плюс asset GET с timeout 120 с. Несвязанные оригиналы той же полосы ждут.
- **Сценарий 2.** Ожидание flock идёт в `asyncio.to_thread`. При параллельной нагрузке ожидающие занимают пул потоков по умолчанию и тормозят `hash_file` и `copyfile`.
- **Обоснование.** [original_cache.py:122-129](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L122-L129); [source_archive.py:85-88](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L85-L88); [illustration_mirror.py:45](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L45).
- **Доработка.** Отдельный ограниченный executor для блокировок, lock per-sha на время загрузки. Чисел без замера не даю.
- **Проверка.** Нагрузочный тест с fake loader: латентность несвязанного sha той же полосы.
- **Порядок.** После A03.

<a id="rkb-a19"></a>

**RKB-A19 · P2 · кодовый риск · уверенность высокая**
- **Сценарий.** Полный источник читается в память: `get_bytes`, `response.content`, `write_bytes`. Параллельные архивации или выдачи крупных PDF/DjVu дают пик RSS.
- **Обоснование.** [source_archive.py:201](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L201), `104`, `136-138`; [illustrations.py:120](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L120).
- **Доработка.** Потоковая передача с инкрементальным sha.
- **Проверка.** Замер RSS на тестовом файле. Цифр нет.
- **Порядок.** Независимо.

<a id="rkb-a32"></a>

**RKB-A32 · P2 · улучшение · уверенность высокая**
- **Сценарий.** Байтовая доступность доказана для 8 PNG и 4 PDF. Для остальных 143 иллюстраций сверены только метаданные.
- **Ущерб.** Невыявленные недоступные entry.
- **Доработка.** Поэтапная выборочная byte-проверка по решению владельца: rate-limited, с учётом FloodWait и диска (A01), без PUT и переимпорта. Учесть, что каждый GET создаёт operation в Vibe.
- **Проверка.** Доля успешных sha-совпадений в выборке. Не экстраполировать как гарантию.
- **Порядок.** После A01, A04, A05.

<a id="rkb-a36"></a>

**RKB-A36 · P2 · доказан (состояние) + улучшение · уверенность средняя**
- **Сценарий.** Все активные crop без `crop_object_id` означают, что единственная копия лежит в архиве Vibe/Telegram. После истечения временного asset Vibe каждое чтение снова идёт в Telegram.
- **Обоснование.** [illustrations.py:98-102](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L98-L102), `112-122`.
- **Ущерб.** Сбой Telegram или Vibe делает недоступными все crop.
- **Доработка.** Небольшой ограниченный локальный кэш crop по `provider_sha`, по образцу `OriginalCache`, только при достаточном запасе диска.
- **Проверка.** Hit-rate и доля `telegram_archive` в событиях `illustration_crop_read`.
- **Порядок.** После A01 и A04.

###### P3

<a id="rkb-a07"></a>

**RKB-A07 · P3 · кодовый риск · уверенность средняя**
- **Сценарий.** Выборка mirror ограничена 4 элементами (`order by mirror_attempt_at nulls first`). Элементы в cooldown не обновляют `attempt_at` и остаются первыми. При 4 и более в FloodWait новые crop ждут до 15 минут на цикл.
- **Обоснование.** [illustration_mirror.py:218-221](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L218-L221), `228-229`, `265-266`.
- **Доработка.** Исключать не-due элементы в SQL или сортировать по `next_due`.
- **Проверка.** Тест с 5 элементами в cooldown.
- **Порядок.** С A06.

<a id="rkb-a09"></a>

**RKB-A09 · P3 · кодовый риск · уверенность высокая**
- **Сценарий.** При чтении из архива MIME жёстко задан как `image/png`. Не-PNG crop получит неверный тип.
- **Обоснование.** [illustrations.py:102](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L102), `135`.
- **Доработка.** Определять тип по сигнатуре или сохранять MIME.
- **Проверка.** Тест с JPEG.
- **Порядок.** Независимо.

<a id="rkb-a17"></a>

**RKB-A17 · P3 · кодовый риск · уверенность средняя**
- **Сценарий.** После retention завершённых query-jobs опрос возвращает `PermissionError('BGE job not found')`. Это неотличимо от отказа доступа.
- **Обоснование.** [bge_queue.py:212-213](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L212-L213), `236-238`.
- **Доработка.** Отдельный код `expired`, не раскрывающий чужие jobs.

<a id="rkb-a20"></a>

**RKB-A20 · P3 · кодовый риск · уверенность средняя**
- **Сценарий.** Оригинал больше 512 MiB полностью скачивается и только потом отклоняется. Это повторяется при каждом запросе, и без копии тоже.
- **Обоснование.** [original_cache.py:129-131](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L129-L131).
- **Доработка.** Ранняя проверка известного размера объекта.

<a id="rkb-a21"></a>

**RKB-A21 · P3 · гипотеза · уверенность низкая**
- **Сценарий.** Понижение flock EX→SH не атомарно в Linux. В окне `cleanup` может удалить только что установленный файл, и тогда yield отдаст отсутствующий path.
- **Обоснование.** [original_cache.py:137](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L137), `106-112`.
- **Доработка.** Повторная проверка `path.exists()` после SH или удержание EX до копии.

<a id="rkb-a22"></a>

**RKB-A22 · P3 · улучшение**
- **Сценарий.** Файл из кэша на hit не перепроверяется (размер или sha).
- **Обоснование.** [original_cache.py:126](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L126), `136`; [source_archive.py:160](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L160).
- **Доработка.** Хотя бы сверка размера с `entries.size`.

<a id="rkb-a23"></a>

**RKB-A23 · P3 · кодовый риск (только OAuth-путь, вероятно legacy) · уверенность средняя**
- **Сценарий.** `grant.new` записывается с umask до `chmod`, то есть есть окно чтения refresh-токена. Нет fsync до `replace`. Сбой между ответом `/token` и записью ведёт к replay старого refresh и отзыву семейства токенов.
- **Обоснование.** [illustration_mirror.py:80-97](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L80-L97).
- **Доработка.** `os.open(..., 0o600)` с fsync, если OAuth-путь ещё используется.
- **Порядок.** Только если `auth_mode != local_service` где-либо активен.

<a id="rkb-a24"></a>

**RKB-A24 · P3 · кодовый риск**
- **Сценарий 1.** Проверка grant-файла смотрит только на mode: нет проверки владельца и symlink.
- **Сценарий 2.** Долгоживущий клиент mirror не подхватит ротацию `service_token` без рестарта.
- **Обоснование.** [illustration_mirror.py:18-22](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L18-L22).

<a id="rkb-a25"></a>

**RKB-A25 · P3 · кодовый риск**
- **Сценарий.** Disk guard mirror измеряет ФС каталога corpus SQLite, предполагая общую ФС с Vibe. Без атрибута `corpus` guard молча отключён.
- **Обоснование.** [illustration_mirror.py:235-237](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L235-L237).
- **Доработка.** Явный путь state-тома Vibe; fail-closed при отсутствии.

<a id="rkb-a26"></a>

**RKB-A26 · P3 · улучшение (наблюдаемость)**
- **Сценарий.** `mirror_error_type`/`source_archive_error` хранят имя класса. `RuntimeError` склеивает разные состояния: readback missing, not verified, digest changed, дубликаты entry. Тупиковые состояния (PUT не verified, 2 и более совпадений origin) крутятся без алерта.
- **Обоснование.** [illustration_mirror.py:203-204](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L203-L204), `246-247`, `287-292`, `296`; [source_archive.py:286](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L286).
- **Доработка.** Стабильные коды ошибок и метрика «застрявшие более N tick».

<a id="rkb-a27"></a>

**RKB-A27 · P3 · улучшение**
- **Сценарий.** Descriptor отдаёт MCP-клиенту `vibepublish_entry_ref`, то есть внутренний provider ref.
- **Обоснование.** [illustrations.py:40](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L40).
- **Доработка.** Минимизация полей, если потребителям ref не нужен.

<a id="rkb-a28"></a>

**RKB-A28 · P3 · кодовый риск**
- **Сценарий.** SSE-парсинг берёт первую строку `data:`. Многострочный или многособытийный ответ ломается, а отсутствие строки даёт `StopIteration` внутри корутины, то есть неинформативную ошибку.
- **Обоснование.** [illustration_mirror.py:137](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L137).

<a id="rkb-a29"></a>

**RKB-A29 · P3 · кодовый риск**
- **Сценарий.** Связывание подписей молча усекает данные до 8 страниц и 200 строк на страницу без сортировки.
- **Обоснование.** [illustrations.py:66](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L66), `74`.
- **Доработка.** Признак усечения в ответе.

<a id="rkb-a30"></a>

**RKB-A30 · P3 · кодовый риск**
- **Сценарий.** `SourceArchive.update` не проверяет `u.status='active'`, в отличие от fence в mirror. Отзыв владельца в середине tick не останавливает запись метаданных.
- **Обоснование.** [source_archive.py:179-181](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L179-L181). Для сравнения: [illustration_mirror.py:211](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L211).

<a id="rkb-a31"></a>

**RKB-A31 · P3 · гипотеза**
- **Сценарий.** `_ensure` читает `control`, затем пишет. Если `connect()` очереди открывает отложенную транзакцию, параллельные enqueue получат SQLITE_BUSY при апгрейде блокировки. Это ошибка запроса, а не двойной run.
- **Обоснование.** [bge_queue.py:93-101](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L93-L101), `114`. `connect` очереди в пакете нет.

<a id="rkb-a33"></a>

**RKB-A33 · P3 · гипотеза**
- **Сценарий.** У corpus SQLite `busy_timeout` 15 с. Режим WAL в пакете не виден. Параллельные индексатор, API и mirror могут дать `database is locked`.
- **Обоснование.** [sqlite_corpus.py:121-128](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/sqlite_corpus.py#L121-L128).
- **Проверка.** Read-only `PRAGMA journal_mode`, счётчик ошибок locked.

<a id="rkb-a34"></a>

**RKB-A34 · P3 · улучшение (покрытие)**
- **Сценарий.** Ветка legacy DjVu `source.pdf`/`source.djvu` при `idempotency_conflict` сложна, но в списке тестов её нет. Неизвестно, покрыта ли она.
- **Обоснование.** [source_archive.py:210-251](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L210-L251).

##### Узкие места (качественно, без чисел)

- Внутри одного MCP-вызова: опрос receipt до 60×0.5 с, затем asset GET с timeout 120 с ([source_archive.py:41](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L41),85-88; [illustration_mirror.py:45](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L45)). Таймаут MCP-клиента неизвестен.
- Новый `VibePublishClient` и httpx-клиент на каждое чтение crop, без лимита параллелизма ([illustrations.py:117-122](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L117-L122)).
- Telegram как единственная копия crop после истечения asset (A36), плюс FloodWait (A05, A06).
- Блокировки по 64 полосам и пул потоков (A18). Полные файлы в RAM (A19).
- Mirror обрабатывает 4 элемента за tick с head-of-line эффектом (A07).
- Холодный старт BGE до 15 мин и общая очередь 256 (A13, A15).
- Диск: запас около 0.36 GiB до резерва (A01).

##### Уже защищено, повторно не исправлять

- Детерминированные ключи PUT и `Idempotency-Key` asset: [illustration_mirror.py:239](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L239), `153-154`; [source_archive.py:203-205](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L203-L205). Lost reply PUT безопасен.
- Durable `request_key` до I/O, переиспользование при конкуренции и рестарте, compare-and-delete: [original_cache.py:60-89](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L60-L89). Сеть, auth и неизвестные ошибки не создают новый operation. Повтор после 404/410 ограничен двумя чтениями ([source_archive.py:63-69](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L63-L69), `96-103`). Всё это подкреплено перечисленными тестами.
- Повторная авторизация актора после получения байтов через `descriptor`: [illustrations.py:131](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L131). Отзыв ACL в середине вызова не отдаёт данные.
- Цепочки sha: [illustrations.py:110](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustrations.py#L110), `124-130`; [source_archive.py:107](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L107), `128`, `137`, `277`; [sqlite_corpus.py:141-144](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/sqlite_corpus.py#L141-L144).
- Fence ревизии, владельца и статуса в mirror: [illustration_mirror.py:211](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L211). Проверки владельца оригиналов: [source_archive.py:123](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L123), `135`, `149-151`.
- Same-host граница:
  - строгая loopback-привязка `127.0.0.1` ([illustration_mirror.py:34-42](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/illustration_mirror.py#L34-L42));
  - `trust_env=False` (`:45`);
  - bearer только на путях закреплённого issuer (`:109-110`);
  - service bearer не попадает в OAuth refresh (`:52-53`).
- Кэш: приватные права, сборка partial только под lock, лимит ёмкости ([original_cache.py:22-27](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/original_cache.py#L22-L27), `95-100`, `131`, `148`).
- BGE: тот же job ID на холодном старте, предел прогрева 900 с, mismatch идемпотентного payload, результаты в scope актора ([bge_queue.py:85](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L85), `115-120`, `212-213`).
- Дедупликация источников по sha ([source_archive.py:165](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/source_archive.py#L165), `280-283`). Отложенный admission mirror по диску (`:236`).
- Не переквалифицировать как текущие дефекты:
  - PR146 смержен;
  - PR88 поднимает только wall-бюджет CI, продуктовые таймауты не меняются;
  - PR87 — draft, не деплоить;
  - POI `unresolved_identity` не равен принятой идентичности.

---

#### Edge cases

| Сценарий | Поведение по коду | Оценка |
|---|---|---|
| Crash после PUT до сохранения `operation_id` | replay того же ключа | защищено |
| Crash после admission readback GET | новый GET без ключа | A05 |
| Crash в `archive_payload` до `archive_admitted` | тот же `request_key` | защищено (тест) |
| Crash после получения payload до install | receipt сохранён | защищено (тест) |
| Crash при install или при refresh OAuth | partial собирается; refresh: риск | защищено / A23 |
| Рестарт или потеря BGE worker | heartbeat/lease → jobs pending, тот же ID | защищено; recovery зависит от частоты maintenance (не видна) |
| Lost reply asset GET (сеть) | operation сохраняется | защищено |
| Cancel MCP-вызова при опросе, загрузке или в `access` | durable state в кэш-БД, `temp` удаляется в `finally`, клиент закрывается | защищено |
| Истёк временный asset | один новый keyed GET | защищено |
| Истекла идемпотентность или receipt в Vibe | conflict, вечный тупик | A11 (гипотеза) |
| Terminal transient сбой чтения | вечный `archive_source_unavailable` | A04 |
| Истёк done-job BGE | «not found» как PermissionError | A17 |
| Отзыв ACL актора | descriptor-перепроверка | защищено |
| Отзыв service token | 401/403 без нового operation | защищено |
| Деактивация владельца | mirror fence, проверка в source tick; `update` source без fence | защищено / A30 |
| Мало места | mirror откладывает; source PUT, кэш, copy не проверяют | A01–A03 |
| Параллельная нагрузка | полосы lock, пул потоков, очередь 256, busy 15 с, RAM | A15, A18, A19, A31, A33 |
| FloodWait | cooldown обходится после постороннего исключения | A06, A07 |

---

#### Recommendation

Порядок, если владелец решит действовать:

1. **A01**: ёмкость и мониторинг. Без этого остальное вторично.
2. **A02, A03**: disk guard для всех писателей.
3. **A04** вместе с **A06/A07**: единая transient-политика. Перед этим закрыть гипотезу **A11** чтением кода Vibe.
4. **A05**: ключи readback GET.
5. **A13**, затем **A14**: признак degraded при холодном старте BGE и eval качества.
6. **A08, A10, A15, A36, A32**.
7. P3 по мере касания кода.

Я не согласен с любым прочтением 151/151 или 2959/2959 как доказательства массовой доступности или качества. Это доказательство согласованности метаданных и полноты индекса. Новый gateway, расширение прав, prod-удаления и schema downgrade не требуются ни для одного пункта.

---

#### Confidence

- **Высокая:** A01–A06, A09, A19 и список защищённого. Следуют прямо из строк пакета.
- **Средняя:** A07, A08, A10, A13 (наличие флага), A14, A15, A18.
- **Низкая (гипотезы):** A11, A12, A16, A21, A31, A33.

**Непроверяемо из пакета:**
- RLS-политики `rkb_illustrations` и `rkb_documents`.
- Эмуляция `data_client._connection` и PostgreSQL-синтаксиса поверх SQLite.
- Код claim, complete и launch BGE ([bge_queue.py:1-77](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/481241fa36db30e3f0e6c4c4b88fb3ce84964676/src/regional_knowledge/bge_queue.py#L1-L77), `127-208`).
- Ранжирование и lexical fallback поиска.
- Сторона Vibe: retention ключей и receipts, TTL assets, ENOSPC, FloodWait.
- Кто и когда удаляет crop-объекты (условие «держать до проверенного SHA»).
- Частота maintenance и `cleanup`.
- Таймауты MCP-клиента, лимиты systemd, режим WAL, резервные копии.
- Реализации тестов (видны только имена).
- Полное содержимое приёмочного отчёта.
- Идентификационная часть Street Story вне scope.

## Полный текст Claude — часть VibePublish

**Модель:** Claude Opus 5.5 (`claude-opus-5-5`). Платформа сообщает только о запуске через Claude Agent SDK, поэтому называть это Claude Code CLI я не буду. Использован только переданный пакет: файлы, хост, сеть и логи не читались, код не менялся.

Ссылки ниже даны в форме `VibePublish@d1f880b <file>:<line>`. Полный SHA: `d1f880b24788f609398a660ba53dd8c4731b7aec`, схема 11.

---

### Аудит VibePublish: media GET, legacy grants, cache, FloodWait/UNKNOWN, leases, SQLite/disk, соседство со Street Story

#### 0. Границы и как читать доказательную базу

- **Каталожный аудит 151/151 и 8 свежих скачиваний подтверждают разное.**
  - 151/151 — это согласование метаданных: tenant, topic, origin.ref и provider `download_sha256`.
  - 8 PNG и 4 PDF — это фактические байты, прошедшие через GET.
  - Из 34 грантов реальным скачиванием проверены только 3 legacy-изображения и 4 PDF, то есть 7 из 34. Остальные 27 грантов проверены пересчётом proof без скачивания.
  - Доступность файлов в Telegram для этих 27 не проверялась.
- **Успешная приёмка не гарантирует массового качества.** 119 сфокусированных тестов и 8 скачиваний показывают корректность проверенных путей. Они ничего не говорят о поведении под нагрузкой, при нехватке диска или при длительной эксплуатации.
- **P0 не найдено.** Ни один пункт ниже не означает утечку доступа или повторную публикацию в текущем состоянии. Защиты от отзыва прав, подмены байтов и повторной отправки UNKNOWN в коде присутствуют и покрыты тестами (раздел 4).

---

#### 1. Findings — реестр

<a id="vp-a01"></a>

##### VP-A01 · P1 · кодовый риск · уверенность: высокая (код), средняя (масштаб)
**Download-cache пишет полные байты в основную SQLite без проверки свободного места, лимита размера и дедупликации.**

- **Сценарий.** Потребитель (RKB-зеркало, ручной аудит или повторы клиента) делает серию GET крупных PDF или многократные GET одной записи.
  - Каждый GET вставляет **новую** строку `assets` с полным BLOB и TTL 3600 с.
  - Повторного использования ещё живого кэша того же `(principal, entry, sha)` нет.
- **Триггер.** Свободного места ~2.36 GiB, live Vibe SQLite ~4.54 GB, WAL. На том же хосте RKB и Street Story.
- **Обоснование.**
  - Вставка без проверки места: VibePublish@d1f880b [social_operations/worker.py:1040-1047](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1040-L1047).
  - Для PDF/DjVu нет размерной проверки, аналогичной `verify_image`: [worker.py:1033-1037](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1033-L1037).
  - Резерв на диске есть только у RKB-зеркала (2 GiB, из пакета). У Vibe аналогичной проверки в показанном коде нет.
- **Ущерб.**
  - `SQLITE_FULL` для всех писателей Vibe.
  - При общей ФС — нехватка места у соседних сервисов, включая Street Story. Общая ли ФС, из пакета не видно.
  - После purge файл БД не сжимается (страницы уходят во freelist). Пиковый рабочий набор остаётся занятым на диске.
- **Минимальная доработка.**
  - До admission и до вставки проверять `statvfs` с порогом «резерв + `evidence.size`». Размер известен из `observed_media` ещё до скачивания.
  - Ввести квоту download_cache на principal (суммарные байты).
  - Если есть неистёкший cache-asset с тем же `entry_ref` и sha, возвращать его, а не вставлять копию.
  - Явный максимум размера документа.
- **Не делать:** VACUUM или копирование сырой БД на почти полном диске. VACUUM требует порядка размера БД свободного места.
- **Проверка и метрика.** Read-only `mode=ro` или тестовая среда: `count` и `sum(length(bytes))` по `media_store_assets.purpose='download_cache'` в разрезе principal; `PRAGMA freelist_count`/`page_count`; размер WAL; свободное место до и после серии GET. Тест: GET при искусственно малом порог-резерве должен дать доменную ошибку до скачивания.
- **Порядок.** Первым, раньше A02 и A04.

<a id="vp-a02"></a>

##### VP-A02 · P2 · кодовый риск · уверенность: средняя
**Отказ из-за места или I/O SQLite не отделён от contention и может уронить worker.**

- **Сценарий.** `database or disk is full` или `disk I/O error` при claim либо purge. Обработчик ловит только `locked` ([worker.py:191-197](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L191-L197)) и пробрасывает остальное из `run_once`.
  - В `run_read` такая ошибка попадает в `except Exception` и вызывает `fail_operation` ([worker.py:275-276](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L275-L276)). Тому тоже нужна запись, и он может упасть повторно.
- **Ущерб.**
  - Возможен crash/restart-цикл процесса. Политика рестарта и внешний цикл в пакете не показаны.
  - Байты Telegram уже скачаны, но не сохранены. Трафик потрачен, ответ клиенту не дан.
- **Доработка.**
  - Классифицировать `SQLITE_FULL`/`IOERR` как отдельное деградированное состояние: пауза claim с backoff и метрика вместо исключения наружу.
  - В `run_read` проверять место до скачивания (связано с A01).
- **Проверка.** Unit-тест с подменённым `store.tx`, бросающим `OperationalError('database or disk is full')`. Ожидается: процесс жив, операция остаётся durable и не помечена ложным терминальным состоянием.
- **Зависимость.** После A01 или вместе с ним.

<a id="vp-a03"></a>

##### VP-A03 · P2 · кодовый риск · уверенность: высокая
**Purge в write-транзакции на каждом пользовательском вызове `read_asset` и `media_store`.**

- **Обоснование.** [service.py:87-88](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L87-L88) и [service.py:142-143](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L142-L143) берут `BEGIN IMMEDIATE`, а при наличии истёкших строк ещё и удаляют BLOB. Worker уже делает purge раз в 60 с ([worker.py:181-184](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L181-L184)).
- **Сценарий.** Параллельные list, search, get и read_asset сериализуются на writer-lock вместе с worker. При длинной транзакции worker (A04) API ждёт до 5 с busy_timeout плюс 15 с цикла повторов ([storage.py:98-123](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L98-L123)).
- **Ущерб.** Рост латентности API и ошибки `locked` под параллельной нагрузкой, даже на чисто читающих командах.
- **Тонкость.** Purge сейчас и есть механизм соблюдения TTL при чтении. Убирать его из пути чтения можно только вместе с явной проверкой `expires >= now` в `read_asset`.
  - Заодно закрывается узкая гонка: в [service.py:87-89](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L87-L89) purge и чтение идут в разных соединениях.
- **Доработка.**
  - Фильтрация `expires` на чтение.
  - Удаление — только в worker или при дешёвом read-only предикате «есть что удалять».
- **Метрика.** Число `BEGIN IMMEDIATE` на API-запрос и время ожидания writer-lock в API (замерить, не предполагать).

<a id="vp-a04"></a>

##### VP-A04 · P2 · кодовый риск · уверенность: высокая
**Тяжёлая работа внутри writer-транзакции `run_read`.**

- **Обоснование.** В одной `BEGIN IMMEDIATE` ([worker.py:1010](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1010)) выполняются:
  - SHA всех байтов в `check_download` ([media_entry_grants.py:185](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L185));
  - повторный SHA ([worker.py:1030](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1030));
  - `verify_image`, то есть декодирование и санитизация ([worker.py:1039](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1039));
  - вставка BLOB при `synchronous=FULL` ([storage.py:102](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L102));
  - `_media_store_items` ([worker.py:1074](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1074)).
- **Ущерб.** Writer-lock удерживается пропорционально размеру файла. Остальные писатели Vibe ждут (A03, A05).
- **Доработка.**
  - Хеши и `verify_image` выполнять до `BEGIN IMMEDIATE`.
  - Внутри транзакции оставить fence, повторный `require_grant`, сравнение заранее посчитанного хеша с proof и insert.
  - Семантика fail-closed при отзыве гранта не меняется: повторная проверка остаётся внутри транзакции.
- **Проверка.** Существующие тесты 216/236/180 (revoke during download, wrong bytes) должны пройти без изменений. Метрика — длительность транзакции `run_read` по размерам файлов.

<a id="vp-a05"></a>

##### VP-A05 · P2 · кодовый риск · уверенность: средняя
**Синхронное ожидание блокировки в async-worker может задушить heartbeat и lease.**

- **Обоснование.**
  - `time.sleep(0.05)` в цикле до 15 с: [storage.py:115-123](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L115-L123).
  - `busy_timeout=5000` блокирует C-вызов: [storage.py:101](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L101).
  - `tx()` вызывается из корутин: [worker.py:1010](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1010), `374`, `406`.
  - Event loop стоит, задача `heartbeat` ([worker.py:203](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L203)) не выполняется.
- **Сценарий.** Длинная транзакция API или другого процесса совпадает с `run_read` или `schedule_*`. Если lease короче суммарного ожидания (длительность lease в пакете не видна), lease истекает, другой worker забирает операцию, у первого fence даёт `stale_worker`, и работа выполняется повторно.
  - Для read это повторное скачивание. Для PUT защищает fence и правило «observe before resend».
- **Доработка.** Ожидание блокировки — через `asyncio.to_thread` или неблокирующий цикл с `await asyncio.sleep`. Либо гарантировать, что lease заметно превышает 5 + 15 с.
- **Проверка.** Тест: удерживать внешний writer-lock N секунд и проверить, что heartbeat продолжает продлевать lease, а `stale_worker` не возникает.

<a id="vp-a06"></a>

##### VP-A06 · P2 · доказан (по коду) · уверенность: высокая
**Ожидание lane не ограничено deadline, а бюджет считается до захвата lane.**

- **Обоснование.**
  - `budget = min(budget, op['deadline'] - clock)` вычисляется в [worker.py:992-993](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L992-L993).
  - Затем `async with self.lane(...)` без таймаута ([worker.py:995](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L995)).
  - `asyncio.timeout(budget)` начинает действовать только после захвата ([worker.py:1000](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1000)).
- **Сценарий.** Lane соединения занят чужим чтением или публикацией (до 120 с на GET). Чтение ждёт неограниченно и затем получает полный бюджет, то есть может выполняться после deadline операции.
  - Worker, обслуживающий несколько соединений, в это время блокирован (head-of-line blocking).
- **Ущерб.** Нарушена семантика deadline. Операции других соединений голодают.
- **Доработка.**
  - Ограничить захват lane оставшимся до deadline временем. Тайм-аут здесь безопасен: provider-эффекта ещё нет, ошибка `provider_read_deadline`.
  - После захвата пересчитывать бюджет.
- **Проверка.** Тест с занятым lane и коротким deadline: ожидается доменная ошибка до вызова adapter и ни одного вызова `read`.

<a id="vp-a07"></a>

##### VP-A07 · P2 · кодовый риск (влияние на соседей — гипотеза) · уверенность: средняя
**GET-скачивания удерживают lane соединения до 120 с и не учитываются ни в каком бюджете.**

- **Обоснование.**
  - Бюджет 120 с для `_media_store_get`: [worker.py:992](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L992).
  - Скачивание внутри lane: [worker.py:995-1003](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L995-L1003).
  - `media_budget.media_files` считает только `publish`/`edit` ([media_budget.py:26-38](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_budget.py#L26-L38)), downloads не учитываются.
- **Сценарий.** Пакетная проверка или массовый пере-GET архива (например, при повторении каталожного прохода с реальным скачиванием). Lane Telegram-соединения занят длинными чтениями.
  - Если Street Story публикует через то же соединение, её публикации ждут.
  - FloodWait на скачивание (`upload.getFile`) не моделируется.
- **Неизвестно из пакета:** общее ли у Street Story соединение и как adapter обрабатывает FloodWait внутри read — спит ли, удерживая lane.
- **Доработка.**
  - Отдельный лимит одновременных и скользящих GET на principal.
  - Публикационная работа приоритетнее фоновых GET или идёт по отдельной очереди.
  - Для массовых проходов — пейсинг на стороне потребителя.
- **Метрика (замерить).** Время ожидания lane по workload; латентность публикаций Street Story во время серии GET; число FloodWait на read.
- **Не предлагаю:** дополнительный gateway или второй Telegram-аккаунт.

<a id="vp-a08"></a>

##### VP-A08 · P2 · доказан (поведение по коду, by design) · уверенность: высокая
**Любой bump epoch необратимо инвалидирует все entry-гранты principal, а восстановление трудоёмко и плохо диагностируется.**

- **Обоснование.**
  - Proof содержит `principal_epoch` и `binding_epoch`: [media_entry_grants.py:97-99](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L97-L99).
  - Отключение **любого** topic-гранта делает `epoch+1` ([storage.py:304-307](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L304-L307)). Например, снятие topic2 ломает и все 30 грантов topic4.
  - Гранты неизменяемы: [media_entry_grant_schema.sql:13-15](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grant_schema.sql#L13-L15).
  - `UNIQUE(tenant, principal, entry_ref)` (`:10`) приводит к тому, что `apply_entry_grants` при существующем гранте с другим proof отвечает `_denied` ([media_entry_grants.py:153-155](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L153-L155)). Нужен revoke по id, затем regrant.
  - Не больше 4 proofs за вызов (`:131`), то есть для 34 грантов не меньше 9 owner-вызовов.
  - Ошибка для потребителя — общий `access_denied/contact_owner`.
- **Оценка.** С точки зрения безопасности поведение правильное (fail-closed, тест [test_media_entry_grants.py:83](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_entry_grants.py#L83)). Риск операционный: незаметная массовая потеря доступа и ручное восстановление.
- **Доработка.**
  - Read-only owner-диагностика: «гранты, у которых пересчитанный proof ≠ сохранённого», с причиной (epoch, binding, fact или evidence).
  - Отдельный код `entry_grant_stale` только для держателя гранта (entry_ref он уже знает, утечки нет).
  - Явно задокументировать, что отключение topic инвалидирует entry-гранты.
- **Не предлагаю:** автоматический regrant или ослабление привязки к epoch.

<a id="vp-a09"></a>

##### VP-A09 · P2 · кодовый риск · уверенность: средняя
**Для origin с `sha256` грант не сверяет origin.sha256 с хешем медиа провайдера.**

- **Обоснование.**
  - `entry_proof` проверяет только формат sha: [media_entry_grants.py:85-87](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L85-L87).
  - При скачивании ожидаемый хеш — `origin.sha256`: `proof_sha256`, `:51-55`; `check_download`, `:185`.
  - Тесты [test_media_store.py:305](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_store.py#L305)/`:321` показывают, что origin.sha256 по дизайну может не совпадать с байтами провайдера.
- **Сценарий.** Owner выдаёт грант на такую запись. Грант принимается, но каждый GET скачивает файл (трафик и lane) и затем отказывает с `access_denied`.
- **Текущее состояние.** Для 4 PDF совпадение подтверждено фактически, сейчас дефекта нет.
- **Доработка.** При выдаче гранта требовать `origin.sha256 ∈ media_hashes/observed_media` последнего fact. В `check_download` при расхождении хеша отдавать `asset_integrity`, а не `access_denied`.
- **Проверка.** Тест: грант на запись с расходящимся origin.sha256 отклоняется на этапе выдачи, без provider I/O.

<a id="vp-a10"></a>

##### VP-A10 · P2 · кодовый риск · уверенность: средняя
**GET собственной записи не проверяет идентичность скачанного элемента запрошенной записи.**

- **Обоснование.**
  - Строгая проверка `check_download` (native_id, namespace, topic, provider_ref, members, sha) выполняется только при `entry_grant`: [worker.py:1014-1019](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1014-L1019).
  - Для собственных записей проверяются лишь внутренняя согласованность readback ([worker.py:1023-1032](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1023-L1032)) и target (`:1062`).
  - Кэш привязывается к `_media_store_entry_ref` без сверки `page.items[*].native_id == args['_native_item']` (`:1046-1047`).
- **Сценарий.** Регрессия adapter, сообщение удалено, или пришла страница из нескольких элементов. В кэш записи попадают байты другого сообщения либо несколько asset.
- **Доработка.** Применять ту же проверку (кроме сверки с хешем гранта) для всех `_media_store_get`. Ожидается ровно один элемент с `native_id`/`namespace`/topic из intent и provider_ref из текущего fact. Расхождение с последним известным `media_hashes` выносить в результат явно.
- **Проверка.** Параметризовать тест [test_media_entry_grants.py:236](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_entry_grants.py#L236) на собственные записи.

<a id="vp-a11"></a>

##### VP-A11 · P2 · кодовый риск · уверенность: высокая
**GET без `request_key` не идемпотентен.**

- **Обоснование.**
  - Гранты всегда создают durable-операцию ([service.py:195](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L195)), но ключ привязывается только при наличии `request_key` (`:216-217`).
  - Собственный GET без ключа идёт через синхронный `self.read` (`:219-220`, `:245-247`).
- **Сценарий.** Ответ потерян, клиент повторяет запрос. Получается новая операция, новое скачивание и новая BLOB-копия (усиливает A01 и A07).
- **Доработка.** Для не-owner principal требовать `request_key` в GET. Либо дедуплицировать по `(principal, entry_ref, grant_id)` при живом кэше.
- **Проверка.** Тест: два GET без ключа подряд дают одно provider-чтение либо понятную ошибку `fix_input`.

<a id="vp-a12"></a>

##### VP-A12 · P2 · кодовый риск · уверенность: средняя
**Media budget: нет FIFO и раздела по principal, deadline при отсрочке продлевается.**

- **Обоснование.**
  - Лимит общий на `connection_id` ([media_budget.py:56-62](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_budget.py#L56-L62)).
  - Порядка ожидающих нет. Все отложенные получают одинаковый ранний `retry_at` (`:62-68`, [worker.py:412](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L412)).
  - `deadline = MAX(deadline, retry_at + 120)` ([worker.py:413-416](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L413-L416)) продлевается при каждой отсрочке.
- **Сценарий.** Поток одиночных PUT занимает окно. Альбом из N файлов (требует N свободных слотов) раз за разом проигрывает гонку. Это голодание; deadline растёт.
  - RKB-PUT и публикации Street Story, если соединение общее, конкурируют без справедливости.
- **Доработка.**
  - Учитывать время первой отсрочки: старшие операции имеют приоритет на окно.
  - Счётчик отсрочек в result.
  - Верхняя граница суммарного продления deadline.
  - Опционально — доли по principal.
- **Метрика.** Максимальный `media_store_retry_count` и число отсрочек на операцию; возраст самой старой отложенной операции.
- **Не трогать:** консервативный учёт прерванного pre-dispatch ([media_budget.py:5-6](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_budget.py#L5-L6)). Он правильный.

<a id="vp-a13"></a>

##### VP-A13 · P3 · улучшение · уверенность: высокая
**Временные ошибки GET терминальны, клиент должен прийти с новым ключом.**

- **Обоснование.**
  - `run_read` возвращается раньше `schedule_media_store_retry` ([worker.py:218-220](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L218-L220) против `:267`).
  - Timeout становится `provider_read_deadline` (`:1005-1006`), тест [test_media_store.py:559](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_store.py#L559) фиксирует терминальность.
- **Риск.** Внешний ретрай-цикл потребителя во время FloodWait продлевает flood. Подсказки `retry_after` нет.
- **Доработка.** Добавить в ошибку read `retry_after`, если adapter его знает. Документировать для RKB-потребителя пейсинг и паузу.

<a id="vp-a14"></a>

##### VP-A14 · P3 · кодовый риск · уверенность: низкая–средняя
**Backoff для `telegram_cooldown` не учитывает срок, указанный провайдером.**

- **Обоснование.** `delay = min(300, 2**min(count, 8))` ([worker.py:394](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L394)), без связи с FloodWait.
- **Сценарий.** Ранние ретраи PUT попадают в ещё действующий cooldown. Если проверка cooldown локальная, это лишь лишняя работа. Если нет — дополнительные обращения.
- **Доработка.** `retry_at = max(backoff, cooldown_until)`.

<a id="vp-a15"></a>

##### VP-A15 · P3 · кодовый риск · уверенность: низкая
**`schedule_media_store_retry` переоткрывает операцию без fence.**

- **Обоснование.** UPDATE в [worker.py:398-401](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L398-L401) выполняется без `store.fence`, тогда как соседний `schedule_media_budget_retry` его вызывает (`:411`).
- **Сценарий.** Lease этого worker уже перехвачен (см. A05), а он всё равно переписывает state и lease. Вероятность низкая, так как вызов идёт сразу после `aggregate`.
- **Доработка.** Добавить `self.store.fence(db, op['id'], self.id, op['fence'])` для единообразия.

<a id="vp-a16"></a>

##### VP-A16 · P3 · гипотеза · уверенность: низкая
**Несколько fact на одну публикацию дают дубли или устаревшие записи в list/search, а равные `observed_at` дают недетерминированный выбор.**

- **Обоснование.**
  - `_media_store_items` соединяет все `facts` без выбора последнего ([service.py:266-274](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L266-L274)).
  - `_entry` и GET берут `ORDER BY observed_at DESC LIMIT 1` без tie-break ([media_entry_grants.py:24](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L24), [service.py:169](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L169), `:185`). Сама форма запроса намекает, что fact может быть несколько.
- **Проверить нельзя из пакета:** схему `facts` и `save_fact`. Тесты [test_media_store.py:164](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_store.py#L164)/`:180` частично это покрывают.
- **Безопасная проверка.** На тестовой копии или через `mode=ro`: `SELECT publication_id, count(*) FROM facts GROUP BY 1 HAVING count(*) > 1`.
- **Доработка при подтверждении.** Выбирать последний fact в `_media_store_items`, tie-break по `f.id`.

<a id="vp-a17"></a>

##### VP-A17 · P3 · кодовый риск (производительность) · уверенность: средняя
**list/search — полный скан с разбором JSON и запись операции на каждый вызов.**

- **Обоснование.**
  - Нет LIMIT в SQL, фильтр `casefold` в Python: [service.py:266-288](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L266-L288).
  - Каждый list/search создаёт complete-операцию с результатом: [service.py:237-238](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L237-L238), `:243-244`.
  - Вместе с A03 каждый вызов — две write-транзакции.
- **Ущерб.** Линейный рост цены вызова и рост БД. Чисел без замера не даю.
- **Доработка.** Ранний выход после `limit+1` совпадения (сортировка уже есть). Решить, нужна ли durable-операция для чистого чтения.

<a id="vp-a18"></a>

##### VP-A18 · P3 · гипотеза · уверенность: низкая
**Replay GET по `request_key` после revoke и повторной выдачи гранта не сравнивает grant_id.**

- **Обоснование.** На replay сверяются только `_binding_id` и `_binding_epoch` ([service.py:200-206](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L200-L206)), но не `_media_store_grant_id`. Revoke не меняет epoch ([media_entry_grants.py:166-171](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L166-L171)).
- **Почему низкая уверенность.** Тест [test_media_entry_legacy_origin.py:214](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_entry_legacy_origin.py#L214) («regrant cannot revive old receipt or cache») утверждает обратное, а `_replay` и `private_operation` в пакет не входят.
- **Защита кэша есть.** Связь `media_service_entry_assets` удаляется каскадом, и `read_asset` отказывает ([service.py:106-107](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L106-L107)).
- **Проверка.** Прочитать `_replay` и тест 214. При подтверждении добавить сравнение `saved['_media_store_grant_id']` с текущим грантом.

<a id="vp-a19"></a>

##### VP-A19 · P3 · улучшение · уверенность: высокая
**Revoke без аудита и без индикации результата.**

- **Обоснование.** `DELETE` без проверки `rowcount` и без возврата результата ([media_entry_grants.py:170-171](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L170-L171)). Неверный id — тихий no-op. Следа «кто и когда отозвал» не остаётся.
- **Доработка.** Возвращать `{revoked: bool}`. Писать append-only событие (без приватного содержимого).

<a id="vp-a20"></a>

##### VP-A20 · P3 · гипотеза · уверенность: низкая
**FK `entry_ref → publications(id)` без каскада.**

- **Обоснование.** [media_entry_grant_schema.sql:6](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grant_schema.sql#L6).
- **Сценарий.** Если какой-либо lifecycle-путь удаляет строку `publications` у записи с грантом, удаление упадёт с FK-ошибкой. Это fail-closed, но с неожиданным кодом.
- **Проверка.** Выяснить, удаляются ли когда-либо строки `publications` (из пакета не видно). **Не предлагаю** менять схему или удалять что-либо в prod.

<a id="vp-a21"></a>

##### VP-A21 · P3 · кодовый риск (хрупкость) · уверенность: средняя
**Legacy-proof привязан к `source_fact_id` и `observed_media` последнего fact.**

- **Обоснование.** [media_entry_grants.py:88-94](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L88-L94). При этом `run_read` перезаписывает fact после каждого GET ([worker.py:1064](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1064)).
- **Текущее состояние.** Сейчас стабильно: тест [test_media_entry_legacy_origin.py:51](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_entry_legacy_origin.py#L51), реальные 3 legacy-скачивания. Но любое будущее изменение `save_fact` в сторону «новая строка на refresh» массово и молча инвалидирует все 30 legacy-грантов.
- **Доработка.** Явный инвариант-тест «GET и refresh не меняют `fact_id` и `observed_media` при неизменном провайдере» плюс комментарий у `save_fact`.

<a id="vp-a22"></a>

##### VP-A22 · P3 · улучшение (покрытие) · уверенность: высокая
**27 из 34 грантов не проверены реальным скачиванием.**

- **Суть.** Proof пересчитан read-only, но доступность файла в Telegram и совпадение байтов для этих грантов фактически не проверялись.
- **Предложение (только с одобрения owner).** Редкая выборочная read-only GET-проверка с пейсингом (учитывая A07), с ключами и без новых грантов. Метрика — доля успешных выборочных GET.

<a id="vp-a23"></a>

##### VP-A23 · P3 · кодовый риск · уверенность: низкая
**Неоднозначный выбор binding при нескольких alias на один destination.**

- **Обоснование.** `next(...)` берёт первый подходящий binding: [service.py:187-188](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L187-L188), `:122-123`, `:282-283`. При разных topic-грантах на разные binding результат зависит от порядка строк.
- **Доработка.** Детерминированный выбор либо отказ при неоднозначности.

<a id="vp-a24"></a>

##### VP-A24 · P3 · улучшение · уверенность: высокая
**Фиксированный TTL кэша 1 ч и replay с «висящим» `asset_ref`.**

- **Обоснование.** [worker.py:1047](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L1047). Тест [test_media_store.py:658](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/tests/runtime/test_media_store.py#L658) подтверждает, что replay после истечения возвращает квитанцию без пере-скачивания, а `read_asset` отвечает `asset_not_available/refresh` ([service.py:95-96](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L95-L96)).
- **Риск.** Потребитель (зеркало RKB) обязан забрать байты в течение часа, иначе нужен новый GET с новым ключом.
- **Доработка.** Отдавать `expires_at` в квитанции и документировать контракт.

<a id="vp-a25"></a>

##### VP-A25 · P3 · улучшение · уверенность: высокая
**Длительность CI.** Python 3.12 упёрся в лимит 15 мин всей job, а не в assertion; PR88 поднимает лимит до 20 мин. Это не продуктовый дефект, но тренд стоит отслеживать (время по версиям Python) и рассмотреть шардинг, а не только рост лимита.

---

#### 2. Узкие места (сводка)

1. **Writer-lock SQLite.** На него завязаны:
   - purge в каждом API-вызове (A03);
   - тяжёлая транзакция `run_read` (A04);
   - durable-операции list/search (A17);
   - синхронное ожидание в event loop (A05).
2. **Lane Telegram-соединения.** До 120 с на GET без учёта в бюджете (A07), ожидание без deadline (A06).
3. **Media budget.** Одно окно на соединение без FIFO (A12).
4. **Диск.** BLOB-кэш в основной БД без проверки места и дедупликации; файл не сжимается после purge (A01).
5. **Память.** Документ целиком в памяти на каждом этапе: adapter, хеши, `read_asset` ([service.py:134-136](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L134-L136)). Пиковое потребление при параллельных GET в пакете не измерено.

#### 3. Сценарии отказов

| Сценарий | Текущее поведение (по коду и тестам) | Остаточный риск |
|---|---|---|
| Crash после скачивания, до commit | Ничего не сохранено; lease истекает, операция перезапускается ([worker.py:269-271](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L269-L271); тест 526) | Повторное скачивание и трафик (A07) |
| Crash после commit, до ответа | Replay по `request_key` (тесты 495, 526) | Без ключа нет защиты (A11) |
| Потерянный ответ на admission | Ключ и операция фиксируются в одной транзакции ([service.py:196-217](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L196-L217); тест 495) | — |
| Cancel worker | Claim сохраняется для наблюдения ([worker.py:269-271](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L269-L271)) | Задержка до истечения lease (длительность неизвестна) |
| UNKNOWN после dispatch PUT | Повтор только с известными ids (наблюдение); без ids ручной review ([worker.py:386-387](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L386-L387)) | — защищено |
| FloodWait/cooldown на PUT | Backoff до 300 с ([worker.py:394](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L394)) | Срок провайдера игнорируется (A14) |
| FloodWait/таймаут на GET | Терминальный `provider_read_deadline` | Ретрай-шторм потребителя (A13), lane удерживается (A07) |
| Истёкший кэш | `asset_not_available` → refresh; replay без пере-скачивания (тест 658) | Контракт TTL не виден потребителю (A24) |
| Отзыв entry-гранта во время скачивания | Проверка до lane, в lane и в транзакции ([worker.py:976-999](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L976-L999), `:1014-1019`; тест 216) | — защищено |
| Отзыв гранта после кэширования | Каскадно удаляется связь, `read_asset` отказывает ([service.py:106-107](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L106-L107)) | — защищено |
| Отключение topic | Epoch+1 → все операции и гранты principal недействительны | Массовая скрытая потеря доступа (A08) |
| Мало места на диске | Специальной обработки нет | A01, A02 — главный риск |
| Параллельная нагрузка | Сериализация на writer-lock и lane | A03–A07, A12 |

#### 4. Уже защищено — повторно не исправлять

- Неизменяемость грантов, отсутствие «воскрешения» после revoke/regrant для кэша, идемпотентность выдачи ([media_entry_grant_schema.sql:13-15](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grant_schema.sql#L13-L15); тесты 48, 83, 214).
- Проверка фактических байтов, а не заявленного хеша ([media_entry_grants.py:185](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L185); тест 139). Present origin.sha без отката на observed sha (тест 135).
- Изоляция tenant и principal для грантов, кэша и ключей (тесты 97, 263, 616).
- Повторная проверка гранта в трёх точках `run_read` с fence в транзакции.
- Legacy-путь сужен до topic 4, `regional_knowledge`, ref в форме UUID и одного изображения ([media_entry_grants.py:27-46](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/media_entry_grants.py#L27-L46)). Неоднозначные или неверные evidence отклоняются (тест 94).
- List и search не раскрывают чужие записи: owner-гранты по ним не видны ([service.py:257](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L257), `:265`). Это совпадает с «scoped list returned 8 service-owned only».
- Повтор после UNKNOWN — только наблюдение по известным ids.
- Консервативный учёт прерванного pre-dispatch в media budget.
- Отказ запускаться на неизвестной версии схемы ([storage.py:51-52](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/storage.py#L51-L52)). Откат на schema 10 не нужен и не предлагается; PR87 остаётся черновиком.

#### 5. Что нельзя проверить по пакету

Из пакета не видны:

- реализация `lane` (межпроцессная? FIFO?), длительности lease и heartbeat, порядок `claim`;
- обработка FloodWait внутри adapter;
- `save_fact`, `_replay`, `private_operation`, `project_item`, `_new_operation` (deadline чтения);
- лимиты `verify_image`, максимальный размер документа в adapter;
- схема `facts`, `media_store_assets` (FK и каскад на assets), `auto_vacuum`, `journal_size_limit`, политика checkpoint;
- общая ли ФС у Vibe, RKB и Street Story, общее ли Telegram-соединение и общий ли API Vibe у Street Story;
- systemd restart policy и внешний цикл worker;
- меняется ли epoch при ротации токена;
- состояние POI `unresolved_identity` — это вне VibePublish.

#### 6. Edge cases

- Ровно на границе `expires` между purge и чтением asset может быть отдан чуть позже TTL ([service.py:87-89](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/service.py#L87-L89)). Закрывается вместе с A03.
- Изменение provider_media в Telegram (редактирование сообщения) ломает гранты fail-closed (`check_download`). Это правильно, но выглядит как `access_denied` (A08).
- `schedule_media_store_retry` может назначить `lease_until` после deadline. Тогда pre-dispatch-дети завершатся `command_expired` ([worker.py:231](https://github.com/onedayonemasterpiece/vibepublish/blob/d1f880b24788f609398a660ba53dd8c4731b7aec/social_operations/worker.py#L231)). Это корректно, но ретрай бесполезен.

#### 7. Recommendation

**Порядок (предложение, не поручение):**

1. **A01 + A02** — проверка свободного места и размера по `evidence.size` до скачивания, квота и переиспользование кэша, обработка `SQLITE_FULL` без падения. Это единственный риск, способный задеть соседний Street Story через общий хост.
2. **A06 + A07** — ограничить ожидание lane deadline-ом и ограничить фоновые GET относительно публикаций.
3. **A03 + A04 + A05** — сократить и вынести тяжёлую работу из writer-lock, убрать синхронное ожидание из event loop. Регрессионный пакет: тесты 216/236/180/558/658.
4. **A08 + A09 + A10 + A11** — диагностика устаревших грантов, проверка sha при выдаче гранта, единая проверка идентичности для всех GET, обязательный ключ для сервисных GET.
5. Остальное (P3) — по возможности. A16 и A18 сначала подтвердить чтением кода или read-only запросом.

**Чего не делать:** VACUUM или копирование сырой БД при ~2.36 GiB свободного места; автоматический regrant; ослабление привязки к epoch; новые гранты или права; повторные публикации; откат схемы; дополнительный gateway.

#### 8. Confidence

- **Высокая** — для пунктов, доказанных строками кода: A03, A04, A06, A08, A11, A19, A24.
- **Средняя** — для кодовых рисков, чей масштаб зависит от неизвестных параметров среды (размеры файлов, lease, общая ли ФС и соединение): A01, A02, A05, A07, A09, A10, A12, A17, A21.
- **Низкая** — для гипотез, требующих кода вне пакета: A14, A15, A16, A18, A20, A23.
- Числа производительности не приводились, потому что замеров в пакете нет.

## Происхождение и полнота

Оба terminal result имеют `actualModel=claude-opus-5-5`, execution status `completed`. Исходные ответы Claude сохранены без смысловых сокращений; добавлены заголовки, anchors и ссылки на исходные строки. Критическая сверка Codex находится выше и не переписывает мнение консультанта. Приватные изображения, оригиналы книг, токены, native message/entry IDs и содержимое источников в GitHub не включены. Полные provided-only пакеты и platform receipts сохранены в managed retained evidence.

- `rkb-evidence.txt`: SHA-256 `54f854a4ebecb2c8a2058d64d46bb3967f3e5a83402188b2779b26f7b19d3f38`, 70719 bytes.
- `vibe-evidence.txt`: SHA-256 `d46e7ffe1846309fdcf33ea959a14889caa968d1bcf87fd944a4cc39462e8bd6`, 69789 bytes.
- `claude-rkb-full.md`: SHA-256 `aa4bf0c98b219ddba758f33adf502b47eb8f9eb87af8d2d392b6600975f1cd20`, 40098 bytes.
- `claude-vibe-full.md`: SHA-256 `be3f56dd7993c41faf051ea592118b2ce4cdd58b7828b207a444fdfd43bbf301`, 44269 bytes.
