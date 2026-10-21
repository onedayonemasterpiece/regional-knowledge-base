# RKB migration parity: обязательные сценарии после аудита 21.10.2026

Это **план**, не отчёт об успешных runtime-проверках. Начальный статус всех добавленных сценариев — NOT RUN. Существующее доказательство можно зачесть только при неизменных затронутых inputs/code/runtime и совпадающем scope. Основной контракт — [canonical handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), [owner alignment](../designs/rkb-owner-alignment-20261021.md), [подробный аудит](../reports/rkb-requirements-code-audit-20261021.md).

Каждая строка результата содержит exact commit, выбранный backend profile и tool/route, окружение/fixture, фактический outcome и ссылку на evidence. Колонки: legacy semantics preserved / target wired / focused tests / live acceptance. Test definition, test execution, deployment и продуктовая готовность не объединяются.

## A. Импорт и источник

| ID | Сценарий | Критерий |
|---|---|---|
| IMP-A1 | Модель уже видит реальные страницы PDF-вложения | start/stage не вызывают backend OCR, native-text extraction или полный render; источниковые страницы/edition связаны; структурная validation остаётся |
| IMP-A2 | Image-only scan и PDF с ошибочным/CID native layer | Model transcription сохраняется с честным provenance; отсутствие/отличие native text не отклоняет правильную модельную работу |
| IMP-A3 | Клиент не имеет страницы или продолжает в новом visual context | Существующий `book_pages` выдаёт запрошенные pixels малым batch, без притворного visual review по одной ссылке; неподдержанная client capability явная |
| IMP-A4 | Stage первых страниц длинной книги на холодном cache | Незапрошенные страницы не рендерятся, не парсятся и не загружаются в S3; счётчики unrequested page work равны0 |
| IMP-A5 | 300+ страниц и исходник больше pilot2MiB | Последовательные bounded stages, memory/bytes budget измерен; нет ограничения всей книги32pages/32passages/12stories; не загружается полный corpus в Dev |
| IMP-A6 | DjVu-вложение; затем отдельный stripe на его странице | Нет target_pdf_only для поддержанного source; модельная работа сохранена; декодер вызывается только для требуемых pixels/геометрии |
| IMP-A7 | Два конкурентных owner start/resume и non-owner import | Ровно один активный import lease; второй не исполняется параллельно, видит busy/pending/resume; non-owner отказ до private bytes/provider effect |
| IMP-A8 | Разрыв клиента, повтор stage/finalize, перезапуск | Сохраняются ingestion/source_revision/operation IDs, принятые model inputs и cursor; duplicate не создаёт новые domain entities, не требует повторного inference |
| IMP-A9 | reprocess/rechunk и metadata-only correction | Тот же logical source, явная новая revision там где нужна; accepted_reuse проверяется; metadata-only не переписывает source/vectors |
| IMP-A10 | Semantically complete page, figures/footnotes/table/cross-page continuation | Ничего не теряется; фигура либо зарегистрирована, либо явно обоснованно исключена; transport batch не определяет semantic passage |

Проверка source identity/edition отличается от custom Telegram download checksum. Не вводить обязательную media hash verification под именем IMP-A8/A9.

## B. Чанки, медиа и рассказы

| ID | Сценарий | Критерий |
|---|---|---|
| SEM-B1 | Final augmented BGE input ровно512 и513tokens | 512 принимается,513 требует корректного semantic split; без усечения. Учитываются caption/description/prefix. 256 — target, не запрет более длинного связного chunk |
| SEM-B2 | Caption/labelled description изменились; title-only metadata изменилось | Вектор изменён только при изменении полного embedding input, source text остаётся исходным |
| SEM-B3 | Chunk с принятой иллюстрацией | PG сохраняет figure/page/caption/model-description bindings; search/fetch отдаёт media refs; `illustration_fetch` реально включён в target |
| SEM-B4 | Answer-relevant chunk имеет figure | Вызывающая модель получает actual pixels и решает релевантность; description-only не зачтён как visual review |
| SEM-B5 | Малое фото и source выше реально доступной photo rendition | Первое не получает лишний document; второе имеет осмысленный high-res document locator. Политика основана на actual metadata, не магической константе |
| SEM-B6 | Telegram photo рекомпрессирован; сообщение удалено | Различие bytes не ошибка; успех по наличию доступного media. Удалённое media получает unavailable без выдуманного SHA и без зависания готового search |
| SEM-B7 | Связный эпизод и справочный фрагмент в одном чтении | Первый сохраняет настоящий model-authored Variant.body/default ref/evidence; второй сохраняет факты без искусственной Story. Нет story quota/forced length |
| SEM-B8 | Ordinary get/search после restart | Возвращает точный сохранённый body/labelled excerpt, не summary под видом рассказа; права на чтение отличаются от publish approval |
| SEM-B9 | needs_context после принятия source | Existing extraction/reconcile workflow достижим, создаёт недостающий result, не перезаписывает immutable accepted input и чужую human revision |
| SEM-B10 | Source withdraw / private draft / shared Slack output | Отозванный evidence/media исключён; private draft не раскрывается; публикация illustration не делает весь original публичным |

## C. Прямой Telegram

| ID | Сценарий | Критерий |
|---|---|---|
| TG-C1 | Upload/read оригинала и figure | Файловые bytes идут Kaggle↔Telegram/поддержанный client hosting; Vibe и Dev bulk proxy отсутствуют; locator metadata сохранена |
| TG-C2 | Потерян send ACK | Reconcile той же operation/native identity; blind resend и duplicate publication отсутствуют; при неопределённости UNKNOWN остаётся UNKNOWN |
| TG-C3 | FloodWait | Фактическая задержка соблюдается, state/continuation сохраняются, search reserve освобождён; не новый notebook или другая account session для обхода |
| TG-C4 | Один active uploader и конфиг pacing | Нет второго независимого лимитера/публикатора;20/min не объявлен универсальным provider SLA |
| TG-C5 | Конкурирующие runtime используют session | Один exclusive owner; expiry/disconnect не доказательство Stop; RKB2 не используется без actual readiness |
| TG-C6 | Наличие media по locator | Проверены access/type/chat/topic/message binding; не выполняется обязательный post-upload download/hash comparison |

## D. Dataset и восстановление

| ID | Сценарий | Критерий |
|---|---|---|
| REC-D1 | Публикация новой target PG Dataset version | Consistent artifact с cursor/schema/model/control metadata; подтверждённая native version; primary pointer не заменён до готовности |
| REC-D2 | Cold start | Exact confirmed Dataset version восстановлена; PG/indices/BGE проверены; старый SQLite archive не подставлен вместо target state |
| REC-D2a | **Первый MCP search при остановленном Kaggle** | Авторизованный MCP search регистрирует/переиспользует *один* startup demand у existing controller и **сразу возвращает короткий typed pending/continuation с demand ref** — до launch, Dataset restore или PG/BGE READY; исходный tool call не держится на прогреве. Далее отдельная контролируемая работа reconciles ownership/UNKNOWN → один разрешённый notebook → exact Dataset restore → PG/BGE READY. Клиент по readiness/status инициирует **новый** read с актуальными permissions/generation/deadline и получает usable evidence. Cold измеряется отдельно от warm ≤2000ms; нет duplicate launch или скрытого обязательного poller. |
| REC-D3 | Crash после PG apply, до Dataset coverage | Честный applied/persistence_pending; исходные IDs/payload не удалены; восстановление не требует нового semantic inference, если сохранённый spool доступен |
| REC-D4 | Потеря Kaggle и Dev disk | Отчёт точно показывает внешне закреплённый cursor и необеспеченный хвост; нет ложного zero-loss или подмены weekly backup актуальным tail |
| REC-D5 | Weekly Yandex backup и его восстановление | Внешняя копия конкретной подтверждённой version, actual age/restore proof. Не daily default и не непредусмотренный online primary |
| REC-D6 | Lost Dataset publish ACK / interrupted snapshot | Сверка той же version/operation до повтора; last-good база остаётся; live PGDATA не подменяется |
| REC-D7 | GC и изменившаяся семантика SQL | Retention не удаляет единственный recovery input; schema/function transitions и accepted replay упорядочены, новые функции не подменяют старый результат |

До решения Dataset/tail/ACK protocol старые реально принятые remote records не удаляются. Внешняя сохранность должна предшествовать соответствующему окончательному durable статусу, а не выводиться из успешного upload старого backup.

## E. Локальность и скорость

| ID | Сценарий | Критерий |
|---|---|---|
| PERF-E1 | Query и document embedding | Dev-bound vector bytes=0 в обоих путях, измерено по реальным frames/results; source staging/validation/tokenization/PG writes выполняются в Kaggle |
| PERF-E2 | Warm search при Telegram outage и одном импорте | Готовый текст не ждёт media/archive; общий hard deadline2000ms и measured search reserve сохраняются |
| PERF-E3 | Representative corpus/load | ≥100book-equivalent/100000activechunks, warm backendp95≤1000ms/publicMCPp95≤1500ms; ошибки/pending не исключаются из отчёта |
| PERF-E4 | Dense/lexical/fusion | ≥30 held-out fact families; BGE Hit@5≥85%,Hit@10≥90%, criticalRU→DEHit@10≥85%; lexical не prefilter dense; ANN сравнён с exact |
| PERF-E5 | Storage steady/peak | ≤80%/≤90% всех применимых измеренных budgets, включая heap/vector/index/WAL/temp/versions/caches/restore overlap |
| PERF-E6 | Телеметрия и короткие reads | Нет обязательного fsync/cloud seal/log lock до ready reply; bounded async buffer/drop counters; domain audit не удалён как telemetry |
| PERF-E7 | Hot SQL/function и app-handler change | Exact applied version/readback, одинаковые run/model/PG residency где совместимо; no needless Save/RunAll; UNKNOWN reconcile |
| PERF-E8 | Go/Rust decision | Baseline traces + обоснованный узкий prototype при необходимости + IPC/lifecycle/quality parity либо измеренное объяснение no-change; нет произвольного нового процентного gate |

## F. Расхождения и дела

| ID | Сценарий | Критерий |
|---|---|---|
| CAS-F1 | Последовательный импорт A→B с противоречащими source assertions | Persisted research continuation достижим; модель находит основания, фиксирует реальное расхождение и создаёт/дополняет Case без повторного полного импорта или отдельного второго corpus scan |
| CAS-F2 | Третий источник и повтор того же research result | Найдено прежнее предметное дело, добавлена позиция/evidence с сохранением истории; replay не создаёт дубликаты; Case identity не сводится к паре книг |
| CAS-F3 | Поиск дела без известного UUID | Target Case search возвращает sourced result; BGE/lexical режимы названы честно, отсутствующий path не заменён чтением legacy SQLite |
| CAS-F4 | Expert contribution, private draft и exact-version support | Current identity/permissions проверены; model suggestion не считается человеческой поддержкой; private draft не виден владельцу без grant; подтверждается конкретная версия вывода |
| CAS-F5 | Новый материал после поддержанного вывода | Current и last_supported различимы; прежняя поддержка не переносится на новый текст автоматически; withdraw источника переоценивает связанные outputs |
| CAS-F6 | Source request/discussion и Astro public export | Разрешённые запросы источников и обсуждение доступны через прежние группы инструментов; постоянная страница без логина, desktop/mobile, безопасная проекция, last-good/readback/revocation |
| CAS-F7 | Restart/UNKNOWN после Case mutation | Тот же operation/result/receipt, непрерывная история, без новой inference/публикации; чтение не фабрикует отсутствующий вклад |
| CAS-F8 | **Ответ Case create потерян, клиент не знает case_id, короткий Dev raw spool уже очищен** | `create` применён в PG с original operation_key/result, но ответ с case_id **не дошёл до клиента**; затем DevCoveer restarted и bounded raw spool действительно compacted/GC по собственной политике. По одному исходному `operation_key` тот же authorised actor получает **тот же case_id и exact persisted PG outcome** через `disagreement_edit(operation_status)` или эквивалентный target PG method; иной actor не видит outcome, повтор не создаёт второе дело. Тест не должен проходить за счёт ещё живого raw spool; reuse [теста PR151](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4/tests/test_disagreement_journal.py#L57-L63) с усилением целевой PG/GC границы. |
| CAS-F9 | **Неверная связь экспертного аккаунта и авторство** | `AccountUnlink(incorrect_binding=True)` отзывает конкретный link/revision, ставит `attribution_review_required` сохранённым связанным contributions (по действующим grants), не переписывает origin/текст и не позволяет старой связке читать/дописывать новые материалы. Реальный автор не угадывается автоматически. Reuse [PR151 participants](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/adf78a50013e2d8a2fbbe425c61f4ac479eaa0c4/src/regional_knowledge/participants.py#L253-L275). |
| CAS-F10 | **Преемственность PR151 expert/Slack workflow** | Target MCP сохраняет `disagreement_participants`, `expert_request_delivery`, `expert_research`, `research_request` и Slack native ACK/reconciliation: личный verified actor и delegated worker имеют те же текущие permissions, exact result hashes, per-dialog order, UNKNOWN reconcile без duplicate sends. PR151 — отдельный unsmerged baseline, не production acceptance. |

В дополнение к OLD main учитывается [черновой PR151 at adf78a5](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/151) как фактически написанная (но не слитая и не развёрнутая) baseline реализации экспертного/Slack lifecycle; её существующие контракты и проверки должны быть перенесены по смыслу в PostgreSQL target без копирования legacy SQLite authority.

Наличие четырёх native Case tool groups или schema010 не закрывает эту матрицу. Все строки первоначально NOT RUN; старые данные не мигрируют, но весь согласованный функционал для новых дел обязателен.

## G. Каналы и итоговый выпуск

Personal MCP, настоящий non-owner, Slack-delegation и Mira/Projects Hub проверяются отдельно. Не дописывать owner UUID в запрос для имитации внешнего пользователя. Сохранить per-dialog order, current grants/withdrawal, cancellation/generation и evidence context. Телефон/голос не объявлять принятыми по Python fixture.

Разрешение release следует из применимых продуктовых сценариев, не из общего количества тестов. В итоговой таблице PASS/FAIL/BLOCKED/NOT RUN каждый BLOCKED имеет реальную причину и минимальный следующий шаг; дополнительные необязательные аудиты не превращаются в бесконечные ворота. Числа исходного protected contract этим документом не меняются.
