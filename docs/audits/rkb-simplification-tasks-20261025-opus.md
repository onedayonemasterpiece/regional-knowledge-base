# RKB: задания на упрощение критического пути (RKB-Opus, 25.10.2026)

Аудит и проверка — RKB-Opus (Claude). Код читан лично на `main` @ `a08bab4d`: путь поиска на Dev и Kaggle, общий WSS-канал, admission, путь страницы импорта, приём пакета, control spool, Dataset-журнал, применение в PG, checkpoint, запуск. Каждое задание ниже проверено по коду; где нужен живой факт — так и написано. Находки аудита ChatGPT Pro (`docs/audits/rkb-simplification-audit-20261025-pro.md`), которые я лично не проверил, в задания **не** включены (список в конце). Накопительный реестр: issue #481.

## 0. Правила для исполнителя (обязательны, читать до кода)

Цель каждого задания — **убрать** условие/работу или **заменить** её уже существующим механизмом. Если правка требует добавить что-то сверх разрешённого в задании — остановись и напиши в общий тред `BLOCKED: <что именно нужно добавить и почему>`. Не «улучшай» соседний код.

Запрещено в любом задании, если в самом задании прямо не разрешено:
- новые таблицы, колонки, статусы, флаги, режимы, env-переменные, настройки, таймеры, ретраи, очереди, фоновые задачи, классы-обёртки, «proof»/хеш/SHA-сверки, новые лог-события и метрики;
- новые защитные проверки «на всякий случай», в том числе вокруг удаляемого кода;
- изменение 12 файлов materializer-fingerprint: `kaggle_source_materializer.py, source_batch_selection.py, source_batch_context.py, kaggle_source_batch.py, source_graph_native.py, source_stage.py, source_activation.py, stage_graph.py, source_packet_tails.py, entity_graph.py, source_manifest.py, bge_contract.py`. Если без этого нельзя — `BLOCKED`;
- переименования, перенос кода между файлами, рефакторинг «заодно», правка документации, кроме строк, прямо описывающих удаляемое поведение.

Тесты: добавить **ровно один** различающий тест из задания (реальный путь, не подделка, где сказано). Тесты, закрепляющие удаляемое поведение, — **удалить**, а не переписать под новое. 

Бюджет строк — по production-коду (`src/`, `scripts/`, `sql/`), без тестов. Превышение = `BLOCKED`, а не «чуть больше».

Результат: один draft-PR от свежего `origin/main` на задание, полный CI зелёный. В описании PR таблица: `удалено/заменено | какой сбой это вызывало | что теперь покрывает | тест`. Не сливать (сливает RKB-Opus после ревью), не трогать live/Kaggle/prod-БД — живую проверку делает Шкипер.

Приоритет: P0 — бьёт по живому пути или останавливает запись; P1 — реальный дефект/лишняя работа на пути импорта; P2 — не начинать без отдельного слова. Задания P0-1, P0-2 и P1-x независимы друг от друга, кроме оговорённого.

---

## P0-1. Общий WSS-приёмник не должен ждать служебную работу

**Продукт.** Ответы поиска с Kaggle приходят на Dev через один последовательный приёмник. Пока он обрабатывает служебный кадр, следующие ответы поиска не читаются, поиск упирается в 2 с и возвращает отказ. Это вторая половина живого сбоя «рендер страницы + поиск» (первую сняли PR 476/482).

**Доказательство (`bge_channel.py`, receiver, ~L377–599).** Внутри цикла `receiver()` выполняются долгие `await`:
- `RECOVERY_PHASE_RESULT` (L528–551): если владельца ещё нет — ожидание до `2*SEND_TIMEOUT_SECONDS` = 10 с (L542); затем `recovery_dispatch.receive()` (`kaggle_recovery_dispatch.py:206`) — синхронная SQLite-транзакция `BEGIN IMMEDIATE` прямо на event loop (`with self.queue.connect() as db:` L209; `bge_queue.connect(timeout=5)` — до 5 с ожидания блокировки, весь процесс стоит), там же синхронный `self.rebind_current()` (L220);
- `SNAPSHOT_UPLOAD_INTENT` (L512–527): `issue_upload` → блокировка writer guard + вызов Kaggle API `publisher.reserve` (`kaggle_dataset_tickets.py`) — сетевой вызов на каждом checkpoint;
- `RKB1_BIND_REQUEST` (L446–463): `owner.grant` до 5 с.

**Сделать.**
1. В `kaggle_recovery_dispatch.receive()` всю работу с SQLite вынести в `asyncio.to_thread` (как уже сделано в `queue_call` в `bge_channel.py`), а вызовы, меняющие объекты рантайма (`rebind_current`, `schema_context`, `_notify_result`, `_bindings.pop`), оставить на event loop после возврата из потока.
2. Кадры `RECOVERY_PHASE_RESULT`, `SNAPSHOT_UPLOAD_INTENT`, `RKB1_BIND_REQUEST`, `SOURCE_COMMAND_RESULT` обрабатывать не в `receiver()`, а в **одной** фоновой задаче этого же соединения, которая берёт их по порядку из `asyncio.Queue` (создать в `endpoint`, рядом с `outgoing`). `receiver()` только кладёт кадр в эту очередь и читает дальше. Порядок обработки этих кадров между собой сохраняется; задача создаётся и отменяется вместе с `sender/receiver/dispatch` (список `tasks`). Ожидание владельца (L535–545) переносится туда же без изменений.

**Нельзя.** Менять семантику ACK, `durable_result_settled`, порядок commit до ACK; распараллеливать служебные кадры между собой; добавлять таймауты/ретраи; трогать обработку `QUERY_RESULT`.

**Бюджет.** ≤ 40 строк production добавлено, удалённые не ограничены.

**Тест (один).** Реальный `channel_route` с поддельным worker-сокетом: `RECOVERY_PHASE_RESULT`, чей `receive()` ждёт 3 с, затем `QUERY_RESULT`; `finish_read` для второго вызван раньше, чем через 200 мс.

**Живая проверка (Шкипер).** Поиск во время рендера страницы DjVu и во время checkpoint Dataset: нет `not_dispatched`/`deadline_exceeded`.

---

## P0-2. Сбой native-задания не должен держать всю durable-очередь до конца run

**Продукт.** Одна неожиданная ошибка при рендере страницы, capture или применении пакета сейчас останавливает **всю** durable-работу (импорт, `book_pages`, сохранение в Dataset) до конца сессии Kaggle (до 9–11 ч), и notebook не засыпает. Это прямо нарушает решение владельца: «неизвестный исход разрешается по журналу `applied_ops`; никакой вечной блокировки».

**Доказательство.**
- `scripts/production/bge_wss_transport.py` `_encode` (L350–418): при неожиданном исключении печатает `native_job_outcome_unresolved` / `source_command_outcome_unresolved` (L405, L412) и выходит **без** `self.pending`; `self.job` остаётся занят.
- Тот же файл: новая `DISPATCH` при занятом `self.job` → `ValueError('durable result still awaiting acknowledgment')` (L668–669) → сессия рвётся; повтор того же задания → `continue` (ничего не делает); `STOP` игнорируется, пока `self.job` не пуст (L751).
- `bge_queue.py` `heartbeat` (L655–659) продлевает `lease_until` всем claimed-заданиям `document/source_command/recovery_phase`; `_claim_specific` (L783–805) не выдаёт новую durable-работу, пока у run есть claimed-задание. Итог — задание «занято» до конца run.
- Для `source_command` после двух `ApplyUnknown` (`kaggle_source_commands.py` L302–303, `raise`) исключение доходит до того же места.

**Сделать (только на worker, `bge_wss_transport.py` `_encode`).**
1. `recovery_phase`: неожиданное исключение → обычный результат с `receipt=dict(phase=..., request_sha256=..., value={'error':'native_phase_failed','retryable':True})` через существующий `self.pending` (как уже делаются ошибки `original_intake_unavailable`). Никаких новых полей, кроме этих двух ключей в `value`.
2. `source_command` (включая `ApplyUnknown`/`ApplyExecutionError`, дошедшие сюда): поднять `ConnectionError` (как `BgePendingResultResync`), чтобы существующий reconnect повторил **то же** задание; повтор безопасен — `apply_accepted` по `operation_key`/`sequence` возвращает уже записанный итог (`kaggle_apply.py`, блок `existing_outcome`) или применяет впервые. Перед повтором очистить `self.job`, чтобы повтор не был проглочен `continue`. Внимание: `run()` в начале каждой итерации вызывает `self._failure(self.encoding)` **вне** `try` (L845; внутри сессии — L545) — если оставить упавшую задачу в `self.encoding`, процесс worker завершится. Поэтому исключение поднимать так, чтобы его поймал существующий `except (ConnectionError, …)` reconnect-цикла, а `self.encoding` после этого не держал упавшую задачу.
3. На Dev (`kaggle_recovery_dispatch.request`, L97–117): если результат — `{'error':..., 'retryable':True}`, следующий запрос с теми же аргументами должен выполниться заново, а не вернуть закешированную ошибку. Использовать уже существующий приём из ветки `schema_migrate` (L102–110: суффикс `:retry:N` к `operation_key`), распространив его на `retryable`-ошибки; лимит попыток — существующий (256 в этой ветке).

**Нельзя.** Добавлять новые состояния заданий, таймеры, счётчики попыток в БД, новые типы кадров, «карантин»; менять `heartbeat`/лизы; трогать `kaggle_apply.py`.

**Бюджет.** ≤ 30 строк production добавлено.

**Тест (один).** Реальный `BgeWssTransport` + `Peer` из `tests/test_bge_shared_worker.py`: обработчик `recovery_phase` бросает `RuntimeError` → приходит `RECOVERY_PHASE_RESULT` с `value.error='native_phase_failed'`, после `RESULT_ACK` следующая `DISPATCH` исполняется.

**Живая проверка (Шкипер, до и после).** В логах Kaggle за последние run: есть ли `native_job_outcome_unresolved` / `source_command_outcome_unresolved` и совпадают ли они с «зависаниями» импорта.

---

## P0-3. Журнал на Dev растёт без удаления до пожизненного предела 512 МиБ

**Сначала замер (Шкипер, только чтение), без него задание не начинать:** текущий `ControlSpool._used_bytes` (SQL из `control_spool.py:599–606`), `count(*)` и сумма `length(command_bytes)+length(outcome_bytes)` в `rkb_control_operations`, сумма `length(data)` в `rkb_dataset_recovery_objects`, размер последнего `journal-control.json` в Dataset.

**Продукт.** Когда суммарный объём достигнет 512 МиБ, любая новая запись (пакет импорта, Case, Story) получит отказ «spool full», импорт встанет.

**Доказательство.** `control_spool.py:599–606` `_used_bytes` суммирует все операции за всё время (+32 КиБ накладных на каждую) против `max_bytes=512 MiB` (L223); `DELETE` нигде нет; `raw_retention_seconds` (L223–231) объявлен, проверен и нигде не используется. `dataset_recovery_store.py:210` — свой пожизненный предел 512 МиБ на объекты журнала; `export_control` (L91–106) выгружает **всю** историю в каждую версию Dataset.

**Сделать (после замера и только если рост подтверждён).**
1. В существующем месте после успешного `promote` checkpoint (`kaggle_dataset_checkpoint.py`, после `publish_outcome` для покрытых ключей) удалять из `rkb_control_operations` строки с `outcome_state='DURABLE'` и `sequence <= checkpoint.base_sequence`, у которых `updated < now - raw_retention_seconds` и нет `jobs` в состоянии `pending/claimed` по этому `operation_key`. Компактные `rkb_control_receipts` не трогать (идемпотентность клиентов).
2. Из `rkb_dataset_recovery_objects` удалять объекты с `covered_version IS NOT NULL`, относящиеся к сегментам/манифестам с последовательностью `<= base_sequence`, **кроме** текущей головы, манифеста текущей базы и control текущей базы (обход цепочки останавливается на базе — `verify_chain(stop=base)`).

**Нельзя.** Удалять что-либо с `SENT_UNKNOWN`, `NOT_SENT`, без `covered_version`, выше `base_sequence`; вводить новые таблицы/метки «к удалению»; менять форматы.

**Бюджет.** ≤ 30 строк production.

**Тест (один).** Реальный SQLite: 3 операции, checkpoint покрывает 2 → после promote и истечения `raw_retention_seconds` `_used_bytes` уменьшился, повторный `seal/apply` новой операции и restore по `verify_chain(stop=base)` проходят.

---

## P0-4. Убрать Dev-«барьеры» книг на каждом чтении

**Продукт и решение владельца.** Закрытая группа: все видят все книги, проверок видимости книг быть не должно. Снятие книги (`source.withdraw`) уже учитывается в PG: `NOT s.withdrawn` во всех функциях чтения.

**Доказательство.** `source_read_gate.py`: `_restricted_sources` (L103–109) → `spool._barriers_for` (SQLite по каждому источнику) вызывается в `_gate` дважды (L281, L366); `_gate` выполняется при `finish` и снова при `refresh`; плюс `issuance_current` (L432).

**Сделать.** Удалить `_restricted_sources` из `source_read_gate.py` и все ветки, зависящие от `restricted` (`evidence_withdrawn` из-за барьера); `read.facts` оставить только если используется где-то ещё — иначе удалить. **Не трогать** `case_read_gate.py` и `kaggle_cases_backend.py` (приватные дела остаются), проверки `supported()`, scope, `grant_refs`, сравнение `observed_database_generation`/`min_data_seq`.

**Цена (принято RKB-Opus).** Снятая, но ещё не применённая в PG книга может находиться несколько секунд — до применения команды снятия.

**Бюджет.** Только удаление.

**Тест (один).** Удалить тесты, ожидающие `evidence_withdrawn` из-за локального барьера; добавить: поиск с ожидающей (не применённой) командой снятия на Dev не делает ни одного запроса к `rkb_control_operations`.

---

## P1-1. Страница импорта: убрать проверку собственной только что загруженной картинки

**Продукт.** Каждая страница импорта (`book_pages`) — это цепочка durable-заданий и обращений к S3; это главный путь «страниц/час».

**Доказательство.** `original_intake_controller.py` `_archive` (L140–164) для страницы: `HEAD` (L149) → presign PUT → `original_archive` (Kaggle загружает JPEG) → `HEAD` (L156) → presign GET → `original_verify` (L157–162: Kaggle **скачивает обратно** только что загруженный JPEG и сверяет SHA, `kaggle_original_native.py` L338–344) → `HEAD` (L163) → Dev скачивает JPEG. Условная загрузка `If-None-Match:*` и `ETag` уже подтверждают объект; решение владельца по медиа — «никаких цепочек SHA как условий».

**Сделать.** Для **страниц** (`page is not None`) убрать `original_verify` и последний `HEAD`; второй `HEAD` после `original_archive` оставить единственной проверкой. `_page_data` использует из результата только `key` и `native_version` — вернуть их и в ветке, где первый `HEAD` нашёл уже загруженный объект. Для исходного файла книги (`page is None`) ничего не менять — его `native_verification_sha256` входит в принятый манифест (`source_manifest.py`).

**Нельзя.** Трогать путь оригинала книги, `source_manifest.py`, формат `issued` receipt.

**Бюджет.** Только удаление (+ ≤ 3 строки ветвления `if page is None`).

**Тест (один).** `pages()` для новой страницы делает ровно 2 durable-фазы (`original_pages`, `original_archive`) и ≤ 2 `head_object`.

---

## P1-2. DjVu: не разбирать инвентарь всей книги на каждой странице

**Доказательство.** `kaggle_original_native.py` L363: для каждой ещё не отрисованной страницы DjVu `inspect_djvu(record['path'])` разбирает структуру всей книги; результат не сохраняется.

**Сделать.** Сохранить `inventory` в `record` при первом вызове (ключ в уже существующем словаре `record`), дальше использовать его.

**Бюджет.** ≤ 5 строк.

**Тест (один).** Три страницы одного DjVu → `inspect_djvu` вызван один раз.

---

## P1-3. Fetch: страницы и регионы для пассажей не из первого пакета

**Продукт.** Точные цитаты и страйп: сейчас для большинства пассажей книги `pages` и `regions` в fetch пустые. **Проверено живьём 25.10:** пассажи стр. 16 и стр. 161 — `"pages": null, "regions": null`.

**Доказательство.** `sql/kaggle/009_story_editorial.sql` L509–523 `source_passage_metadata_v1` берёт страницы/регионы из `source_manifests.manifest`, а `026_source_batch_activation.sql` L817 вставляет туда манифест только первого пакета (`ON CONFLICT DO NOTHING`). Уже есть функция, сливающая манифесты всех пакетов: `rkb_v4.read_source_manifest_v1(source_id)` (026, ~L980).

**Сделать.** Новая миграция по существующему порядку `sql/kaggle/0NN_*.sql`: `CREATE OR REPLACE` `source_passage_metadata_v1` с заменой `m.manifest` на `rkb_v4.read_source_manifest_v1(p.source_id)`; остальная функция без изменений.

**Нельзя.** Менять формат ответа, таблицы, другие функции.

**Бюджет.** ≤ 25 строк SQL.

**Тест (один).** Реальный PG17: книга из 2 пакетов, fetch пассажа второго пакета → непустые `pages` и `regions`.

**Живая установка** — Шкипер (SQL-функция на Kaggle).

---

## P1-4. Импорт не должен блокировать правку историй и GEO

**Продукт.** Пока идёт импорт и до следующего сохранения в Dataset (15–30 мин), правка Story и разрешение GEO отвечают `prior_write_pending`.

**Доказательство.** `kaggle_story_editorial_backend.py:263` и `kaggle_source_geo_mvp.py:184` требуют `scope['pending']==False` из глобального `spool.scope_state()`, где pending = любая операция с `outcome_state<>'DURABLE'` (`control_spool.py` `_scope_state`), включая уже применённые пакеты импорта, ждущие только Dataset. Для чтений уже есть правильный признак: `read_facts(None).pending` использует `_read_pending_sql`, который исключает применённые пакеты импорта (`_read_safe_sql`).

**Сделать.** В обоих местах заменить `scope['pending']` на `facts.pending` из `spool.read_facts(None)` (в GEO он уже вычисляется ниже — использовать его, а `scope['pending']` удалить). Проверки `unknown` и последовательности оставить.

**Бюджет.** ≤ 6 строк.

**Тест (один).** Применённый, но не сохранённый в Dataset пакет импорта не мешает `story_edit`; ожидающая (не применённая) команда — мешает.

---

## P1-5. Удалить старый режим импорта целой книги (`packet_mode=0`)

**Решение владельца.** Сервис в разработке, книги можно переимпортировать; излишняя обратная совместимость не нужна. В живой базе одна незавершённая книга старого режима (37 пакетов) — её импортируют заново.

**Доказательство.** `kaggle_native_source_intake.py`: новые строки создаются с `packet_mode=1` (L110); старый путь — `validate` (seal), `finalize` (materialize/activate), лимит 8 страниц, ветки по `packet_mode` в `_stage_key`, `_can_settle`, `_drive`, `_output`, `status`; `kaggle_importer.py` ~L275–291 — исключения для старых кандидатов.

**Сделать.** Удалить ветки `packet_mode=0` и вызовы маршрутов seal/materialize/activate **из** `kaggle_native_source_intake.py` и `kaggle_importer.py`. Строку старого режима, если встретится, переводить в `failed` с существующим `rejected_reason='legacy_import_reimport_required'` (поле уже есть). Декодеры уже принятых команд (`kaggle_source_commands.decode_command`, `source_activation.py`, `source_stage.py`) **не трогать** — они в журнале и в fingerprint.

**Нельзя.** Удалять/мигрировать данные; трогать fingerprint-файлы.

**Бюджет.** Только удаление (+ ≤ 5 строк перевода в `failed`).

**Тест (один).** Строка с `packet_mode=0` → `status` возвращает `failed` с этой причиной; все тесты packet-режима без изменений.

---

## P2 — не начинать без отдельного слова владельца/RKB-Opus

Проверено мной, но выигрыш мал или нужна осторожность:
- **Повторные проверки на пути поиска (Pro S5, S6, S7, S8, SQL-4).** Права участника перечитываются ~7 раз за поиск, READY-proof — при каждом `binding_current`, `finish_read` дважды собирает головы очередей, результат перепроверяется через JSON на обеих сторонах, `statement_timeout` ставится перед каждым SQL. Подтверждено, но это миллисекунды: чтения SQLite в WAL не ждут писателя, тёплый поиск ~0,3 с при цели 1 с.
- **Режимы активации, кроме Dataset (H16).** Живая конфигурация — только Dataset; владелец: резервный режим будет сделан позже отдельно. Удалить ветки `source_snapshot`/`dataset_seed`/`history.restore/extend` — большое вычитание, но не на горячем пути.
- **Перепроверка всей истории `applied_ops` в каждом снимке (Pro RT03).** O(N), потолок 2 млн — далеко.
- **4096 операций оригинала на процесс (Pro RT04).** Предел в памяти одного run; при ~44 стр/ч недостижим, станет потолком около 150 стр/ч.
- **Полный backup PG на каждом checkpoint (Pro RT07).** Подтверждено; проверка снимка до продвижения указателя — требование владельца, замена не доказана.
- **Опрос `snapshot_status` каждые 2 с отдельными durable-заданиями (Pro RT10).** Подтверждено; часть задержки `book_pages`. Безопасная замена ожиданием результата не доказана — после P0-2 и P1-1 пересмотреть.
- **Дубли проверок приёма пакета (H2/Pro I1), производные счётчики (H3/Pro I5).**

## Не включено (не проверено мной лично или решение не за мной)
- Pro S2 (граница таймера MCP), S3 (`search` сразу с текстом — вопрос владельца о клиентах), SQL-1, SQL-2, SQL-3, SQL-5, SQL-6, I3, I4, RT01, RT02, RT05, RT06, RT08.
- Перезапуск Dev-API при ротации (~18 с) — разрешённый контролируемый перезапуск, не чинить.

## Порядок
P0-1 и P0-2 — первыми, разными авторами (разные файлы: Dev-канал и worker-транспорт; P0-2 трогает `kaggle_recovery_dispatch.request`, P0-1 — `kaggle_recovery_dispatch.receive`, согласовать ветку). P0-4, P1-1, P1-2, P1-4 — независимы. P0-3 — только после замера. P1-3 — отдельно (SQL, живая установка). P1-5 — после P0-2.
