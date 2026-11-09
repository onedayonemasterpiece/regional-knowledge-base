# RKB — единая передача RKB-Opus: follow-up упрощений и проверка замечаний других агентов

**Дата:** 25 октября 2026 года. **Адресат:** RKB-Opus (Claude), единственный merge-owner. **Автор сводки:** ChatGPT Pro.  
**Проверенный основной срез:** `401f308e553e1b834f3526130595f0a68bdbb449` (ветка `main`). Для ещё не слитых PR проверялись их отдельные HEAD, указанные ниже.  
**Тип документа:** передаточная записка о *уже выполненной* проверке follow-up и Slack-ревью, а не новый независимый построчный аудит всего репозитория. Никакие утверждения о полном охвате всех `src/`, `scripts/` и `tests/` не делаются. Сохраняются доказательства, ограничения и различие между воспроизведённым дефектом, достижимым сценарием по коду и рекомендацией.

**Основные исходные документы:**
- [Большой независимый аудит ChatGPT Pro в `main`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/audits/rkb-simplification-audit-20261025-pro.md) — ранее опубликованный полный документ, не заменяется этой сводкой.
- [Основное задание RKB-Opus с `0, P0/P1/P2](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-tasks-20261025-opus.md).
- [Follow-up Opus: F1/F2/F3](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-followup-20261025-opus.md).
- [Список/обзоры команды «Упрощатель» в личном Slack-треде](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792926181208919), [общий тред координации](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792868771439369), [накопительный реестр #481](https://github.com/onedayonemasterpiece/regional-knowledge-base/issues/481).

## 1. Итог и приоритет для Opus

**F1 и F2 внесены в существующие draft PR #489 и #488, полный CI с PG17/pgvector зелёный на Python 3.12/3.13. P0-3 нельзя выполнить буквально: реальный продуктовый путь импортёра теряет доступ к ещё потребляемой завершённой операции после предложенной очистки. Сообщение `BLOCKED` с воспроизведением передано в общий тред; без поправки к условиям retention «голый DELETE» опасен.**

По замечаниям Slack: два небольших дублирования остаются кандидатами на удаление; глобальная последовательность восстановления вопросов PR #472 — содержательный кандидат на упрощение, но сам PR остаётся вне `main` и под P0 freeze veto. Требования удалить очередь PR #492, текущую проверку выдачи первого сообщения в треде или факт доставки именно текущему восстановленному runtime — **не подтверждаются**: они закрывают конкретные достижимые сбои. Несколько замечаний уже исправлены или относятся не к тому PR.

| ID | Значимость | Предмет | Вердикт / действие Opus | Статус |
| --- | --- | --- | --- | --- |
| F1 | P0 | Native `source_command` и детерминированные ошибки | Проверить и принять корректность [PR #489](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/489), не превращать известный rollback в reconnect | Доработка внесена, CI PASS, не слито |
| F2 | P1 | Манифест источника на пути `fetch` | Проверить [PR #488](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/488): один разворот на строку | Доработка внесена, CI PASS, не слито |
| F3 | P0 | Рост `ControlSpool` и Dataset recovery-журнала | **`BLOCKED`:** пересогласовать retention с незавершённым потребителем и restore pins | Найден воспроизводимый регресс при буквальном P0-3 |
| D1 | P2 — малая чистка | `_maybe_release` в worker PR #476 | Удалить недостижимую ветку/вызов без изменения `forwards` | Кандидат; дополнительный отдельный тест обязателен при правке |
| D2 | P2 — малая чистка | Повтор `database_generation` в GEO gate PR #477 | Удалить отдельное дублирующее сравнение; **не** удалять `_Coverage` | Кандидат; выигрыш latency не доказан |
| D3 | Отложить до снятия freeze | `resume_pending` PR #472 | Проверить устранение глобальной зависимости чужих Slack-диалогов | Реальный кандидат; открытый несмёрженный PR |
| K1 | Оставить | Служебные кадры WSS PR #492 | **Не удалять** `service_frames` queue | Защищает приём результатов поиска от длительных служебных `await` |
| K2 | Оставить | Новый delivery receipt PR #400 | **Не удалять без равнозначной замены** наблюдение текущего replay | Старый receipt не доказывает применение в новой БД |
| K3 | Оставить | `request_order.is_head` PR #472 | **Не удалять** порядок в одном Slack-треде | Важно перед Slack POST; не эквивалентно startup wait |
| C | Снять/исправить запись | Заявления по PR #478/#479/#483/#480 и старые лимиты | Уточнить реестр, не ставить задачи-дубли | Исправлено, устарело или неверная идентификация |

## 2. Исполненный follow-up: ссылки на PR и тесты

### F1 — [PR #489: durable-очередь после native-ошибки](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/489)

**HEAD:** `f71fe102e7a5446c9e013e2d749c15785a9b9c80`. [GitHub Actions run 40233968938](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40233968938): **PASS** для Python 3.12 и 3.13, реальные PostgreSQL 17 и pgvector. Лог Python 3.12: **3597 passed, 42 skipped**.

Дефект: неожиданное исключение в `source_command` уходит в `ConnectionError → reconnect`; детерминированная ошибка воспроизводится на каждом круге, сбивает поиски в полёте и способна привести к остановке worker по `reconnect_grace_seconds`. Теперь неожиданные исключения materializer/apply идут через **уже существующий** `apply_accepted(..., reject_reason='native_source_sql_rolled_back')`; поиск ранее записанного `applied_ops` outcome выигрывает при повторе. `ApplyUnknown`, `PermissionError`, `StaleWriter` и cancellation сохраняют прежнюю семантику; `kaggle_apply.py`, Dev admission и heartbeat не менялись.

[Обработчик в `kaggle_source_commands.py`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/f71fe102e7a5446c9e013e2d749c15785a9b9c80/src/regional_knowledge/kaggle_source_commands.py#L250-L310) • [различающий тест на реальном WorkerSourceExecutor / PG17 / WSS](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/f71fe102e7a5446c9e013e2d749c15785a9b9c80/tests/test_source_command_sql_isolation.py#L27-L83).

В тесте `RuntimeError` материализатора даёт `rejected_precondition` с прежним reason, повтор выдаёт `duplicate=True` и тот же итог; следующий поиск выполняется **в той же WSS-сессии**. Дополнение F1 уложилось в бюджет `≤12` новых production-строк (`+10/−8` в этой доработке). Из исходного P0-2 также сохранены штатный `RECOVERY_PHASE_RESULT` для retryable native failure и возобновление original source dispatch после READY_ACK без смены identity.

### F2 — [PR #488: provenance пассажей следующих пакетов](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/488)

**HEAD:** `7e6b88591919c471b5468947eb65b292d804dfdf`. [GitHub Actions run 40233849832](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40233849832): **PASS** на Python 3.12 и 3.13, PG17/pgvector; Python 3.12: **3618 passed, 42 skipped**.

Дефект: `038_source_passage_packet_metadata.sql` в первой версии трижды на одну строку `fetch` вызывал `rkb_v4.read_source_manifest_v1(p.source_id)` для `catalog/pages/regions`; при пяти пассажах — до 15 полных сворачиваний пакетов книги. Исправление: **один** `CROSS JOIN LATERAL ... AS f(manifest)`; `pages` и `regions` из `f.manifest`, `catalog` из прежнего `m.manifest->'metadata'`. Сохраняются прежние фильтры и DTO, реальный тест второго пакета без изменений. Production SQL-файл — 16 строк.

[Миграция `038`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/7e6b88591919c471b5468947eb65b292d804dfdf/sql/kaggle/038_source_passage_packet_metadata.sql) • [история и тест PR #488](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/488/files).

Этот CI доказывает корректность, **не** точное живое ускорение `fetch`. Установка SQL на Kaggle — только Шкипер после решения Opus.

**Совместимость F1+F2 на срезе `main`:** cherry-pick обоих изменений без конфликтов, локальная объединённая выборка **147 passed, 19 PG-dependent skipped**; PostgreSQL-интеграция проверялась отдельно полными CI каждого PR. Ни один из этих PR не merged/deployed на момент передачи.

## 3. F3 / P0-3 — новый воспроизведённый BLOCKED

### Почему прежнее условие очистки неверно

[Основное P0-3](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-tasks-20261025-opus.md) и [F3](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-followup-20261025-opus.md) предписывают удалять сырые операции, когда `DURABLE`, возраст `>4h`, `sequence ≤ checkpoint.base_sequence` и нет `pending/claimed` jobs. Но **задание завершено ≠ его outcome потреблён импортёром**. У импортёра в `rkb_native_source_intake_state` остаётся `pending_operation_key` до очередного `book_ingest status`; здесь он обращается к исходной операции.

**Воспроизведение** в изолированном тесте реального `SQLite/ControlSpool/OAuth/book_ingest` с синтетическим native-result: `start → stage → record_apply → publish_outcome → _settle_recovered_jobs` (job уже `done`); клиент ещё не опросил `status`; удаление точно по предписанному predicate уменьшает `_used_bytes` и оставляет compact receipt, но `book_ingest status` выдаёт **`native_pending_operation_missing`**. Production-state не модифицировался.

[Потребитель `pending_operation_key` и `_drive`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_native_source_intake.py#L348-L374) • [сообщение о `BLOCKED` с точной последовательностью и дополнительными ограничениями](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792962449004329?thread_ts=1792868771.439369&cid=C0C8RT9JB2P).

### Другие достижимые зависимости raw-журнала, которые условие очистки не учитывает

1. **Идемпотентный повтор старой команды.** После raw GC `catalog_update` не читает compact receipt и способен выделить новую sequence при старом operation key; дальше получаем несоответствие канонических байтов. [`kaggle_native_source_intake.py:447–481`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_native_source_intake.py#L447-L481). Достижимо по коду; не отдельный production-incident.
2. **Закреплённая база ожидающего successor.** `target.json` может по-прежнему ссылаться на B0 после продвижения latest до B1; удаление хвоста B0→B1 ломает restore этого successor. [`kaggle_launch_config.py:73–99`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_launch_config.py#L73-L99) и [`kaggle_restore_startup.py:187–203`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_restore_startup.py#L187-L203).
3. **Case recovery.** Существующий Case-reader повторно обращается к original `commands/` и `outcomes/`; удалить их как «покрытый raw-журнал» без замены чтения нельзя. [`kaggle_cases_backend.py:389–458`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_cases_backend.py#L389-L458). Если оставить все такие объекты навсегда, рост истории полностью не устранён.
4. **Текущая голова журнала.** После продвижения очередная операция append должна прочитать сегмент текущей головы; сохранённый pointer `/head` без сегмента недостаточен. [`recovery_journal.py:665–675`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/recovery_journal.py#L665-L675).

**Почему P0-3 всё ещё нужен:** замер Шкипера (`25.10`) показал `ControlSpool≈50,6 МиБ`, Dataset journal objects `≈56,9 МиБ`, следующий `export_control≈80 МБ` при примерно 731 операциях; накопление при потолке 512 МиБ приведёт к отказу записи. Нужен не отказ от очистки, а **проверка существующих consumer references/restore pins и сохранение idempotency до удаления**. Уложить это в `≤30` production-строк и исходный scope пока не доказано.

**Решение для Opus:** уточнить `P0-3` с безопасным условием retention, обязанностью повторного запроса старого operation key и сохранением manifest+segment актуальной головы. По `0` запрещено молча расширять задачу или делать «голый DELETE». Отдельный PR не создавался; продакшен не менялся.

## 4. Перепроверка списка агентов из Slack

Источники: [личный тред «Упрощатель»](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792926181208919) и [общий тред](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792868771439369). Важна хронология: ранние замечания позднее **отзывались** либо относились к прежним HEAD. В таблицах ниже сравниваются текущий `main` и отдельно открытые PR.

### D1. PR #476 — недостижимая ветка worker resource-release

[`scripts/production/bge_wss_transport.py:146–185` на main](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/scripts/production/bge_wss_transport.py#L146-L185) и [`:417–426`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/scripts/production/bge_wss_transport.py#L417-L426).

**Подтверждён небольшой дубль:** в пути `_start` durable resource освобождается синхронно до первого `await`, поэтому альтернативное `else self.encoding` в `_maybe_release` не защищает актуальный durable path; вызов `_maybe_release()` в callback durable encoding лишний. Минимальная правка — оставить `task=self.read_task` и `changed.set()` в callback, не менять `forwards` и владельца пересылки. Гарантия при завершении уже выполняющегося forward **не** заменяется `forward_lock`. Влияние на latency не замерялось; это небольшое сокращение условий, **не P0-блокер**. [PR #476](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/476).

### D2. PR #477 — повтор `database_generation`, но `_Coverage` живой

[`geo_read_gate.py:72–76`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/geo_read_gate.py#L72-L76) и [`binding_key` в `source_read_gate.py:36–45`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/source_read_gate.py#L36-L45).

**Кандидат на удаление:** `binding_key(anchor.binding)==binding_key(binding)` уже сравнивает `database_generation`. У реальных writers `SourceCursor.database_generation` берётся из того же binding; отдельное `anchor.database_generation==binding.database_generation` ничего не различает. Снять ровно этот конъюнкт после регрессии о смене generation; не убирать всю проверку history/current.

**Не удалять `_Coverage` как «мёртвый legacy»**: найден реальный writer в [`source_history_verifier.py:360–374`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/source_history_verifier.py#L360-L374). Поздние фразы «старое замечание по #477 не подтверждается» не доказывают мёртвость `_Coverage` и не снимают именно избыточность дополнительного сравнения. Это два разных вопроса. [PR #477](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/477).

### D3. PR #472 — глобальное ожидание чужого Slack-диалога при восстановлении

[`expert_slack_search_reply.py:142–196` на HEAD PR #472](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_slack_search_reply.py#L142-L196).

`resume_pending()` строит цепочку: первая попытка следующего восстановленного запроса ждёт первую попытку предыдущего. Предыдущая включает поиск, resolution пользователя и Slack POST; независимый диалог становится зависим от чужой сети. Дополнительно задачи создаются почти одновременно, поэтому их двухсекундные таймауты могут истечь вместе и дать массовый старт после зависшей первой попытки.

**Кандидат ЗАМЕНИТЬ существующим admission** без глобального последовательного старта разных диалогов. Обязателен один различающий тест: несколько восстановленных запросов, первый зависает, независимые диалоги не ждут, порядок внутри одного треда не обгоняется. Не заявлять исправление без этого теста.

**Не удалять `request_order.is_head`.** Эта проверка [`:223–245`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_slack_search_reply.py#L223-L245) защищает порядок сообщений *одного* диалога перед Slack POST, чего глобальная startup-цепочка не обеспечивает. Прежнее замечание о трёх проверках доступа источника относится к старой версии кода; на текущем HEAD permit выдаётся после проверок, и проверка актуальности evidence отличается от подтверждения адресата доставки. [Участок permit/POST `:66–83`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_slack_search_reply.py#L66-L83).

**Релизный статус:** [Opus VETO по P0 freeze](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/472#issuecomment-6400740856) — запрет слияния фичи до устойчивой работы поиска/импорта, **не** суждение о ненужности `is_head` или недостаточном CI. [PR #472](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/472) остаётся открытым. До снятия freeze это аудит-кандидат, не поручение второму автору.

### K1. PR #492 — служебную очередь WSS **оставить**

[`bge_channel.py:378–388`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/bge_channel.py#L378-L388).

`service_frames`/последовательная служебная задача изолируют потенциально долгие обработчики `RECOVERY_PHASE_RESULT`, `SNAPSHOT_UPLOAD_INTENT` и `BIND_REQUEST` от общего socket reader. Предыдущее последовательное выполнение `await` прямо в receiver означало, что `QUERY_RESULT` не читается до возврата служебного вызова. Раннее освобождение worker permit, общий admission и encoder lock **не** устраняют `await` на стороне Dev receiver. Сохранено последовательное владение служебными кадрами.

[Различающий тест `test_bge_shared_channel.py:129–166`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/tests/test_bge_shared_channel.py#L129-L166): `service receive` задержан на 3 секунды, следующий `QUERY_RESULT` передаётся в `finish_read` раньше 200 мс. **Вердикт: ОСТАВИТЬ**, несмотря на новую очередь — это адресное исключение, прямо разрешённое исходным P0-1. [PR #492](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/492).

### K2. PR #400 — наблюдение текущей доставки **не равно** job state

[PR #400](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/400) открыт; его код нельзя смешивать с `main`. [`kaggle_source_commands.py:418–510` в PR](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a7c2c7177f58d6e8dbedb1bb5581bfa93bad6ef2/src/regional_knowledge/kaggle_source_commands.py#L418-L510).

Сценарий: Dev сохранил старую квитанцию `APPLIED`, но новая БД восстановлена из более раннего снимка; принятая операция переотправлена текущему native run. Если завершать новый delivery по старому outcome, cursor может продвинуться **до фактического replay в новую БД**. `claimed`/writer fencing не удостоверяют новое применение. Существующий [тест `test_kaggle_source_commands.py:443–495`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a7c2c7177f58d6e8dbedb1bb5581bfa93bad6ef2/tests/test_kaggle_source_commands.py#L443-L495) требует не завершать delivery до собственного native receive.

**Вердикт:** нельзя просто выкинуть `_delivery_observed`/delivery observation. Отдельную таблицу можно подвергнуть следующему анализу минимальности и попробовать хранить тот же факт в существующей записи job **только при доказанной эквивалентности**. Удаление гарантии без замены — недопустимо.

## 5. Замечания, которые следует закрыть или исправить в реестре

| Замечание агента | Уточнение и источник |
| --- | --- |
| PR #483 «новый навык, шесть файлов, 185 строк» | **Неверно указан PR.** [PR #483](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/483) меняет учёт useful-idle для авторизованного direct search; он не содержит заявленных шести файлов навыка `kaggle-warm-debugging`. Замечание снять с #483; оригинальное другое изменение отдельно не идентифицировано. |
| PR #478 «JOIN ломает evidence» | В `main` уже `LEFT JOIN` при hydrate без отдельного catalog fetch; [`kaggle_search.py:230–240`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/kaggle_search.py#L230-L240). Прежнее замечание исправлено, повторно не ставить. |
| PR #479 «два лишних timing-поля» | [Финальный diff PR #479](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/479/files) содержит адресные границы ожидания lock, native parse/render и API download. Раннее замечание относилось к иной обвязке; текущий код ей не соответствует. |
| Произвольный лимит 128 reconciliation hops | Убран [PR #480](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/480); не открывать повторный fix. |
| Лимиты 256 на постановку `document`/`source`/`recovery` jobs | Старые enqueue-квоты убраны; [`bge_queue.py:282–322`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/bge_queue.py#L282-L322). Другие 256 (например, existing recovery retry limit) — **другая** семантика. |
| Завершённые jobs навсегда в `LIVE_JOBS` | Применён единый retirement завершившегося run с `run_expired`; [`bge_queue.py:176–198`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/401f308e553e1b834f3526130595f0a68bdbb449/src/regional_knowledge/bge_queue.py#L176-L198). |
| PR #472 «проверка после единственного Slack POST ошибочно делает `UNKNOWN`» | Текущий HEAD переносит проверки *до* claim/POST; [`expert_slack_search_reply.py:66–83`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_slack_search_reply.py#L66-L83). |
| PR #472 «17-й вопрос не получает ответ из-за cap=16» | Limit относится к предварительным `prefetch`; wake создаётся по каждому принятому вопросу; [`expert_dot_events.py:236–267`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_dot_events.py#L236-L267). |
| PR #472 «ненужный повторный search после prefetch» | Текущий код использует сохранённый prefetch, новый поиск нужен при его отсутствии/истечении; [`expert_dot_events.py:356–387`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_dot_events.py#L356-L387). |
| PR #472 «массовый dot prefetch на restart» | `resume` использует `notify(..., prefetch=False)`, [`expert_dot_events.py:269–281`](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/93576807ed2db26a6fcb88e381cf01dfa5acacfd/src/regional_knowledge/expert_dot_events.py#L269-L281). Глобальный `sequential wake` — **отдельный** кандидат D3. |

### Особо: различать исправления и гарантии

Не принимать «эта проверка встречается дважды» за доказательство эквивалентности: сначала указать одинаковый факт, одинаковую границу доверия и ту же исходную квитанцию. `job state` не удостоверяет `current-run receipt`. Проверка `source evidence current` не равна проверке получателя сообщения. `is_head` одного Slack-треда не равен глобальному ожиданию startup. `service_frames` — не независимый provider/protocol, а единственная существующая последовательная обработка служебных кадров вне socket reader.

## 6. Живая приёмка: что подтверждено и что нет

[Сообщение Шкипера 25 октября о native157 / main401f308](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792962442209769?thread_ts=1792868771.439369&cid=C0C8RT9JB2P): штатный cold restart после операционной корректировки release config, один native run READY; свежая DjVu page85 реально показана; **hybrid search 638,527 мс backend и 672,404 мс HTTP**, хотя рендер той же страницы занимал **16,575 с**, ещё четыре page jobs оставались pending после окончания search. Это **одна** достоверная проверка поиска при рендере, не статистический p95 и не доказательство трёх часов стабильности.

**Checkpoint-overlap — `NOT RUN`.** К моменту проверки `seq731` уже полностью Saved, `pending control=0`; штатный checkpoint refresh закономерно `NOOP`. Шкипер не создавал фиктивных команд, не подменял часы и не начинал новый импорт ради демонстрации. Для продуктовой приёмки нужен реальный checkpoint с новыми dirty-данными.

Отдельно найден **cold-restart config fallback**: при `current_run=NULL` стартовый `source_restore` оставался на старом checkpoint v105 вместо verified v125 и блокировал API, `JournalConflict dataset_checkpoint_change_requires_verified_promotion`. Операционно запуск исправлен корректным release config; **дефект выбора актуального verified Saved в коде не исправлен**. [Сообщение о дефекте](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792962132787969?thread_ts=1792868771.439369&cid=C0C8RT9JB2P). Это самостоятельная техническая задача, не следует смешивать с F1/F2/F3 или менять без распределения авторства.

F1/F2 ещё не развёрнуты в live и не входят в приведённые цифры. Решения о деплое и live только Шкипер; merge только Opus.

## 7. Очерёдность решений, просьба к RKB-Opus

1. **Принять на независимое ревью F1/F2** — [#489](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/489) и [#488](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/488), exact HEAD и оба полных CI выше. Merge — исключительно Opus; live SQL/runtime — исключительно Шкипер.
2. **Исправить текст P0-3 до нового автора/PR**: сохранить незавершённые ссылки intake, исходный результат для duplicate, закреплённые restore bases, текущие голову/манифест/сегмент; определить точный retention контракт для Case. Отличающий тест — `DURABLE, covered, job done, consumer ещё pending → очистка → book_ingest status/duplicate/restore работают`. До разрешённого узкого scope — `BLOCKED`.
3. **Не отменять полезную WSS очередь #492**, наблюдение нынешнего replay в #400 и `is_head` в #472 по ошибочной эвристике «лишний слой». Прямые регрессии и ссылки выше.
4. **Мелкая чистка D1/D2** возможна только как отдельное вычитание с одним тестом на ресурс и смену generation, без нового состояния или улучшений «заодно». Ничего в live от неё немедленно не зависит.
5. **D3/PR #472** — только после решения о P0 freeze; сохранить per-thread ordering. Не назначать второго автора поверх действующего.
6. **Для runtime-приёмки отдельно**: checkpoint-overlap при возникновении реальной грязной базы и исправление cold-restart config fallback после разделения ownership.

**Смысл одной ссылки для передачи:** этот документ содержит весь итог *данного follow-up и верификации Slack*, прямые `file:line`, PR/CI/Slack-ссылки и очередность. Ранний большой независимый аудит — первый пункт источников; текущая сводка не повторяет все H1–H17 и не подменяет реестр #481. Достаточно передать Opus ссылку на этот файл или draft-PR с ним.