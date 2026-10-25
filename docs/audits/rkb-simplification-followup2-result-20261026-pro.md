# RKB — результат follow-up 2: F4 и F5

Дата задания: **26 октября 2026 года**. Адресат: **RKB-Opus (Claude)**. Автор реализации и сводки: ChatGPT.

Исходное задание: [followup2 RKB-Opus](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-followup2-20261026-opus.md).
Правила scope и тестов: [§0 основных заданий](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/docs/rkb-simplification-tasks-20261025/docs/audits/rkb-simplification-tasks-20261025-opus.md).
Предыдущая передача с подробным обоснованием отмены удаления P0-3: [единая сводка, PR #494](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/494).

Обе независимые кодовые ветки созданы от `main` **392318d9a5dfb6e7dcfa3c36d379a125bd44660c**. Предыдущие F1/F2 уже вошли в этот main. В текущем исполнении разрешены только F4/F5: новые микроупрощения, PR #472, другие пункты SQL-аудита и live-операции не выполнялись.

## 1. Итог для принятия решения

| Задание | Изменение | Draft PR и exact HEAD | Полный CI |
| --- | --- | --- | --- |
| **F4** — бюджет несохранённого хвоста | Два SQL-предиката; данные не удаляются | [#496](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/496), `b6460915a5e92451984e0f27218c5c4f3f8a31d6` | **PASS**, Python 3.12/3.13 — [run 40240812818](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40240812818) |
| **F5 / SQLA-01** — стабильный fetch старого пассажа | Удалены JOIN, столбец eligibility и его сравнение | [#497](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/497), `115c013c639d69b9d08773b774e84351c6f231e3` | **PASS**, Python 3.12/3.13 — [run 40241643395](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40241643395) |

**Оба PR открыты как draft, изменения не слиты и не развёрнуты.** Итоговые HEAD сверены с GitHub перед передачей; в обоих PR приложены результаты полного CI. Слияние — решение RKB-Opus, live-установка — Шкипер.

Результат — подготовленные и проверенные изменения исходного кода, **не установка в production и не живая приёмка**. Новые значения p95 не измерялись.

## 2. F4: исключить сохранённую историю из бюджета pending-хвоста

### Что изменено

[ControlSpool._used_bytes](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b6460915a5e92451984e0f27218c5c4f3f8a31d6/src/regional_knowledge/control_spool.py#L598-L607):
в сумме операций теперь только `WHERE outcome_state<>'DURABLE'`.

[DatasetRecoveryStore._put](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b6460915a5e92451984e0f27218c5c4f3f8a31d6/src/regional_knowledge/dataset_recovery_store.py#L201-L219):
в сумме объектов того же storage теперь только `AND covered_version IS NULL`.

**Бюджет правки:** ровно две изменённые production-строки — по одному предикату. [Полный diff #496](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/496/files).

| Удалено/заменено | Какой сбой это вызывало | Что теперь покрывает | Проверка |
| --- | --- | --- | --- |
| Начисление уже DURABLE операций на pending-бюджет | История завершённой работы запрещала принятие новых команд | Прежний лимит и прежняя формула резервирования применяются только к несохранённым операциям | Новый различающий тест на реальном SQLite и ControlSpool |
| Начисление покрытых Dataset объектов на pending-бюджет | Сохранённые объекты исчерпывали 512 MiB без роста несохранённого хвоста | Существующий `covered_version`; прежний лимит и CAS | Существующие тесты Dataset store и полный CI; отдельного нового теста границы 512 MiB для этого предиката не добавлялось |

### Что осталось неизменным

При `apply_bytes IS NULL` учитывается **reserved_bytes**, а не фактический короткий payload. Резервы `rkb_control_source_heads` включены как раньше. Значения лимитов, действительные состояния, receipt/cursor, CAS и promotion не менялись.

**Нет DELETE и нет очистки:** сохранены исходные команды, outcomes, compact receipts, журнальные объекты, intake pending_operation_key и закреплённые базы восстановления. `export_control` и формат `journal-control.json` не изменены.

### Один новый различающий тест

[tests/test_kaggle_native_source_importer.py:260–304](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/b6460915a5e92451984e0f27218c5c4f3f8a31d6/tests/test_kaggle_native_source_importer.py#L260-L304):
`test_journal_budget_counts_unsaved_tail_and_retains_import_progress`.

Использованы реальные OAuth, SQLite, ControlSpool, журнал и `book_ingest`; оригинал и native worker receipts синтетические, как в существующей fixture. Три операции первого пакета штатно доводятся до DURABLE через `publish_outcome`, причём последняя ещё указана в pending_operation_key импортёра.

Для отрицательной ветви **только в локальной SQLite fixture** этим же трём строкам временно выставляется `NOT_SENT`: при малом бюджете действительный `_stage` получает `SpoolFull`. Затем исходные состояния возвращаются: `book_ingest status` читает прежнюю сохранённую pending-операцию, возвращает `next_cursor='4'`, и следующий пакет принимается. Сравнение прежних строк подтверждает неизменность command/outcome bytes, состояний и резервов.

Это тест accounting и сохранности пути продолжения, а не имитация production checkpoint или доказательство живого Saved.

### Проверки F4

Локально на Python 3.14: **93 passed**, без пропусков в выбранных трёх файлах:

```text
tests/test_kaggle_native_source_importer.py
tests/test_dataset_recovery_store.py
tests/test_control_spool.py
```

Полный [CI run 40240812818](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40240812818) — **PASS** на exact HEAD `b6460915a5e92451984e0f27218c5c4f3f8a31d6`: Python 3.12 и 3.13, реальные PG17/pgvector fixtures. Обе matrix jobs завершены успешно. Дополнительная [проверка launch envelope](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40240812814) также PASS.

**Остаточное ограничение:** объём истории на диске и `journal-control.json` продолжает расти. F4 исправляет ложное начисление истории на лимит pending-хвоста, но не физическое хранение, размер экспорта или длительность checkpoint. Удаление P0-3 отменено заданием; отдельное решение по росту экспорта здесь не реализовано.

## 3. F5: старый fetch ID не зависит от нового пакета той же книги

[read_continuation в kaggle_query.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/115c013c639d69b9d08773b774e84351c6f231e3/src/regional_knowledge/kaggle_query.py#L265-L321) больше не выбирает `v.eligibility_revision`, не присоединяет `source_visibility` и не сравнивает `row[6]` со старым locator.

Добавление нового пакета увеличивает eligibility_revision всей книги, но не меняет ранее опубликованный immutable passage. Старое сравнение давало `evidence_withdrawn` для неизменённого текста. Теперь чтение опирается на оставшиеся точные условия.

| Удалено/заменено | Какой сбой это вызывало | Что теперь покрывает | Проверка |
| --- | --- | --- | --- |
| Eligibility JOIN и сравнение с ID прошлой выдачи | Новый пакет B той же книги делал недоступным старый пассаж A | passage_id + source_id + content revision, active_revision, accepted/not withdrawn, database_generation | Новый real-PG тест search A → actual packet B → fetch того же ID A |

**Production — только вычитание** столбца/JOIN/конъюнкта; из docstring удалено упоминание отменённого eligibility fence. Unified diff показывает заменённые SQL-строки, а не новую логику. [Diff #497](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/497/files).

### Сохранённые гарантии

Без изменений: точный passage/source/revision, активная ревизия, `s.accepted AND NOT s.withdrawn`, поколение БД, общий deadline, locator/ID codec и offsets, SQL-функции, precondition команды снятия книги, private Case. Сохранились существующие отрицательные PostgreSQL-регрессии:

- [Смена активной ревизии запрещает прежний locator](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/115c013c639d69b9d08773b774e84351c6f231e3/tests/test_kaggle_query.py#L299-L327).
- [Другое database_generation запрещает прежний locator](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/115c013c639d69b9d08773b774e84351c6f231e3/tests/test_kaggle_query.py#L330-L341).
- [Снятая книга не выдаётся при continuation](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/115c013c639d69b9d08773b774e84351c6f231e3/tests/test_kaggle_query.py#L420-L450).

### Один новый различающий тест

[tests/test_source_batch_pg.py:110–162](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/115c013c639d69b9d08773b774e84351c6f231e3/tests/test_source_batch_pg.py#L110-L162):
`test_search_fetch_id_survives_next_packet_of_same_book`.

В настоящей PostgreSQL 17 выполняются existing stage/text accepted-команды и materializer первого пакета. Настоящий `execute_query` находит A; прежним `passage_fetch_id` кодируется ID из выданного evidence. После этого второй пакет той же книги проходит реальное применение. Тест отдельно подтверждает увеличение `eligibility_revision`, после чего `execute_passage_read` со старым ID возвращает тот же passage_id, текст и provenance. ID не пересоздаётся по новым ревизиям.

BGE/tokenizer callbacks синтетические; веса и модель локально не запускались. Проверяются SQL, материализация пакетов и реальная граница чтения, не качество семантического ранжирования.

### Проверки F5

Локально на Python 3.14: **48 passed, 37 skipped**, поскольку PostgreSQL fixture на Dev для этого запуска не настроена. Новый PG-тест и соответствующие отрицательные PG-сценарии локально пропущены; локальный прогон не выдаётся за их прохождение.

Первый [CI run 40240884416](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40240884416) обнаружил два недочёта тестов: прежний параметр ожидал отменённый eligibility-only отказ, а новая PG-фикстура передавала списки вместо строгих tuple в QueryFilters. Устаревший параметр удалён, тип фильтров исправлен; production diff не расширялся. [Корректирующий test-only коммит](https://github.com/onedayonemasterpiece/regional-knowledge-base/commit/115c013c639d69b9d08773b774e84351c6f231e3).

Полный [CI run 40241643395](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40241643395) — **PASS** на Python 3.12 и 3.13 с PostgreSQL17/pgvector, для итогового HEAD `115c013c639d69b9d08773b774e84351c6f231e3`. Проверен merge-ref `866c7105c374698f030153a05ee2a235a080c09e` (этот HEAD + main392318d9). В логе Python 3.12: **3598 passed, 42 skipped, 5 warnings**; новый PG-сценарий и сохранённые PG-регрессии выполнялись с настроенным disposable test DSN. Дополнительная [проверка launch envelope](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/40241643386) также PASS.

## 4. Полнота и границы передачи

Проверены diff обоих PR: изменены ровно **3 файла F4** и **3 файла F5**; по одному новому тесту на задание. В F5 удалён один устаревший параметр теста, требовавший отказа только из-за eligibility revision; проверки другого поколения БД, offsets, снятия книги и активной ревизии сохранены. 12 materializer-fingerprint файлов, `.devcoveer/requirements.json`, workflow и SQL migrations не менялись. Новые состояния, таблицы, очереди, таймеры, retry, настройки и метрики не введены.

[Штатный полный CI](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/392318d9a5dfb6e7dcfa3c36d379a125bd44660c/.github/workflows/ci.yml) запускает `pytest` на Python 3.12 и 3.13 с PostgreSQL17/pgvector и явным disposable test DSN.

Исходники правились только в двух изолированных checkout. Чужие рабочие ветки и default checkout не переключались. Прочитаны AGENTS, правила упрощения, основное задание и followup2, repo-копия kaggle-warm-debugging/SKILL.md и rkb-profile; автоматическое обнаружение/установка skill в чужой агент не заявляются.

**Следующий шаг Opus:** независимое ревью exact HEAD обоих draft PR и решение о слиянии. **Следующий шаг Шкипера после слияния:** обычная установка назначенных изменений и live-приёмка старого fetch ID после нового пакета.

**Свежий контекст, не результат F4/F5:** Шкипер уже [сообщил о checkpoint-overlap на main392318d9/native158](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792967571830189?thread_ts=1792868771.439369&cid=C0C8RT9JB2P): 12 MCP-поисков во время capture/upload, 278,404–361,078 мс, Saved126/readback; [dot принял этот отчёт](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792967661925879?thread_ts=1792868771.439369&cid=C0C8RT9JB2P). Здесь прочитан именно отчёт оператора, raw runtime receipts повторно не проверялись. Этот результат относится к предыдущей установке F1/F2, а не к новым F4/F5 и не доказывает трёхчасовой SLO/массовый импорт. Прежний NOT RUN по checkpoint-overlap больше не следует переносить как актуальный статус.

**Для передачи достаточно ссылки на этот файл:** все PR, exact HEAD, CI, тесты, код, исходное задание и остаточные ограничения приведены здесь.
