# RKB: простые миграции схемы в живом Kaggle — цель для исполнителя

Автор: RKB-Opus, 2026-10-22. Исполнитель: Шкипер. Проверка результата: RKB-Opus.
База: `origin/main@d7ad1e1` (+PR272). ТЗ: `docs/prompts/rkb-scalable-kaggle-implementation-20261019.md@bd138b2`.

## 0. Цель (одним абзацем)

Изменение схемы (новая колонка/таблица/функция) = **один файл `sql/kaggle/NNN_*.sql` + одна команда применения к живому PG в уже запущенном ноутбуке**. Notebook, PG и BGE продолжают работу; новый Kaggle push, новая Dataset version, отдельный run, drill, заранее измеренные хэши каталога/ACL и ручные пины не нужны. Восстановление из любого старого снимка Dataset = restore → автоматически применить недостающие миграции → replay хвоста. Это прямо требование ТЗ:

- стр. 43: «Обычные SQL/app изменения применяются в уже поднятом Kaggle через existing controller/WSS и точные artifacts; новая Dataset version/новый notebook не нужны каждой итерации»;
- стр. 290: «SQL/function/совместимая schema → применить exact SQL migration к живому PG; notebook, PG и BGE продолжают работу»;
- стр. 102: на restore проверять **PG major, extension/model/tokenizer/schema/protocol versions** — версии, а не хэш всего каталога.

## 1. Что сломано сейчас (факты 2026-10-22)

| # | Механизм | Где | Почему хрупко |
|---|---|---|---|
| 1 | Readiness/restore/история сверяют `catalog_sha256` всего PG-каталога и `acl_sha256` | `kaggle_database_probe.py:78-130,198-204`, `source_history_verifier.py:93-98,202,219,275,305,451,478,510,537`, `kaggle_source_history_native.py:116-209`, `pg_snapshot_recovery.py:151-228`, `kaggle_pg_snapshot_native.py:528-580`, `source_history_wss.py:22-109,284-294` | Каталог включает `server_version_num`, collation/glibc, owners/ACL по именам ролей → хэш нельзя посчитать в CI, только на Kaggle. Каждая смена схемы требует «измерительного» run (сегодня: drill_migrate PR265 на 600 строк, PR267, run B1/B2). |
| 2 | Тройка `schema/function/executor_revision` + ручные парные переходы | `geo_manifest_commands.py` (`GEO_REVISIONS`, `GEO_V2_REVISIONS`), `source_activation.py` (`ACTIVATION_REVISIONS`), `kaggle_restore_startup.py:86-97` (`ordered_upgrade` только 11→12/11→14), `kaggle_target_runtime.py:264` (materializer только при `rkb-v4-sql001-014-v1`) | Каждая новая схема = новая константа + новый путь + новые условия в 5+ файлах. |
| 3 | Миграция = journal-команда `schema_transition` с пинами check/apply функций | `kaggle_geo_upgrade.py` (`GeoUpgradePins`), `kaggle_native_source_upgrade.py` (`NativeSourceUpgradePins`, `transition_sql`), `kaggle_geo_install.py:111-160` (stage/seal, «never dispatched to the old worker»), manifest `scripts/production/manifests/rkb-migration-pins-39793744232.json` | Переход заморожен до следующего run (противоречит стр. 43/290); пины меряются в отдельном CI; ошибки stage/seal/continuation (PR269/271). |
| 4 | Старые handlers хранятся вечно ради replay | `kaggle_source_commands.py:fixed_registry` (кап 32→64, PR263), registry 9→33, `source_history_wss.py` origin closure (PR270) | Растёт registry, launch config 64→96 KiB (PR264/272), restore падает на `registry9 vs registry33`. |
| 5 | Полный registry в launch config | `kaggle_target_launch.py:compose_target` (`target_runtime.command_handlers`), `kaggle_bootstrap.py:407`, `kaggle_readiness.MAX_LAUNCH_CONFIG_BYTES` | Размер конфига растёт с каждой миграцией → новые пределы, новые падения (v110). |

## 2. Целевая модель

### 2.1 Линейные миграции (единственный механизм изменения схемы)

- Файлы уже пронумерованы: `sql/kaggle/001_search.sql … 016_story_reconcile.sql`. Новые — строго `NNN_<имя>.sql`, следующий номер, файл после merge **не редактируется** (только новый файл).
- В PG: таблица учёта
  ```sql
  CREATE TABLE IF NOT EXISTS rkb_v4.schema_migrations(
    version int PRIMARY KEY, name text NOT NULL, sha256 text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now());
  ```
- Runner (в worker bundle, один модуль, напр. `kaggle_schema_migrate.py`): берёт файлы из уже верифицированного source bundle, для каждого `version > max(applied)` по порядку: `BEGIN; <sql>; INSERT INTO rkb_v4.schema_migrations …; COMMIT;`. PostgreSQL выполняет DDL транзакционно — частичного применения нет. Если строка с таким `version` уже есть и `sha256` совпадает — пропуск; не совпадает — явная ошибка `migration_file_changed` (это бухгалтерия «файл после применения изменили», не проверка базы). Advisory lock на время применения, чтобы не было двух исполнителей.
- **Baseline для существующей базы:** при первом запуске runner, если таблицы нет, а `recovery_state.schema_revision` = `rkb-v4-sql001-011-v1` / `-012-` / `-014-` — вставить строки 001..N как уже применённые (с sha текущих файлов) без выполнения SQL. Соответствие revision→N зафиксировать одной таблицей в коде. Файлы 015/016: проверить, к какому revision они относятся, и включить по факту наличия их объектов (`to_regclass`/`to_regprocedure`).
- **Применение к живому ноутбуку:** одна фиксированная WSS-команда/recovery-phase `schema_migrate` (через существующий controller/WSS, owner/writer fence как у остальных записей). Аргументов-SQL нет: worker применяет только файлы из своего bundle. Чтобы доставить **новый** `.sql` в уже запущенный ноутбук, использовать существующий путь обновления source bundle в живом процессе (ТЗ стр. 290/298: exact artifact, sha проверен). Если такого пути сейчас нет — это первый пункт работы (п. 3, PR-1b), не повод для нового run.
- **После каждой применённой миграции — сразу Dataset checkpoint** (существующий snapshot путь). Следствие: хвост журнала при restore **никогда не пересекает миграцию**, значит replay всегда идёт текущими handlers.

### 2.2 Readiness и restore по версиям, без хэшей каталога/ACL

- `READY`/restore/история проверяют: PG major = 17, версии pgvector/PostGIS (как сейчас), model/tokenizer/protocol (как сейчас), **`max(schema_migrations.version)` == ожидаемая версия кода** (константа = номер последнего файла в bundle).
- Удалить сравнение `catalog_sha256` и `acl_sha256` из: `source_history_verifier.py`, `kaggle_source_history_native.py`, `pg_snapshot_recovery.py`, `kaggle_pg_snapshot_native.py` (`finish`, `verify_initialized`), `kaggle_snapshot_startup.py:75,207,219`, `kaggle_restore_startup.py:92-97,206`, `source_history_wss.py`. Из source-service config и target descriptor убрать поля `catalog_sha256`, `acl_sha256`, `geo_dependency_upgrade`, `native_source_upgrade`.
- `NativeDatabaseProbe` оставить только для полей готовности (`applied_seq`, `projection_seq`, `text_search_ready`, `dense_search_ready`, generation), без требования idle/repeatable-read для хэша каталога, если это нужно было только хэшу.
- **Совместимость со старыми снимками:** Dataset control с полями `catalog_sha256/acl_sha256/closure_transitions` читается, поля игнорируются (не сравниваются). Новые control пишут `schema_version` вместо них. Dataset2/base20 обязан восстанавливаться (тест).
- Права на данные (product ACL, eligibility, grants в SQL) **не трогать** — это функциональность, а не «проверка подлинности базы». Удаляем только хэш-пин `inspect_acl` как условие готовности.

### 2.3 Убрать ревизионные тройки и парные переходы

- Заменить `schema_revision/function_revision/executor_revision` одним целым `schema_version` (в `recovery_state`, HELLO/READY, runtime expectation). Старые строковые ревизии читаются только в baseline (п. 2.1).
- Удалить `ordered_upgrade` (`kaggle_restore_startup.py:86-97`), `GEO_V2_REVISIONS`/`ACTIVATION_REVISIONS` как условия путей, `kaggle_target_runtime.py:264` → `schema_version >= <номер миграции 014>`.
- Удалить `schema_transition` как вид journal-команды, `GeoUpgradePins`, `NativeSourceUpgradePins`, `transition_sql`, `geo_dependency_repair_commands` в части transition, stage/seal в `kaggle_geo_install.py:_dependency_repair` (повторные GEO batch-команды, если нужны по данным, — обычные команды после миграции), `scripts/production/manifests/rkb-migration-pins-*.json`, тесты/CI-измерения пинов.
- PR265 (drill_migrate) закрыть без merge. `kaggle_snapshot_drill.py`: оставить, только если используется для пустого drill (`drill_empty`); иначе удалить.

### 2.4 Registry только текущих handlers, вне launch config

- Handlers ключуются по `route` для текущей версии схемы; исторические копии (`registry9`, origin closure PR270) не нужны, т.к. хвост не пересекает миграции (п. 2.1).
- Worker строит registry из своего кода/bundle; launch config несёт только **хэш** registry для сверки, а не сами записи. После этого предел `MAX_LAUNCH_CONFIG_BYTES` вернуть к 64 KiB или убрать отдельный предел для handlers. `fixed_registry` кап — по числу текущих routes.

## 3. Порядок работ (каждый PR в `main`, CI зелёный, маленький)

| PR | Содержание | Тесты (обязательно) |
|---|---|---|
| PR-1a | `schema_migrations` + runner + baseline 011/012/014(/015/016) | PG17 CI: (a) пустая БД → 001..016, (b) база на 011 → baseline → 012..016, (c) повтор = no-op, (d) изменённый применённый файл → `migration_file_changed`, (e) падение посреди файла → откат, версия не растёт. Сравнение итоговой схемы (a) и (b): `pg_dump --schema-only -n rkb_v4`, нормализованный — **только в CI**, как тест, не как runtime-гейт. |
| PR-1b | Команда `schema_migrate` через существующий controller/WSS + доставка нового `.sql` в живой bundle (если ещё нет) + checkpoint после миграции | Fake WSS: команда без запущенного writer отклоняется; повтор идемпотентен; после успеха запрошен snapshot. |
| PR-2 | Readiness/restore/история по `schema_version`; удалить сравнения catalog/ACL; старый control читается | Restore Dataset2-формы (control с catalog/acl/closure) → READY; restore базы на версии N-2 → runner применяет 2 миграции → replay хвоста → READY. |
| PR-3 | Удалить тройки ревизий, `ordered_upgrade`, `schema_transition`, пины, manifest, stage/seal transition в GEO installer, origin closure | Полный `pytest`; grep-тест: в `src/` нет `catalog_sha256` в условиях готовности и нет `GEO_V2_REVISIONS`/`ACTIVATION_REVISIONS` в путях restore. |
| PR-4 | Registry вне launch config, только текущие handlers; предел конфига | Actual-metadata тест: launch config < 64 KiB при всех текущих handlers. |

Порядок применения в проде: PR-1a+1b+2 → один обычный запуск ноутбука (последний «большой») → дальше любые схемы только через `schema_migrate` в живом ноутбуке.

## 4. Что не трогаем (остаётся как есть)

Writer fencing/generations, run identity/nonce, UNKNOWN-reconciliation, terminal proof, единственный controller, immutable Dataset versions и их sha, durable outcomes/idempotency keys, product ACL/eligibility в SQL, PG major/extension/model/tokenizer проверки, запреты: не печатать секреты, PG на Dev не запускать, релиз только из `main`.

## 5. Приёмка (проверяет RKB-Opus)

1. В живом ноутбуке добавить тестовую миграцию `NNN_probe_column.sql` (nullable колонка) → `schema_migrate` → `max(version)` вырос, поиск отвечает без перерыва, Kaggle push не было, Dataset checkpoint создан.
2. Restore из снимка до этой миграции → миграция применилась автоматически → READY.
3. В `src/` нет требований заранее измеренных `catalog_sha256/acl_sha256`, нет парных `ordered_upgrade`, нет `schema_transition` пинов; config/descriptor без этих полей.
4. Launch config ≤ 64 KiB; registry только текущих routes.
5. Первая книга импортирована и находится поиском (если ещё не сделано текущей цепочкой v110 → run B).

## 6. Приоритет относительно первой книги

Текущая цепочка v110 → run B (через PR267: первый run B пишет реальный хэш, второй стартует) **доводится до первой книги без новых изменений**. Эта цель выполняется сразу после неё; если run B снова упрётся в хэш каталога/ACL — не делать третий измерительный круг, а переходить к PR-1a/1b/2 и применять 012–014 миграцией в живом ноутбуке.
