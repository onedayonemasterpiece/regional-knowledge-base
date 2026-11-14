# Конечный PG recovery: объём и проверка адаптера

Цель: восстановленная private PG после потери Dataset, с честным покрытием,
проверенным backup и конечными частями без online controller. Прямое решение
12 ноября: пилот около20 минут, затем части около2 часов с сохранением и перерывами.
Действующий контракт — rkb-single-batch-pg-recovery-crew-20261112.md.

Три основания: владелец потерял Dataset; exact journal сохранён, но автономного
вызывающего кода не было; прежняя оркестрация требует WSS/controller.

Фактический production diff: **+643 / -0 строк**, baseline250, дефицит393.
Все строки notebook включены: pg_rebuild_batch.py232, pg_rebuild_part.py338,
rkb_pg_recovery_notebook.py73. Это исходники, не выполненное восстановление корпуса.
Предварительное обоснование недостающих компонентов опубликовано до превышения
baseline. Оценка165–240 дополнительных строк оказалась ниже фактической: она
не учла полный stdlib entrypoint до импорта зависимостей, вход из физического
checkpoint, раздельные измерения и безопасный выход при ошибке. Код не уплотнялся,
protected materializer и domain guards не изменялись. Рост показан явно для ревью.

| Добавление | Конкретный предотвращаемый сбой | Проверенный существующий механизм и почему его недостаточно | Решение |
|---|---|---|---|
| SnapshotInput/rebuild | Подмена exact bytes и исходных rejected/unknown | RecoveryJournal и apply_accepted не читают локальный экспорт и не восстанавливают исторические rejected receipts | Оставлено |
| Исторический импорт/resolver | Текущий compiler отвергает старый fingerprint или меняет трактовку | Единственный текущий package не представляет15 версий; используются неизменные архивы и существующий fingerprint | Оставлено |
| GenesisPostgres | Текущий initializer не заполняет mandatory revision columns SQL11 | OwnedPostgres lifecycle переиспользован; заменена только инициализация пустой исторической схемы | Оставлено |
| Конечный caller и бюджет | Часть заканчивается без времени на backup либо обрывает COMMIT | Provider deadline не создаёт checkpoint; keep_going проверяется между транзакциями | Оставлено |
| Физический checkpoint/restore | Успешная упаковка не означает восстанавливаемую PG | Использованы pg_basebackup, pg_verifybackup, pack_tree/unpack_tree и applied_prefix; online factory требует scoped transport/binding и обновляет восстановленную схему до текущей | Оставлено |
| Source counts и timing | Частичный prefix или старый CPU профиль выдаётся за полный/измеренный результат | applied_ops и существующие SQL ограничения — источник фактов; отчёт сохраняет prefix, таблицы, source IDs, bootstrap/model/encoder/SQL/backup/restore отдельно | Оставлено |
| Stdlib Stage0 entrypoint | Импорт psycopg до установки зависимостей; private input попадает в working output | Существующий Stage0 запускается до импорта caller, assets идут в private temp; новая установка/загрузчик не созданы | Оставлено |
| Компактный error report | Текст принятого материала попадает в traceback | Исторические PG ошибки могут содержать private payload; наружу сохраняется только тип ошибки и publishable=false | Оставлено |

Один committed regression теперь собирает вымышленный gzip input потоковым
production importer, делает partial replay → настоящий logical dump/restore →
resume. Проверяются correction, rejected, unknown/not-sent, exact receipts и
дубликаты. Отдельная локальная проверка production physical helpers использовала
тот же изолированный PG17 container: настоящий WAL backup, pg_verifybackup,
сжатый output, распаковка именно этого output, запуск восстановленной PG,
сверка prefix/coverage и следующая correction с duplicate no-op. Docker заменял
только локальный транспорт к native tools. Это synthetic PG17/pgvector0.8.2,
не модель, не реальный корпус и не проверка native Ubuntu24 bundle на Kaggle.

Не добавлены scheduler/supervisor, WSS, provider API/retry, новый compiler,
embedding cache, actor identity или модельный режим. Текущий pinned worker — CPU;
GPU совместимость не доказана. Старые16,2ч не ETA и не прогноз GPU.

Ни submit, ни Dataset publication не выполнялись. Для реального пилота нужны
разрешённый доступ после бана, фактическая среда/лимиты, проверенный native bundle,
закреплённые model/wheel mounts и привязанный к версии config/receipt. Зелёные
фикстуры не заменяют эти результаты и не являются заявлением готовой PG.

13 ноября: public Dataset не содержит pinned wheels и имеет другой tokenizer_config. Entry добавляет3 production lines (+646 суммарно) для использования уже существующего Stage0Download вместо предположения о полном mount. Дополнительный downloader/cache/retry не добавлен. Guard и версии сохранены. Синтаксис entry проверен; прежние regression receipts действуют для неизменённого replay/backup.

Фактическая привязка notebook добавляет81 production lines launcher: ABI/ресурсы перед тяжёлой работой, существующая OS identity, пути из retained manifest, точный input mount и public weight staging. Итог727 строк против baseline250, дефицит477: прежний caller не мог привязаться к ещё неизвестной Kaggle среде. Это конкретный недостающий deployment компонент, не новый replay/compiler/scheduler. Проверки ABI и peak32GiB/12GiB предотвращают запуск несовместимых CP313/Ubuntu24 native bytes и известный риск нехватки места при backup/restore. Чувствительный текст исключён из exception report.

13 ноября, исправление фактической доставки: pilot version1 завершился ERROR до PG/модели. Kaggle распаковал входные .tar.gz и убрал исходные archive bytes, необходимые существующим hash/fingerprint guards. Launcher теперь извлекает один opaque recovery-input.blob в private temp; затем прежний notebook проверяет все исходные файлы. Удалён поиск manifest в уже распакованном provider mount. Итог739 production lines против baseline250, дефицит489; изменение launcher +18/-6. Один тест проверяет сохранность вложенного .tar.gz побайтово. Новых retry/controller/cache и ослабления integrity нет.

| Добавление | Конкретный сбой | Существующий механизм и почему недостаточен | Решение |
|---|---|---|---|
| Opaque input unpack | Provider version1 превратил immutable archives в каталоги, caller получил FileNotFoundError | Tarfile/data filter уже используется для code; provider auto-extract происходит раньше notebook, поэтому нужны непрозрачные transport bytes | Оставлено; прежние guards проверяют содержимое |

Pilot2 terminal ERROR/Stage0Error до PG/BGE; version-specific error/environment outputs сохранены. Локальное исполнение трёх исходных строк staging при umask077 воспроизводит stage0_preparation_failed: rkb-artifacts создаётся0700, тогда как существующий Stage0 требует прохода по этому корню для nonroot PG. Launcher создаёт только свой новый artifacts root0711; model directory остаётся0700, данные и файлы не становятся общедоступными. Исходный guard не меняется. Фикстура запускает настоящий Stage0 с уже имеющимися вымышленными bytes, без сети/модели/pip; baseline RED, исправление GREEN, затронутые36 тестов прошли. Live причина ещё требует проверки исправленным пилотом.

Итоговый production diff +748/-0, baseline250, дефицит498; последнее изменение launcher +12/-3. Stage0Error report теперь сохраняет его фиксированный код; PG exception text по-прежнему исключён. Новый входной Dataset не нужен: меняется только notebook launcher, прежний opaque input version2 переиспользуется. Нового submit до окончания60мин перерыва нет.

| Добавление | Наблюдаемый сбой | Существующий механизм и почему недостаточен | Решение |
|---|---|---|---|
| Owned artifacts root0711 и stage_weight | Pilot2 Stage0Error; старый staging под umask077 воспроизводит отказ Stage0 на0700 | Stage0 требует traversal nonroot PG; прежний launcher создавал только temp0711, вложенный artifacts root получал0700 | Оставлено; chmod только вновь созданного собственного каталога |
| Fixed Stage0Error reason | Выход pilot2 скрывал конкретный код Stage0 за одним class name | В Stage0Error все сообщения — фиксированные коды, доменные PG ошибки могут содержать private text и не выводятся | Оставлено |

Pilot3 прошёл Stage0 download/install и затем завершился RuntimeError. Прежний notebook исполнял sync main прямо в активном Jupyter loop, а finite entry вызывает asyncio.run. Этот вызов в существующем loop локально воспроизводит RuntimeError; remote traceback ранее скрывался, поэтому live проверка причины ещё впереди. Notebook теперь вызывает await notebook_main(), который исполняет прежний конечный main через stdlib asyncio.to_thread и ожидает его до завершения. CLI main остаётся штатным. Нет background task, второго controller или замены event loop.

При ошибке сохраняются максимум6 имён модулей/функций и номера строк traceback без source lines, locals и exception text PG: прежнего class name оказалось недостаточно для диагностики pilot3. Три delivery/bootstrap regression PASS, nested asyncio baseline RED. Production+757/-0, baseline250, дефицит507; последнее изменение+9/-0. Весь фактический notebook launcher включён в подсчёт.

| Добавление | Наблюдаемый сбой | Существующий механизм и почему недостаточен | Решение |
|---|---|---|---|
| Awaited notebook_main/to_thread | RuntimeError после Stage0; вложенный asyncio.run воспроизводит такой отказ | Jupyter уже владеет loop, finite caller должен закончить собственный; stdlib thread bridge ожидается одной notebook cell | Оставлено |
| Bounded traceback frames | Pilot3 показывал только RuntimeError без места сбоя | Class-only report исключает private payload, но теряет местоположение; сохраняются только public module/function/line | Оставлено |

Прямое решение владельца13ноября заменяет прежний перерыв после каждого startup ERROR: исправленный запуск продолжается сразу после готовности. Перерыв остаётся после продуктивной части с проверенным результатом. На стадии старта Crew проверяет существующий запуск раз в3мин, без polling-loop.

Part002 version6 завершился QueryCanceled/57014 на операции333: исторический
materialize_packet задаёт5с перед prepare_source_batch_v1. Live RPC deadline
прерывает конечный offline batch и не является проверкой содержимого. Offline
connection теперь использует оставшийся существующий бюджет части вместо этих
SET LOCAL statement_timeout; запас на backup остаётся480с. Exact SQL функций,
materializer fingerprints, model/chunks и domain guards сохранены. Production
+9 строк: всего766 при baseline250, дефицит516. Измеряющий connection вынесен
в фабрику для настоящей синтетической проверки PG17: короткий timeout отменяет
pg_sleep до исправления, внутри бюджета запрос проходит, CHECK violation остаётся,
истёкший бюджет отменяет запрос. Новых retry, checkpoint-on-error, флагов,
таблиц или сервисов нет. Убрана привязка offline SQL к live request timeout.
Полный корпус и операция333 после исправления пока не проверены.

| Добавление | Наблюдаемый сбой | Существующий механизм и почему недостаточен | Решение |
|---|---|---|---|
| Part deadline в существующем measured connection | Version6 QueryCanceled в prepare_source_batch_v1 на333 | Live5с ограничивает отдельный RPC; конечный caller уже имеет7200с с480с на сохранение | Оставлено; используется прежний конечный бюджет, domain SQL не меняется |

14 ноября, version7: ошибка QueryCanceled возникла на6745.68с notebook,
при исчерпании6720с рабочего бюджета внутри historical materialize_packet.
Старый caller прерывал часть до обычного save_checkpoint, теряя новый
префикс этой попытки. Теперь только отмена подготовки после исчерпания
существующего бюджета возвращает последний применённый префикс в обычное
сохранение; QueryCanceled до срока по-прежнему ошибка. Незавершённая команда
не считается применённой и продолжается из checkpoint без повторения
принятых операций. Транзакционный rollback остаётся за psycopg/PG.
Production +13 строк, всего779, baseline250, дефицит529. Существующий
счётчик offline_pg_rebuild направлен на stderr только своего logger.
Новых retry, режимов, флагов и сервисов нет; domain handlers и guards неизменны.

| Добавление | Наблюдаемый сбой | Существующий механизм и почему недостаточен | Решение |
|---|---|---|---|
| Обработка QueryCanceled после истечения существующего бюджета | Version7 ERROR без checkpoint на границе части | keep_going проверялся только до команды, timeout отменяет SQL внутри неё; PG откатывает незавершённую транзакцию | Возврат последнего префикса в обычный save_checkpoint |
| Вывод существующего progress logger | Весь replay не имел видимого счётчика | LOG.info уже есть, но notebook не включал его | Подключён стандартный StreamHandler только к этому logger |
