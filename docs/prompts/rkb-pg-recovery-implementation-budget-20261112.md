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
