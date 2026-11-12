# Конечный PG recovery: объём адаптера

Принятое изменение 12 ноября: пилот около 20 минут, проверенный checkpoint,
затем части около двух часов с реальными перерывами. Источник и действующий
контракт — `rkb-single-batch-pg-recovery-crew-20261112.md`.

Три основания: владелец потерял Dataset; точные команды сохранены, но автономного
вызывающего кода нет; прежняя оркестрация требует online controller. Нужен прямой
локальный вызов существующих materializer, PG apply и backup tools.

Текущий production diff: **+232 строки**, удалений нет. Он читает exact input,
сохраняет исходные отказы и неизвестный хвост, проверяет исторические receipt,
загружает неизменные исторические compiler-модули и останавливает replay между
транзакциями по бюджету вызывающего кода. Это ещё не готовый notebook.

| Добавление | Предотвращаемый сбой | Почему существующего механизма недостаточно |
|---|---|---|
| SnapshotInput и rebuild | Новая трактовка принятых команд и отказов | apply_accepted не читает локальный экспорт и исторические rejected receipts |
| Исторический импорт | Новый compiler отвергает старый fingerprint или меняет смысл | Один текущий package не представляет 15 сохранённых версий |
| keep_going между транзакциями | Часть съедает время сохранения, либо прерывается COMMIT | Срок runtime сам не создаёт проверенный checkpoint |
| Продолжение через существующие applied_ops | Повторное вычисление и дубль эффекта после restore | Используется имеющаяся PG дедупликация, отдельный курсор не добавлен |

Один regression теперь выполняет частичный replay → настоящий маленький PG dump
→ удаление вымышленной схемы → pg_restore → продолжение. Проверяются correction,
rejected, unknown/not-sent, исходные outcome bytes и отсутствие повторных эффектов.
Это проверка логического checkpoint на вымышленной БД; физический backup реального
корпуса ещё не получен и не проверен.

## Необходимый дефицит до написания оставшегося caller

От baseline 250 остаётся 18 строк. Недостающий caller нельзя честно вместить в них:

| Недостающий конкретный компонент | Оценка дополнительных строк | Что переиспользуется |
|---|---:|---|
| Подготовка локального object store из gzip и закрытой metadata, выбор исторического SQL | 35–50 | SnapshotInput, RecoveryJournal, kaggle_schema_migrate |
| Stage0/native PG/BGE bootstrap и genesis SQL11 | 45–65 | kaggle_stage0, kaggle_native_bundle, OwnedPostgres; старый initialize_database с revisions |
| Локальный historical resolver и materializer callbacks | 25–35 | current_registry, проверенный legacy GEO handler, historical_materializer |
| Физический backup, pg_verifybackup, настоящий restore и конечный выход | 45–65 | kaggle_pg_snapshot_native и существующий owned PG lifecycle |
| Coverage и измерения длительности частей | 15–25 | applied_ops, recovery_state, сохранённые квитанции и стандартный monotonic |

Оценочный итог: 397–472 строки, дефицит baseline 147–222. Это обоснование до
превышения, а не уже добавленный код и не обещание точного размера. При реализации
сначала искать прямое переиспользование и вычитать лишнее; итоговые net lines
показывать в PR. Не скрывать notebook code вне production подсчёта.

Не добавлены scheduler, supervisor, WSS, provider retry, новый compiler, отдельная
очередь, хранилище векторов или модельный режим. Protected fingerprint-файлы,
chunk boundaries, precision contract и domain guards не менялись. Старые 16,2 часа
CPU — предупреждение по историческому профилю, не ETA и не прогноз GPU.
