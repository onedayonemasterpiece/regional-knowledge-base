# RKB/Kaggle: аудит тёплого PG/BGE runtime, 21.10.2026

Короткий (20 мин) аудит фактически работающей инфраструктуры против
[постановки](../prompts/rkb-scalable-kaggle-implementation-20261019.md) §0, §5.4, §6, §14.5
и [owner alignment](../designs/rkb-owner-alignment-20261021.md). Это не перепроектирование: здесь
только наблюдения с доказательствами, одно контрольное исправление и следующий шаг.

## 1. Зафиксированное состояние (22:11–22:17 UTC)

| Что | Факт |
|---|---|
| `origin/main` | `f2c6ad0` (merge PR #206) |
| Работающий controller | один, PID 1360, `releases/f2c6ad0…/source`, запущен 22:04:25 после **перезагрузки хоста** (boot 22:04:05) |
| MCP-сервер | `regional-knowledge-base.service`, тот же venv, `RKB_RELEASE_SHA=f2c6ad0` |
| Target config | `kaggle-v4/verified-restore-<release12>-<run8 v103>/target.json`, Dataset checkpoint **v1**, `writer_generation` от run v103 |
| PR #207 | OPEN, head `dc9d155`, CI 2/2 SUCCESS; **в работающий релиз не входит** |
| Dirty WIP | `projects/regional-knowledge-base` (ветка `chatgpt/two-book-vector-stress-20261006`, 18 изменённых файлов) — чужой, не трогал. Worktree PR #207 чистый |
| Текущий run | notebook **v104**, `status=starting`, `launch_state=dispatched`, HELLO/WSS gen 1 в 22:05:03, heartbeat живой, `ready_at=NULL` |
| Writer/restore binding | `rkb_writer_control.run_id` = run v103 (retired), `rkb_writer_restore_control.identity` = [run, nonce, incarnation] v103, `state=restoring` |
| Что видно в журнале | MCP-сервер каждые 5 с с 22:05:01: `target_source_routing state=closed code=restore_current_attachment_unavailable` |
| Dev | 3.3 GiB RAM, свободно 152 MiB, swap 2.1/4 GiB. PG на Dev не запускался |

Таймлайн notebook-версий за ~2 ч 40 мин (из `runs`):

| v | start | READY | последний heartbeat / жизнь |
|---|---|---|---|
| 97 | 19:28 | +361 s | 15 мин |
| 98 | 19:57 | +273 s | 5 мин |
| 99 | 20:20 | +254 s | 19 мин |
| 100 | 20:41 | — | 0 мин |
| 101 | 20:44 | — | 8 мин |
| 102 | 21:14 | +225 s | 26 мин; GEO1–20; **отозван** `revoked_run` 21:41:38 |
| 103 | 21:49 | — | 3 мин; restore fail → retired |
| 104 | 22:04 | — | жив, restore закрыт по построению (см. P2) |

8 cold starts, 4 READY, ни один run не пережил следующего изменения кода или конфига. Каждый cold
start по target заново тянет `stage0.install_bytes=1 712 050 038` (59 downloads), PG restore и
BGE load. Под каждую пару (релиз, run) вручную создан отдельный `target.json` (7 каталогов
`verified-restore-<release>-<run>` с 19:57 до 21:49).

## 2. Ответ на главный вопрос: почему нет устойчивого тёплого PG/BGE

Коротко: тёплый run технически поднимается (READY за 4–6 мин, reconnect и API-only rollout Dev
работают). Но в main нет пути применить **native SQL/handler** внутри живого run, а restore/writer
identity **привязана к одному конкретному run**. Поэтому правка native-части означает revoke →
новый notebook → полный cold restore, а любая потеря run (reboot, retire) оставляет следующий run
без возможности восстановиться. Третья причина, обнаруженная во втором проходе (§7): даже живой
READY run отдаёт чтения нестабильно, потому что их доступность завязана на 5-секундный цикл
пере-верификации с Dev.

> Поправка 22:30 по [follow-up dot](../prompts/claude-runtime-followup-20261021.md): API-only
> rollout PR206 уже прошёл на живом v102 (та же incarnation, generation 3, без перезагрузки PG и
> модели; квитанция оператора). v102 отозвали из-за дефекта native `dataset_verify` workspace, а не
> только из-за релиза Dev. Формулировка «никакое изменение нельзя применить в живом контуре» из
> первой версии была чрезмерной.

### P1. Нет in-run обновления SQL/handler (§5.4, первые две строки таблицы; DBG-1)

- Поправка (третий проход): узкий путь **горячей замены read handler** в main есть. Это подфазы
  `recovery_phase` `read_handler_status/stage/activate/rollback`
  (`kaggle_recovery_phases.py:19-20`, `kaggle_warm_maintenance.py`, `kaggle_warm_read.py`). В
  первой версии я искал только по видам DISPATCH и его пропустил. Живого **SQL migration apply**
  в живом run нет. Живого прогона handler-пути (DBG-1) в журналах тоже не видно.
- Следствие на практике: v102 (READY, GEO1–20 выполнены) отозван в 21:41:38. Причина —
  native-дефект `dataset_verify`, который нельзя было исправить в живом run. По журналу: 30
  закрытий чтений с `dataset_native_snapshot_readback_changed` за окно READY, см. §7 O2. Исправление
  native-кода без нового notebook и есть отсутствующий путь (§5.4, методика п.3, DBG-1).
- DBG-1 остаётся **NOT RUN**.

### P2. Restore/writer identity привязана к одному run и не переносится на successor (§5.4 «Идентичность», §6.3)

- `target.json` содержит `database.writer_generation`; `rkb_writer_control`/`rkb_writer_restore_control`
  хранят `run_id + launch_nonce + runtime_incarnation` одного run
  (`queue_writer.py:138-158`, `kaggle_restore_startup.py:100-115`).
- После retire v103 и reboot хоста pending startup demand создал новый run (`bge_queue.py:_ensure`),
  и controller **запушил v104 с target, чей writer принадлежит retired run v103**. Restore для v104
  закрыт по построению (`restore_current_attachment_unavailable` каждые 5 с), но Kaggle-сессия,
  install 1.7 GB и CPU-квота расходуются.
- `_expire` не переводит такой run в expired, пока provider outcome не доказан
  (`bge_queue.py:187`). Controller не может получить proof (`remote_compute_terminal_proof:
  unavailable`). Итог: слот занят до ручного retire. Это и есть «v103 retired вручную».
- Чего нет: операции «перепривязать проверенный restore plan к HELLO-verified successor того же
  slug» внутри существующего writer fence. Сейчас её заменяют ручной минтинг нового `target.json`
  и рестарт сервисов.

### P3. Каждый успешный push логируется как UNKNOWN (гипотеза, не исправлял)

- Для всех запусков v100–v104: `bge_provider_boundary_failed boundary=save_kernel
  submission_state=unknown error_type=RuntimeError`, при этом версия затем находится
  reconciliation по source run_id. Источник — `kaggle_save_receipt_invalid`
  (`bge_kaggle_controller.py`, ветка `save_kernel`): `saved.error`, `saved.ref!=ref` или
  `version_number<1`.
- Гипотеза: `ApiSaveKernelResponse.ref` в установленном kaggle 2.2.4 / kagglesdk приходит не в форме
  `owner/slug`, например `/code/owner/slug`. Без живого provider-вызова не доказано. Последствие:
  ложный UNKNOWN в статусе и до 30 s лишней reconciliation на каждый запуск. Исправлять только после
  readback одного реального ответа без секретов, по §5.4 «не подменять identity придуманным
  значением».

### Что работает (не ломать)

- Reconnect: v102 дошёл до `connection_generation=3`; в 21:40:58 сработал
  `recovery_job_binding_restored … phase=source_history` (PR #206).
- Поведение fence: при неизвестном provider outcome новый push не делается, capacity удерживается
  (DBG-2/DBG-4 по смыслу соблюдены). P2 не надо лечить ослаблением этого fence.
- Startup-budget: `MAX_STARTUP_FAILURES=3` → `blocked`, бесконечного crash→push нет.

### Необходимые и искусственные cold restarts

| Причина | Нужен новый notebook? | Сейчас |
|---|---|---|
| Изменение SQL/function/совместимой schema | нет, live migration (§6.6) | новый notebook (P1) |
| Изменение stateless read handler | нет, narrow swap (§5.4) | новый notebook (P1) |
| Изменение controller/MCP-кода на Dev | нет: reconnect + API-only restart | run отзывался вместе с релизом (v102) |
| Новая Dataset version (Dataset2) | нет, PG switch в maintenance window (§6.6) | новый target + notebook |
| Потеря session, reboot Dev | Dev reboot → reconnect; Kaggle session gone → новый run с restore | successor не может взять restore plan (P2) |
| Torch/tokenizer/ABI, bootstrap/образ | да | да (корректно) |

## 3. Проверка путей из задания

| Путь | Результат аудита |
|---|---|
| launch → HELLO | работает (v104: attach + bootstrap_environment через 10 s после push) |
| restore → READY | работает только для run, под который вручную создан target (v97–99, v102); для successor закрыт (P2) |
| reconnect / API-only restart | reconnect подтверждён (gen 3). API-only rollout PR206 на живом v102 подтверждён операторской квитанцией (см. поправку в §2); сам я его не наблюдал |
| checkpoint / native readback / Saved | target на Dataset v1; `rkb_dataset_recovery_store.latest_json=NULL`; Dataset2 promotion pending (согласуется с вводной). Readback-путь из PR #207 в работающий релиз не входит. Глубже не проверял |
| совместимые SQL/handler обновления без потери PG/model | **не реализовано** (P1) |

## 4. Контрольное исправление (эта ветка)

`scripts/production/bge_kaggle_controller.py`: перед составлением notebook для target
`VERIFIED_INITIALIZED` controller требует, чтобы `rkb_writer_control` принадлежал **запускаемому**
run и `writer_generation` target'а. Иначе — известный `launch_preparation`/`not_sent`
(`preparation_step=verify_writer_binding`). Он учитывается в `MAX_STARTUP_FAILURES` и после трёх
попыток даёт явный `blocked`, без `kernels_push`.

- Устраняет ровно случай v104: successor demand после reboot/retire больше не сжигает
  Kaggle-сессию на невозможный restore.
- Не ослабляет fence, авторизацию и сохранность. Легитимный путь (target создан под этот run до
  push) проходит без изменений.
- Тест: `tests/test_bge_controller.py::test_restore_launch_refuses_writer_bound_to_another_run`.
- Проверки (локально, тот же venv): `test_bge_controller.py` и `test_kaggle_launch_envelope.py`
  (33 passed); `test_kaggle_target_runtime.py`, `test_kaggle_launch_config.py`,
  `test_kaggle_provider_terminal.py`, `test_bge_wss_worker.py` (76 passed).
- **Не задеплоено.** Работающий controller остаётся на f2c6ad0; на v104 исправление не влияет.

## 5. Что осталось

1. v104 занимает слот и не может восстановиться. Его нужно scoped-остановить или
   retire через существующий controller/operation surface. Решение за действующим исполнителем;
   в этом аудите не делалось (нет полномочий на stop).
2. PR #207 не слит. Без него restore v103 повторится и на правильно привязанном run.
3. P2 по существу: rebind restore plan на HELLO-verified successor под тем же writer fence вместо
   ручного `target.json` на каждый run.
4. P1: минимальный maintenance DISPATCH для exact SQL migration (§6.6) и stateless handler swap
   (§5.4) с readback loaded hash и `model_load_count`. Затем DBG-1.
5. P3: один redacted readback `ApiSaveKernelResponse` и нормализация сравнения `ref`.
6. Dataset2: native verify и promotion (pending по вводной, в аудите не проверялось глубже).

7. §7 O1–O4: развязать доступность чтений с фоновым обслуживанием и убрать durable-job'ы у
   read-only наблюдений. Без этого даже тёплый run отдаёт чтения примерно 2/3 времени.

## 6. Один следующий шаг до работающего сервиса

Слить PR #207 и этот guard в main, retire v104 через controller, создать **один** target под
следующий run на Dataset v1 и после READY **не отзывать этот run**. Перед этим или вместе с этим
сделать O1+O2 из §7: сбой promotion или продления не должен закрывать чтения. Иначе тёплый run
снова будет выглядеть «сломанным» и его отзовут. Последующие правки SQL/handler держать в очереди
под P1 (live apply), а не выпускать новыми notebook-версиями. Это даст первый run, который живёт
дольше одной правки, и честную метрику edit→apply→useful из §14.5.

## 7. Переусложнения и избыточные операции (второй проход, 22:26–22:40 UTC)

Критерий отбора: операция повторяется часто, добавляет задержку или новую точку отказа и при этом
не защищает ничего сверх того, что уже защищено fence/hash/версией. Проверки сохранности и
авторизации сюда не относятся. Всё ниже измерено по журналу MCP-сервера и копии `queue.sqlite`
(read-only), а не выведено только из кода.

### O1. Готовые чтения зависят от 5-секундного цикла пере-верификации с Dev (P0, latency + fragility)

- Цикл `kaggle_service_startup.py:_maintain` вызывает `refresh()` каждые `refresh_seconds=5`
  (`:66`). Каждый тик последовательно: readback writer publication → `restorer.refresh()` →
  `current_binding` (SQLite) → `writer.ensure_current(journal)` → `history.extend()`
  (WSS open → страницы по 16 строк → close, плюс чтение checkpoint и prefetch remote outcomes из
  журнала) → `dataset_checkpoints.refresh()` → GEO gate/installer (`:337-396`).
- GEO/source чтения разрешены, только пока `_Coverage.current()`, а это deadline
  `coverage_seconds=30` (`source_history_verifier.py:58-62`, `geo_read_gate.py:74-78`).
- **Любое** исключение в любом шаге: `self.coverage=None; self.gate.invalidate()` (`:400-401`).
  Следующий тик идёт уже в `history.restore()` (mode `full`, полная проверка всего applied
  prefix), а не в `extend` (`:366-369`). Неудача делает следующую попытку тяжелее, то есть
  получается положительная обратная связь.
- `renew/extend` сначала **сбрасывает** coverage и только потом идёт в Kaggle
  (`source_history_wss.py:183-185`). Даже успешное продление открывает окно закрытых чтений.
- Измерено на живом v102 в окне READY 21:18–21:41: `state=closed` 3–7 раз в минуту, всего 97
  тиков примерно из 276. Причины: `TimeoutError` 53, `dataset_native_snapshot_readback_changed`
  30, `SpoolConflict` 11, `StaleWriter` 1, `DatasetPublicationError` 1.
- Это противоречит контракту репозитория: «Ready reads do not wait for batch fill, fsync…»,
  «Read-only search does not wait for a cloud journal write» (AGENTS.md).
- Упрощение без потери гарантий: проверить native applied prefix один раз при attach/restore и
  после собственных apply (результат apply уже несёт новый cursor). Gate чтения проверяет локально
  monotonic cursor + binding/generation fence. Продление делать по событию (reconnect, смена
  generation, write), а не по таймеру. Старую coverage заменять только при успехе.

### O2. Promotion Dataset-checkpoint выполняется внутри цикла чтений (P0, fragility)

- `dataset_checkpoints.refresh()` вызывается в том же `refresh()` (`kaggle_service_startup.py:372-374`).
  При pending версии каждый тик делает `dataset_verify` через WSS и `store.promote()`. Несовпадение
  readback бросает `JournalConflict` (`dataset_recovery_store.py:104`), и это **закрывает все
  чтения**.
- Измерено: 30 закрытий с этой причиной за окно READY v102 и 30 job'ов `dataset_verify`. По
  follow-up именно этот дефект привёл к отзыву v102.
- Упрощение: promotion — отдельная maintenance-задача со своим статусом `checkpoint_pending`.
  Её сбой оставляет чтения на текущей проверенной базе. Fence на pointer CAS и readback-проверка
  остаются как есть.

### O3. Каждое read-only наблюдение — durable job с UNKNOWN-семантикой (P1, latency + fragility)

- Шаги source_history (open/page/close) и `dataset_verify` создаются как durable `jobs` в
  `queue.sqlite`: insert → claim → result, у каждого свои транзакции. `operation_key` включает
  случайный nonce (`source_history_wss.py:168, 244`), поэтому каждая попытка — новый job без
  дедупликации.
- На run одновременно может быть claimed **один** durable job (`bge_queue.py:791`). Наблюдения
  ждут за любым claimed job того же run, включая импорт.
- Верификация на Dev ограничена 10 с после открытия (`phase_timeout`, `timeout_seconds=10`, `source_history_wss.py:188, 242`). При timeout ожидание отменяется, а job нет («a timeout preserves an unknown
  operation», `kaggle_recovery_dispatch.py:5`). У v102 `open` от создания до done в среднем 156 с
  (111 штук, суммарно 17 345 с), при 48 `close`. Kaggle исполнял open уже после того, как Dev
  сдался, а около 63 native-сессий никто не закрыл.
- Упрощение: read-only наблюдения у состояния без side effects проводить как эфемерный запрос
  (как QUERY: deadline, generation, без durable row и UNKNOWN). Ещё лучше один вызов, который
  возвращает весь диапазон receipts: ≤2048 строк × 11 полей влезают в recovery-frame 4 MiB
  (`bge_channel.py:34`), вместо open + N страниц по 16 + close. Durable/UNKNOWN оставить для
  мутаций (`prepare`, `capture`, apply).

### O4. Вечные pending recovery job'ы мёртвых run'ов (P1, fragility + лишние cold starts)

- В очереди 134 `recovery_phase` в состоянии `pending`. Они привязаны к шести уже мёртвым run'ам;
  самый старый от 20.10 21:11 UTC. 131 из них — read-only `source_history` (128 open, 1 page, 1 close, 1 abort), 3 — мутирующие `prepare`/`capture`.
  Исполнить их нельзя: actor `maintenance:<run>`, identity с nonce/incarnation мёртвого run
  (`bge_queue.py:749`, `kaggle_recovery_dispatch.py:45-66`). Очистки нет.
- Последствия:
  - (а) `maintenance()` считает **любой** незавершённый job спросом и вызывает `_ensure()`, который
    при пустом `current_run` создаёт новый run (`bge_queue.py:931`). Вместе с P2 это объясняет
    автоматический push v104 после reboot.
  - (б) Они занимают 134 из 256 мест общего admission cap (`bge_queue.py:309, 333`). Ещё около 120
    таких сирот, и новые document/source job'ы получат `BGE queue full`.
- Упрощение: read-only фазы run'а, который retired или terminal, закрывать как `abandoned`, не
  исполнять и не считать спросом или ёмкостью. Мутирующие фазы остаются UNKNOWN, но тоже не
  создают startup demand сами по себе. После O3 класс сирот исчезает.

### O5. Повторяющиеся тяжёлые операции cold start (P2, latency)

- Каждый cold start по target тянет 59 артефактов, `stage0.install_bytes=1 712 050 038`. За
  вечер было 8 таких стартов, порядка 13 ГБ скачанного заново. Плюс отдельный `target.json` на
  каждую пару (релиз, run), см. P2.
- Проверить: упаковать wheels/native bundle в одну Kaggle Dataset/Model version на каждый
  `dependency_sha256` и монтировать, а не скачивать. Это меняется только при изменении lock, по
  §5.4 «dependencies готовить один раз по lock и fingerprint». До измерения экономию не заявлять.

### O6. Шум вместо сигнала (P2, диагностика)

- 14 087 из 15 170 строк журнала MCP за 21.10 (93 %) — `target_source_routing state=closed` раз в
  5 с. `state=ready` пишется на INFO и в этом журнале не виден. Реальные переходы (attach, revoke,
  restore fail) тонут.
- Упрощение: логировать только смену состояния и причины с счётчиком повторов, как требует
  §5.4 («краткая projection»). Каждый успешный push тоже пишется как UNKNOWN (P3), что
  добавляет ложных сигналов.

### O7. Одна сущность под несколькими идентификаторами (класс ошибок, P2)

- Ложные несовпадения возникают там, где один и тот же неизменяемый объект сравнивается по разным
  представлениям: `saved.ref` vs `owner/slug` (P3), PUT `gcs-generation:<N>` vs GET ETag
  (замечание Кварца B к PR207), source run_id vs provider version label. Каждая такая пара даёт
  ложный UNKNOWN или conflict при правильных данных.
- Правило: на каждой границе одна каноническая identity (exact version + SHA/size), нормализуемая в
  одном adapter. Сравнивать её, а не сырые строки провайдера.

### O8. Дублированная логика верификации (P3, стоимость изменений)

- `NativeSourceHistoryVerifier` (671 строка, 68 веток `SpoolConflict`) и
  `WssSourceHistoryVerifier._verify_remote` (353 строки, 41 ветка) реализуют одну проверку двумя
  копиями. Любое исправление семантики надо делать дважды. Используется ли базовый путь в
  production, я не проверял.
- Масштаб для ориентира: `src/regional_knowledge` — 53 200 строк в 195 модулях, из них `kaggle_*` —
  17 619 строк в 78 модулях.

### Что не трогать

Fence на side effects и pointer CAS, exact version + SHA/size, writer generation, UNKNOWN для
мутаций и provider effects, единственный controller, авторизация чтений. Предложения выше убирают
**повторение** проверок и связывание независимых задач, а не сами проверки.

### Порядок по эффекту на единицу работы

1. O2 и часть O1: перехватывать сбои фоновых шагов (promotion, GEO installer, snapshot) внутри
   своих задач, не обнулять coverage. Не сбрасывать coverage до успешного продления; после
   транзиентного timeout продолжать `extend`, а не `full`. Небольшая правка
   `kaggle_service_startup.refresh` и `source_history_wss._verify_remote` с фикстурными тестами.
2. O4: не считать сирот спросом и ёмкостью; abandon read-only фаз у terminal/retired run.
3. O3: однократное эфемерное наблюдение receipts вместо open/page/close durable job'ов.
4. O1 полностью: продление по событию вместо таймера 5 с; O6 вместе с ним.
5. O5, O7, O8 — после того, как тёплый run живёт стабильно.

### Исправлено в этой ветке (третий проход, 22:33–23:00 UTC)

| Пункт | Изменение | Что сохранено |
|---|---|---|
| O2 | `SourceDatasetCheckpoints.refresh`: сбой capture/`dataset_verify`/`promote` **до** перемещения pointer превращается в `CheckpointPending`. `kaggle_service_startup.refresh` логирует `target_dataset_checkpoint state=pending` и оставляет чтения на текущей проверенной базе | Readback-проверка, pointer CAS, «не публиковать v3 после потерянного readback». Ошибки после promotion (settle в spool) по-прежнему закрывают чтения |
| O1 (часть) | `WssSourceHistoryVerifier._verify_remote`: при `TimeoutError` или `observation unavailable` в режиме delta/renew возвращается прежний **неистёкший** proof. Условия: фаза фиксации не начата, spool-версия не менялась, binding тот же | Proof не продлевается и истекает по своему deadline. Любое противоречие, режим `full`, начатая запись или смена версии закрывают чтения как раньше. Write guard `_native_history_unsettled` не тронут |
| O4 | `bge_queue.LIVE_JOBS`: `recovery_phase` run'а в статусе `expired/failed` не считается startup demand, admission cap 256, `queue_depth` и `outstanding` | Строки не удаляются и не меняются, остаются evidence. Мутирующие фазы живого run учитываются как раньше |

Проверки:
- новые тесты `test_pending_dataset_checkpoint_keeps_verified_reads_open`,
  `test_renewal_without_new_evidence_keeps_unexpired_proof_only`,
  `test_dead_run_recovery_phases_are_not_startup_demand_or_capacity`; обновлён
  `test_kaggle_dataset_checkpoint` (ожидает `CheckpointPending` с причиной `TimeoutError`);
- полный набор: **2276 passed, 133 skipped**. В первом прогоне один раз упал
  `test_bge_runtime_readiness.py::test_model_only_ready_does_not_promote_target_channel`
  (WebSocket/TestClient). Отдельно он проходит и на ветке, и на main, файл трижды зелёный,
  повторный полный прогон зелёный. Флейк по времени на загруженном хосте. Main полностью:
  2272 passed;
- O4 на read-only копии живой очереди: незавершённых job'ов, считающихся спросом, было 134,
  стало 0.

Не сделано: O1 целиком (продление по событию вместо таймера, без 5-секундного цикла), O3
(эфемерное наблюдение receipts одним вызовом), O5–O8. Ничего не задеплоено, live-контур не
трогался. Эффект O1/O2 на долю закрытых тиков надо измерить на следующем тёплом run, до этого
экономию не заявлять.

Срез на 22:26 UTC: v104 всё ещё `starting/dispatched`, heartbeat не обновлялся 389 с, `ready_at`
пуст, слот занят. Процессы те же (controller PID 1360 на f2c6ad0, второго controller нет).
