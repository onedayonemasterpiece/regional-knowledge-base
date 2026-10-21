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

Коротко: тёплый run технически поднимается (READY за 4–6 мин, reconnect работает), но **в main нет
пути менять что-либо внутри живого run**, а restore/writer identity **привязана к одному
конкретному run**. Поэтому каждая правка кода/SQL/конфига означает revoke → новый notebook → полный
cold restore, а любая потеря run (reboot, retire) оставляет следующий run без возможности
восстановиться.

### P1. Нет in-run обновления SQL/handler (§5.4, первые две строки таблицы; DBG-1)

- DISPATCH-виды в main: embed/query jobs, `source_command`, `recovery_phase`
  (`bge_channel.py:144-146`, `bge_kaggle_worker.py`). Видов вроде live SQL migration, stateless
  handler swap или maintenance bundle **нет**; `model_load_count` есть только в bootstrap readiness
  (`kaggle_bootstrap.py:251`).
- Следствие на практике: v102 (READY, GEO1–20 выполнены) был отозван в 21:41:38 ради релиза
  f2c6ad0 и нового restore-плана. Это **искусственный** cold restart: постановка прямо требует
  применить SQL/совместимый handler в том же run (§5.4, методика п.3, DBG-1).
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
| reconnect / API-only restart | reconnect подтверждён (gen 3). API-only restart при живом READY run в окне аудита не наблюдался: v102 был отозван до restart MCP в 21:49 |
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

## 6. Один следующий шаг до работающего сервиса

Слить PR #207 и этот guard в main, retire v104 через controller, создать **один** target под
следующий run на Dataset v1 и после READY **не отзывать этот run**. Все последующие правки
SQL/handler держать в очереди под P1 (live apply), а не выпускать новыми notebook-версиями. Это
даст первый run, который живёт дольше одной правки, и честную метрику edit→apply→useful из §14.5.
