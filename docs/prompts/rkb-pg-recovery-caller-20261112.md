# Вызов конечной части восстановления

Только после установленного разрешённого доступа. Скрипт не обращается к Kaggle,
не публикует Dataset и не выбирает identity или имена ресурсов:

```sh
python rkb_pg_recovery_notebook.py /kaggle/temp/recovery-part.json
```

Этот source-файл доставляется с immutable batch-кодом. Входной пакет содержит
закрытый journal export, allowlisted metadata, source archives, SQL и native
packages. Отдельный разрешённый mount публичных BGE/wheel assets содержит ровно
пути из prepared inventory. Entry копирует assets в private temp, использует
существующий cached Stage0, затем импортирует и вызывает execute_part.
Никаких model downloads или inference на Dev для подготовки не требуется.

Поля закрытого config (без credentials):

| Поле | Значение и источник |
|---|---|
| input_root, input_manifest_sha256 | Mount закрытого входа и SHA его финального manifest |
| assets_input_root | Проверенный mount закреплённых model/wheel bytes; не identity |
| code_root | Пустая папка под /kaggle/temp для batch archive |
| output_root | Папка под /kaggle/working только для pg.tar.gz, checkpoint.json, README и error-report |
| prepared_model | root строго /kaggle/temp/rkb-artifacts, entries{name,size,sha256,role}, max_total_bytes;61 public assets плюс2 native files |
| stage0 | Существующий offline-stage0-plan: cached-only downloads[], wheels/packages/install_bytes/total_seconds |
| native_manifest | name и sha256 exact native084 manifest; Ubuntu24.04/x86_64/glibc2.39 пока только ожидаемый ABI |
| database | OwnedDatabase.wire: NEW_EMPTY_GENERATION, original database_generation, writer_generation, cluster/socket под private temp, dbname, uid/gid, native_paths, ownership_evidence, schema_version11 |
| run_id | Привязка к сохраняемой identity именно этой части; задаётся до submit, сохраняется в output |
| storage, stream | Исходные сохранённые journal namespace, без переименования или нового accepted stream |
| seconds, save_seconds | Пилот1200/480; затем7200 и резерв по фактическому пилоту; время начинается до input verification/Stage0 |
| checkpoint | Для продолжения: archive и manifest из последнего фактически проверенного exact output; иначе поле отсутствует |

Фактические uid/gid/paths/ABI и доступный mount нельзя угадывать. Native paths
выбираются из VerifiedNativeBundle.pins; vector_sql соответствует реальной
версии native bundle (retained native084 содержит0.8.4). Локальная фикстура0.8.2
не доказывает работу этого Ubuntu bundle в ещё не проверенной среде.
CP313 wheels и pinned CPU torch2.11.0+cpu требуют совместимой Python/runtime.
Полный запуск с моделью пока NOT RUN, подходящий разрешённый mount ещё не установлен.

На входе из checkpoint сверяются compressed archive, pg_verifybackup и actual
restored prefix/coverage. Полный journal проверяется штатным RecoveryJournal;
уже применённый prefix идёт через существующие receipts без повторной inference.
Продолжение исходных NOT_SENT/SENT_UNKNOWN не выполняется.

checkpoint.json различает replay.visited counts, реальный coverage.applied_sequence,
next_sequence, accepted_last_sequence и input_scan_complete. Последнее означает
конец обхода входа, а не качество всего корпуса. В report есть counts по source IDs,
непустые Variant.body, prefix digest и реальные результаты restore. Encoder timing
относится к вызовам внутри materializer; warmup входит в model_seconds.
SQL timing измеряет execute(), а не всё время fetch/транзакций. Provider save и
terminal подтверждаются внешними receipts; compute_terminal внутри report=false.

Перед следующей частью Crew подтверждает предыдущий terminal и сохранённый
checkpoint; выбран перерыв60 минут без compute. Скрипт не спит между частями и
не запускает следующую часть сам. Публикация final private Dataset версии1 —
отдельное разрешённое действие после полного coverage; частичный checkpoint не
объявляется готовой БД. Новые имена выбираются после установления доступа согласно
последнему пожеланию владельца, старые slugs не являются default.

13 ноября: при отсутствии полного публичного mount entrypoint использует уже существующий Stage0Download для закреплённых публичных файлов. Закрытые native files копируются из input Dataset; веса идут из masterstar/bge-m3 version1, конфиги и wheels — по исходным публичным immutable URL с прежними SHA/размерами. Новых загрузчиков и смены версий нет.

Доставка Kaggle: все allowlisted входы и input-manifest.json передаются внутри одного несжатого tar с именем recovery-input.blob. Это сохраняет исходные .tar.gz от автоматической распаковки Dataset. Launcher извлекает blob в /kaggle/temp/recovery-input, затем исходный entrypoint проверяет manifest и SHA/size файлов. Dataset metadata находится вне blob; полный forensic SQLite и секреты не входят.
