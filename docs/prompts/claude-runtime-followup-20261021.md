# Передача Claude: RKB/Kaggle после аудита 21.10.2026

Дата: 2026-10-21, 22:24 UTC. Это отдельная координационная передача dot, **не новая исходная постановка и не её замена**. Владелец рассматривает временное продолжение инфраструктурных исправлений Claude вместо Шкипера. Объём live-операций и временной лимит сверить с прямым поручением владельца; сам документ не расширяет доступ и не разрешает обход отказов инструментов.

## 1. Исходные документы и разработанная схема отладки

Прочитать оригиналы, а не заменять их этим конспектом:

- [Единая исходная постановка RKB, актуализированная 21 октября](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md): особенно §0, §5.4, §6/6.6 и DBG-1 в §14.5.
- [Уточнения решений владельца](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/docs/designs/rkb-owner-alignment-20261021.md).
- [Существующая схема тёплой отладки — kaggle-warm-debugging/SKILL.md](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/SKILL.md).
- [RKB-профиль этой схемы](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/references/rkb-profile.md).
- В том же каталоге references: [migrations](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/references/migrations.md), [lifecycle](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/references/lifecycle.md), [transfers-and-cleanup](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/references/transfers-and-cleanup.md), [evidence](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/main/.agents/skills/kaggle-warm-debugging/references/evidence.md).
- [Ваш аудит и draft PR208](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/208).

Схема различает SQL migration, stateless read-handler swap, stateful executor replacement и несовместимый bootstrap. Не обещает, что модель переживёт замену владеющего ею процесса. Не вводить general remote shell, второй controller или новый оркестратор.

**Более позднее прямое уточнение владельца:** PostgreSQL на DevCoveer не запускать, даже disposable fixture. Dev уменьшен владельцем до 4 ГБ. Старое разрешение маленьких локальных PG fixtures в профиле этим перекрыто. GitHub CI не запрещён, но не подменяет требуемую работу с живым PG в Kaggle.

## 2. Проверенные ссылки и пределы знания

- Main при проверке: f2c6ad0ba9df283a08240ee31de5ae771c2820eb.
- [PR207](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/207): dc9d155e9787ef8954697b93bd521748b246f7b6, open/not merged при проверке; CI39683312500 SUCCESS подтверждён через GitHub.
- [PR208](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/208): 6705d2c99abb596bde47b7e01cb6882860645087, draft/open; guard предотвращает push с writer другого run, **но не реализует автоматический successor rebind**.
- v102 восстановил Dataset1/schema11/base7 и применил GEO1–20. Dataset2 опубликован на том же slug, но native verify/promotion и Saved оставались pending.
- v103 упал при restore; затем retired по сообщению оператора.
- v104 после reboot и несовместимый с ним restore binding — ваши свежие наблюдения из отчёта, dot самостоятельно live не перепроверял. Перед действием снова проверить exact run и текущие процессы.

### Поправка к хронологии аудита

API-only rollout PR206 уже работал на **живом v102**, same runtime incarnation, generation3, без PG/model reload: исходный source_history job aa09209e-0f1c-4858-aa33-5cef5f069653 DONE с attempt1, около40 retained jobs settled. Операторская квитанция: v102-api-only-maintenance-reconnect-verified.json.

[Сообщение оператора](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792618566037469).

Последующий отзыв v102 объяснялся immutable native dataset_verify workspace defect, а не только изменением Dev-релиза. Проблема отсутствующего native warm apply остаётся, но формулировка «никакое изменение нельзя применить внутри живого контура» чрезмерна. Сохранить уже работающий API-only путь.

## 3. Два точных замечания Кварца к PR207

Это read-only source review, не доказательство нового live failure.

### A. Legacy mount branch всё ещё принимает self-attested version

В kaggle_recovery_phases.py на dc9d155e ветка checkpoint + dataset_base ещё вызывает prepare_dataset_base с dataset_observation из того же WSS frame, без version-qualified ticket. Новый happy path этого не посылает, но смешанный rollout/старый frame может выбрать прежний путь.

SHA/size продолжают защищать содержание; принятие неправильных bytes не доказано. Проблема — недостоверное свидетельство версии. Выбрать одну поддерживаемую production семантику. Если legacy mount больше не нужен, отвергать legacy dataset_base/dataset_observation для Dataset restore до prepare; negative regression плюс сохранённый exact-ticket happy path. Не добавлять ещё один механизм fallback.

### B. Original PUT native_version и GET ETag могут быть разными идентификаторами

Реальный download adapter kaggle_recovery_phases.py:45–58 требует GET If-Match == blob.native_version. native_snapshot_transfer.py:163–185 при PUT без ETag сохраняет gcs-generation:<N>. Новый DatasetSnapshotTicketIssuer получает If-Match из GET ETag historical version. Эти строки могут различаться при правильном архиве; тогда recovery_download_ticket_blob_changed возникает до GET.

Это **условный source failure**, actual PUT receipt kind у Dataset1 Кварц не проверял. Новый тест PR207 подменяет download callback, поэтому не покрывает этот adapter.

Нужен synthetic regression через настоящий adapter с PUT generation и GET ETag; actual metadata сверить без секрета/архивных bytes на Dev. Сохранить exact version/file, SHA/size, operation/run/authority и HTTP If-Match; не удалять проверки без понимания их разных назначений.

[Полный финальный review Кварца](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792620700713499).

## 4. Предлагаемый порядок продолжения

1. Fresh HEAD/WIP/process/run inventory; исключить одновременное live-управление Шкипером и Claude.
2. Довести PR207/208 с замечаниями выше и минимальными regression checks. Не сливать по одному green CI без разбора границ.
3. Исправить автоматическое создание/перенос restore-плана на допустимого проверенного successor в existing writer lifecycle. Не использовать совпавший slug, TTL или disconnect как доказательство смерти старого владельца. Развести pre-push intent и post-HELLO identity, не создать цикл «push требует HELLO».
4. Controlled retirement только exact текущего run, если действительно необходимо и разрешено владельцем; сохранять UNKNOWN, spool, accepted tail и Dataset versions. Не отпускать reservation по одному Stop ACK.
5. Один корректно связанный запуск: exact historical Dataset restore → ordered accepted tail → retained published Dataset2 native verification → обычный promote/Saved. Не создавать version3 из-за lost ACK. Не переходить к SQL12/13/14 поверх неподтверждённого состояния.
6. Оставить READY run тёплым. Затем минимальный совместимый SQL/handler apply в существующем WSS/maintenance пути; exact hashes/readback, сохранение replay semantics. Доказать DBG-1: тот же run/incarnation, PG state, отсутствие повторного model load, новая версия handler/schema реально действует.
7. Несовместимые ABI/bootstrap/model-owning process изменения по-прежнему могут требовать controlled restart. Не заменять реальную совместимость лозунгом «никогда не перезапускать».

Сначала полезный рабочий контур, затем оптимизации. Гипотезу save receipt/ref normalization проверить на сохранённом либо следующем необходимом redacted response; не делать новый push только ради диагностики.

## 5. Параллельные границы и продуктовый критерий

- Кварц: пользователи/Slack ingress/verified canonical person/expert ACK.
- Риф: Story.body/basic Variant, расхождения/Case/operation_key/edit/continuation.
- dot: сохранённая интеграция native-book/Telegram. Frozen src_a1a3422215c34c564d2940a5 на base c95263fa1d37b0ad0a9464d658c4e5a031b89d60,63 changed files; все63 byte-identical исходному snapshot, но **UNREADY**, не выкладывать целиком.
- Book UNREADY: post-READY Telegram pending/ACK/generation fence; ordered SQL12→14 wiring;101code+59base=160/160 prepared-file cap до дополнительных wheels. Это не мешает отдельно чинить инфраструктуру.
- Не менять чужой dirty checkout, credentials/private locator, не создавать постоянный дополнительный доступ, не выдавать новый transport вместо существующего.
- Dataset slug неизменен: zigomaro/rkb-pg-checkpoints. Не создавать genesis, не удалять принятые хвосты.
- Цель пользователя: помощники проверяют свои части на устойчивом Kaggle PG без пересоздания notebook каждой итерацией; source CI и число PASS сами по себе не результат.
- Итог продолжения: exact commit/diff, реальные проверки, deployed/native identities раздельно, длительность сохранённого тёплого run, состояние Dataset2 и один оставшийся blocker. Если runtime операция не разрешена/инструмент отказал — фиксировать точно, не обходить другим executor.
