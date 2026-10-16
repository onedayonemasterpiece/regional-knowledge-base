# Журнал испытаний и доработок

Связанная [ПМИ](testing.md). Журнал начат 2026-10-16; ниже отдельно отмечены исторические свидетельства, свежие наблюдения и ещё не выполненные проверки. Публикуются только безопасные агрегаты: private corpus/quotes/raw traces остаются в закрытом хранилище.

## Известные свидетельства (не новый прогон)

| Запись | Основание/среда | Результат и ограничение |
| --- | --- | --- |
| HIST-20261008-DENSE | [Датированный production stress](reports/expanded-retrieval-stress-20261008.md), deployed SHA `50ffeb698315eb84f594ae970a9db51c75eb3355`; Brünneck rev 2 / Gause rev 4 | Исторический FAIL: held-out original BGE FP32 Hit@5 25/32 (78.13%), Hit@10 27/32 (84.38%). 32 вопроса относятся к 16 held-out families; не доказан минимум 30 held-out families. Не текущий повторный замер. |
| HIST-20261008-ENCODER | Тот же отчёт, local INT8, concurrency 2 | Прервано 128/384, 7 HTTP errors + 1 connect timeout. INCONCLUSIVE для полной quality sample; реальные availability failures. Partial p95 1066.74 ms не является успешной end-to-end MCP приёмкой. |
| HIST-20261008-EXPERIMENT | Тот же отчёт, frozen experimental t256 | 28/32 Hit@5, 29/32 Hit@10; не активный production corpus. Candidate+integrity p95 148.09 ms на 384 frozen-vector запросах исключает query encoding. Эти числа не заменяют production gates. |
| HIST-20261008-CAPACITY | Тот же отчёт и [readiness](mass-ingestion-readiness.md) | В тот момент DB 75806387 bytes; planning quota 500 MB, обычный vector(1024)+HNSW. Synthetic compact 100k projection ≈345/352 MB не доказывает production migration или актуальную квоту. |
| DOC-20261015-120 | [Merged PR #120](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/120), merge SHA `aaac18305619d3bd8ea32a2b215eddade89ddcdc` | Синхронизация 8 документов; PR явно не запускал runtime/retrieval acceptance. Это документационный checkpoint, не продуктовый PASS. |
| OBS-20261016-INDEX | Read-only MCP `indexing_status`, 2026-10-16 18:50:30 UTC | Наблюдение: active_chunks=2434, bge_ready=2434, bge_missing=0, e5_ready=553, e5_missing=1881; indexing_state=ready, indexing_owner_state=ready, bge_query_state=ready, bge_worker_state=stopped; effective mode=bge, required spaces=[bge]. Это snapshot одной доступной области, без замера retrieval quality/latency и без подтверждения deployed SHA. `stopped` записано как возвращено, причина не установлена. |

Последний найденный датированный production quality gate остаётся FAIL из отчёта 08.10. Нового passing production acceptance эти записи не устанавливают. Текущие ready vector counts и docs merge не закрывают дефект качества.

## Очередь следующего согласованного прогона

| Cases | Статус | Следующее действие / открытое условие |
| --- | --- | --- |
| PRE-01 | NOT RUN (полная проверка) | Проверить deployed SHA всех трёх сервисов и реальные профили/schema; snapshot INDEX выше не покрывает весь case. |
| IMP-01…06 | NOT RUN | Согласовать bounded test source/стенд/лимиты, выполнить импорт с текстом/иллюстрациями/примечаниями, проверить инкрементальные истории/entities и activation отдельно. |
| SRC-01…03 | NOT RUN | Зафиксировать current revisions и private fixture ≥30 held-out families, выполнить ablations, paraphrases/language slices/negatives без маскировки dense fallback. |
| EXT-01…03 | NOT RUN | Выполнить model-led extraction/reconciliation, проверить собственные цитаты, replay/lease и семантику на размеченной выборке. |
| SEC-01 | NOT RUN | Подготовить разрешённые тестовые principals и negative fixtures без раскрытия production private data. |
| LAT-01 / CAP-01 | NOT RUN | Отдельное согласование нагрузки; честный end-to-end timing и актуальная measured quota. |

## Шаблон записи прогона

Копировать блок для каждого запуска; у каждого case должна быть отдельная строка фактического результата. Не менять старую запись после исправления: добавить retest и связать их.

```text
Run ID / UTC start-end / исполнитель:
Цель и разрешённый scope; стенд; профиль MCP:
Deployed SHA MCP / indexer / BGE controller; tools schema hash:
Corpus fixture ID/hash; source revisions; active/pending states:
Models/tokenizers/vector/index/fusion config; actor scope (обезличенно):
Case ID / ожидаемый результат / фактический результат / статус:
Private artifact ID/hash; безопасный публичный отчёт (если есть):
Sample N / held-out families / language slices / ablations:
Latency boundary / cold-warm / concurrency / p50-p95-max:
Ошибки / timeouts / degraded ответы / число первых попыток и retries:
Defect ID / severity / PR или commit / ответственный / следующий шаг:
Связанный предыдущий run / retest run / итог gate:
```

Для privacy/quote/semantic дефекта публично описать тип ошибки без исходной цитаты. Незавершённый запуск сохраняет фактические числа и INCONCLUSIVE/BLOCKED, а не PASS. Неисполненные проверки остаются NOT RUN. Закрытие дефекта требует нового evidence, не только merged PR.

## RKB-20261016-2023

16.10.2026, 20:23–20:27 UTC. [Подробный отчёт](reports/rkb-live-acceptance-20261016.md). Read-only действующий MCP, новый импорт и изменения корпуса не выполнялись. Точный deployed SHA неизвестен, remote main2067f2b; полный PRE-01 INCONCLUSIVE.

- Уборка: подтверждено удаление 27 старых release/source, +130,6 МиБ. Используемые checkout и уникальные изменения сохранены.
- INDEX: 2434/2434 BGE, required=[bge], query ready; не свидетельство свежей активации.
- SRC-01 diagnostic: 6 семейств, 12 positive attempts, 11 ответов; Hit@5 10/11, вся программа 10/12. RU→DE 3/3 rank1. Один подтверждённый semantic miss, RKB-LIVE-02. Hit@10 не измерен (8 результатов).
- Истории: semantic → lexical_degraded, целевая история отсутствует top5 естественного вопроса, присутствует rank1 точного lexical: FAIL RKB-LIVE-01.
- LAT diagnostic: 11 positive success backend p50/p95 408,3/1871,4 мс; observed MCP wall3571/8364 мс. Cold/warm и глобальная concurrency не контролировались; формальный LAT-01 INCONCLUSIVE, RKB-LIVE-03.
- Два out-of-corpus negatives дают нерелевантных кандидатов, не ответы. Нельзя считать их доказательством.
- POI → entity/story PASS ограниченного чтения. Полная семантика свидетельств INCONCLUSIVE, RKB-LIVE-04. Новые EXT-01…03 NOT RUN.
- Четыре вызова в двух блоках отменены с user cancelled MCP tool call, без повторов; не отнесены к server errors.
- IMP-01…06, полный holdout/ablations/ACL/CAP NOT RUN. Итог не GREEN: следующий одиночный импорт возможен как диагностический, не как принятая эксплуатация. Исправления не выполнялись.

Старые записи выше сохраняют историческое значение. Текущий ограниченный прогон не закрывает полный gate из отчёта 08.10.

## RETEST-20261016-SEARCH-PREDEPLOY

**Тип:** новый bounded read-only live run + отдельная source/CI verification, не замена прежнего RUN из docs-ветки. **Исходный run:** `RKB-20261016-2023` (остался выше без перезаписи). **Полный безопасный отчёт:** [rkb-search-repair-predeploy-20261016.md](reports/rkb-search-repair-predeploy-20261016.md). **Исправление:** [PR #122](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/122).

| ID | Причина | Фактический результат до выпуска | Новый код / ретест | Статус |
| --- | --- | --- | --- | --- |
| STORY-SEM-01 | У реестра историй была только FTS ветка, обещанная векторная индексация не исполнялась | 2/2 вызовов `story_search(semantic)` вернули `lexical_degraded` | Версионный BGE-кэш из существующего outbox, ACL до ранжирования; синтетические тесты/CI PASS, deploy не выполнялся | **SOURCE PASS, LIVE FAIL** |
| BOOK-GOLD-01 | Соседняя исходная страница не расширяла выдачу без грамматического продолжения | Естественный gold отсутствует top-8 в 2/2 попыток BGE; полный правильный источник прочитан отдельно | Один ограниченный same-page contextual slot с сохранением top-2 и ACL; синтетические проверки PASS | **SOURCE PASS, LIVE FAIL**; Hit@5 не доказан |
| PROOF-OWN-01 | Короткий excerpt не доказывал все утверждения | Полный связный фрагмент о свайных основаниях и трудности строительства собора проверен в действующей редакции Гаузе | Исторический account отделён от независимого факта | **SOURCE PASS**, не mass-quality |
| PROOF-CROSS-01 | Источники в compact snapshot неполны; контекст Бруннека был не прочитан | Собственные Gause/Brünneck страницы прочитаны; обе книги присутствуют в авторитетном sources_page, свидетельствуют о разных деталях арбитража | Не выполнять авто-слияние/ложную независимость | **SOURCE PASS**, старый точный B3 **NOT REPLAYED** |
| LAT-01 | Разность backend/tool wall в старом N=11 | В новом N=5: backend p50/p95 429,1/1 032,1 мс, MCP/connector wall p50/p95 1 270/1 561 мс. Конкурентность 1, не cold-start benchmark | Транспорт не менялся; границы измерений разделены | **DIAGNOSTIC**, не новый latency gate PASS |
| UNIT-REF-01 | Статические unit override содержат ссылку на релиз, не подтверждённый среди существующих каталогов | `service_config` отклонён `target_denied: service is not registered`; штатный read-only user-systemd вернул `unavailable` | Никаких сервисов/конфигураций не меняли | **UNVERIFIED** до effective release readback |
| NEG-01 | Выдача кандидатов не равна ответу | Два контрфактических вопроса дали кандидатов, но подтверждённых ответов не предъявлено | Не засчитывать кандидатов за доказательства | **NO ANSWER PROVEN**, не ответный hallucination PASS |

Четыре прежних `user cancelled MCP tool call` остаются **CANCELLED, причина неизвестна**, не переименованы в отказ безопасности. Приватные UUID, оригинальные исходные тексты и трассы не опубликованы. Текущие книги/редакции, Supabase/SQLite, права доступа и незавершённый импорт KA10 не изменялись. **Post-deploy продуктовый retest — NOT RUN, mass-readiness — NOT RUN.** Для live PASS требуется отдельное разрешение на production deploy, точная аттестация работающего SHA и повторение естественных вопросов.


## RKB-20261016-POSTFIX — source search restored, story catch-up in progress

**Связанная запись:** RKB-20261016-2023 и RETEST-20261016-SEARCH-PREDEPLOY выше. **Источник новых результатов:** [пострелизная приёмка](reports/rkb-search-postdeploy-acceptance-20261016.md). **Точный served SHA:** `f4b08bc426ba9fee71886a4aae5c4772bfd420cb` (RKB MCP и indexing unit), подтверждён штатным PID/env readback и двойной health после SHA-pinned release deploy. Две отдельные консистентные SQLite резервные копии от 16.10 с integrity/FK PASS. Основная рабочая копия не переключалась; книг, graph unit и ACL не меняли.

| Дефект → причина → изменение → retest | Итог |
| --- | --- |
| STORY-SEM-01 → отсутствовала исполняемая story-vector branch → PR #122: BGE story outbox/revision/ACL search → живой `story_bge_partial`, обычный lexical не подменяет dense режим; story index выполняет backfill через текущий worker | **LIVE PARTIAL, NOT YET PASS** до полной готовности и исходного естественного запроса |
| BOOK-GOLD-01 → нужный абзац через один промежуточный чанк и длинная общая соседняя выдержка переоценивалась по числу совпадений → PR #123: две ближайшие позиции той же страницы, один protected context slot, length-normalized question cues → исходный дословный естественный вопрос нашёл gold Гаузе rank 8/8, без лексического fallback | **LIVE PASS@8, MISS@5**, общая dense quality не считается прошедшей |
| STORY-INDEX-LATENCY → optional provider archive иногда давал HTTP 400/Timeout и удерживал pass → PR #123: откладывать лишь ancillary archive/mirror пока есть текущий story outbox → логи подтверждают последовательные `stories_written=1`, `archive_deferred_for_story_index=true` | **PASS текущего catch-up**, причина отдельного OAuth 400 не исправлялась |
| SOURCE-REF → короткие excerpts и частичный compact snapshot → полное чтение собственных свидетельств и авторитетный `sources_page` → Гаузе rev4, Бруннек rev3, обе зависимости сохранены | **PASS provenance в проверенном объёме**, не source-truth consensus |
| LAT-01 → backend и MCP wall ранее смешивались → шесть backend-only измерений p50 489,8 мс, p95 1285,7 мс, другие boundaries отдельно | **INCONCLUSIVE formal LAT** (N=6, concurrency/host load not controlled) |
| NEG → отрицательные вопросы возвращают retrieval-кандидатов → source fetch top-1 не подтверждает выдуманные технологии | **No affirmative source proof**, не generation-level no-hallucinations PASS |

Текущая bounded выборка двух принятых книг: 6/6 Hit@8, 4/6 Hit@5; query mode BGE на всех; двусторонние source ACL/исторические права не ослаблялись. Это НЕ тот же независимый holdout и НЕ массовая программа. Четыре старые `user cancelled MCP tool call` остаются UNKNOWN cancellation и не стали «отказами безопасности». Полный story semantic retest ждёт индексации всех 68 карточек; внешний archive OAuth, необязательный E5 auto-restart и строгие требования к latency/scale не объявлены закрытыми.
