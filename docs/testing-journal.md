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
