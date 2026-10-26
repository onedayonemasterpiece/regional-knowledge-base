# Численные данные пилота 26 октября 2026 года

[Отчёт](../../../reports/rkb-retrieval-strategy-pilot-20261026.md) · [ПМИ](../../rkb-retrieval-strategy-pmi-20261026.md) · [manifest](manifest.json)

Эти файлы получены из закрытого frozen trace. Они не содержат текстов книг, query strings, native IDs или opaque handles. Query-only банк опубликован отдельно; source IDs разрешаются заново в приватном manifest следующего опыта.

## Единицы и пустые значения

- Время в CSV выражено в миллисекундах. tool wall и backend time — разные часы.
- Пустая ячейка CSV и JSON null означают ненаблюдаемое/неприменимое значение, а не ноль и не false.
- True/False — логические оценки. N относится к фактическому числу наблюдений.
- Pxx/source labels — локальные аналитические метки, не native identifiers.
- family_id — единица оценки. Несколько языков и парафразов одного вопроса не являются независимыми families.
- Перед основным срезом применены frozen selected_families и core cutoff. Post-core source-filter и context diagnostics не добавляются к результату A4.
- 53 family×evidence judgments относятся к 49 уникальным passages: один passage может оцениваться для нескольких вопросов.

## Файлы

| Файл | Содержание |
|---|---|
| summary.json | Счётчики, времена, ограничения и проверка bindings |
| calls.csv | Все 156 попыток; execution отдельно от semantic quality |
| family_arms.csv | 40 строк F01–F08 × A0–A4 |
| latency_by_arm_query.csv | Реальные вызовы и интервалы по arm/query position |
| rank_comparison.csv | Изменения ordered top-3 относительно A0 |
| rrf60.csv | Offline fusion двух списков; это не runtime reranker |
| paired_native_A1_A0.csv | Парные ratios и deltas; ratio median не равен ratio of medians |
| top3_source_scope.csv | Совпадение с указанным источником, не историческая истинность текста |
| quality_by_family_arm.csv | Релевантность, supporting-set completeness, unknown при неполной разметке |
| reader_outcomes.csv | 8 Q480 + 8 full passage + 8 actual A4 answers |
| context_diagnostics.csv | Два post-core matched context cases |

## Как понимать качество

Partial hit требует хотя бы одного положительно оценённого in-scope candidate. Полная достаточность требует целиком найденный минимальный supporting set. Наличие одного члена нужной пары не считается полным успехом.

Q480/full проверены по строгому frozen gold и отдельно по task-essential sensitivity F05. Reader-declared sufficient — самооценка читателя; она не заменяет независимую оценку. Для A4 declared_sufficient здесь null, поскольку таблица берёт оценки фактических ответов, а решение stop/continue сохранено в исходном trace.

Полнота ответа по own packet и по cited subset разделена. Citation completeness измерена для фактических A4 answers; она не была отдельной endpoint-метрикой Q/F и остаётся null.

Известный sufficient supporting subset может подтвердить true даже при других unjudged candidates. Отсутствие оценки или native URL не превращается в false. При неизвестной полноте retrieval/identity helper сохраняет unknown.

Ни одна таблица не содержит user→answer, голосовой transport или Slack E2E latency. Empirical p95 — описание данного набора, а не подтверждение SLO.

## Повторное вычисление

Использовать [offline helper](../../../../scripts/benchmarks/rkb_cascade_pilot_summary.py) с приватными frozen trace, resolved question bank и independent evidence judgments. Полный пример CLI находится в ПМИ. Public CSV достаточны для проверки агрегатов, но не заменяют закрытый оригинальный evidence для повторной содержательной разметки.

Публикация этих случаев переводит их в dev/regression-набор. Новая приёмка использует отдельный held-out.

