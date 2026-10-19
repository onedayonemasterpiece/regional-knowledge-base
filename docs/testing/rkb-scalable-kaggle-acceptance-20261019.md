# Приёмка новой RKB: PostgreSQL, WSS, stories и пользовательские каналы

Дата: 2026-10-19. **NOT RUN**: план проверок будущего внедрения, не новый runtime report.

Самодостаточный [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) содержит тот же нормативный набор gates. Этот файл предназначен для отметок конкретных запусков с exact SHA/config/corpus и evidence. Условия отличаются от исходного SQLite/Supabase/E5; старые PASS не переносить автоматически.

## 14. Приёмка: обязательные численные gates и конкретные отказы

Все новые проверки в этом файле — **NOT RUN** на момент документационного изменения. Исторические результаты из старых отчётов не относятся автоматически к PostgreSQL/WSS/new story pipeline.

### 14.1. Protected product gates

Девять требований из `.devcoveer/requirements.json` сохраняются. Исторический ID CORE-SUPABASE-CAPACITY оставлен для стабильных ссылок; его новый смысл — measured storage headroom, а не обязательность Supabase.

| Gate | Критерий |
|---|---|
| Масштаб | Не менее 100 book-equivalent sources и 100 000 active chunks либо больший реальный corpus; benchmark готовности, не продуктовый лимит. |
| Ёмкость | Steady ≤80% каждого применимого storage budget; peak крупнейшего reimport, snapshot/restore и cutover ≤90%. Учитывать heap/vectors/indexes/TOAST/WAL/temp/system и overlap, отдельные control/recovery/cache budgets. |
| Dense quality | Не менее 30 held-out source-grounded fact families. BGE-only Hit@5 ≥85%, Hit@10 ≥90%; критический RU-query→DE-source slice Hit@10 ≥85%. |
| Ablations | BGE-only, lexical-only и configured fusion отдельно; dense не prefilter через lexical. E5-only не нужен целевому решению. |
| Warm backend | Ranked-candidate retrieval p95 ≤1000 ms на representative corpus/concurrency. |
| Public MCP | p95 ≤1500 ms; hard evidence interaction deadline 2000 ms. Не маскировать долгую очередь 10-секундным budget или исключением ошибок. |
| Chunk/source | Semantic passages около 256 BGE tokens; exact augmented input вместе с prefix ≤512, без silent truncation; all printed book material/provenance сохранены. |
| Per-book | Natural/paraphrased и relevant cross-language evidence queries на точной новой active revision до accepted readiness. |
| Story | Малый blind source comparison + негативные/положительные случаи; facts/metadata сохранены при zero story, связность улучшена без выдумки. Квот/минимальной длины нет. |
| Каналы/queue | Реальный не-owner, actual provider identity, dialogue order/ACK/fairness, measured ETA/resizing, private scopes, current revocation при поздней доставке. |
| Надёжность | Проверенный notebook restore, durable exact results, snapshot+tail, WSS reconnect/fencing/duplicate/UNKNOWN outcomes и отдельно обозначенная защита DevCoveer disk loss. |

В latency отчёте показывать p50/p95, end-to-end и этапы queue/dispatch/wire/SQL/encode/render; cold/warm и cache-hit/miss отдельно, error/pending denominator не исчезает. Не выдавать retrieval Hit@K за accuracy готового исторического ответа. Для ANN сравнить с exact vector search на той же выборке; фильтрация tenant/source/revision после ANN может уменьшить recall, что требует bounded refill/настройки, а не «в корпусе ничего нет».

### 14.2. Минимальная матрица конкретных рисков

| ID | Сценарий | Ожидаемый наблюдаемый результат |
|---|---|---|
| DUR-1 | Crash после intake ACK, до model result | Request и полный нужный input доступны из новой сессии; нет просьбы переслать accepted input. |
| DUR-2 | Crash после сохранения точного model payload, до PG apply | Replay использует тот же payload/hash/UUID/time; новая inference не подменяет его. |
| DUR-3 | Crash после PG commit, до result ACK | Applied_ops и cursor возвращают duplicate с тем же outcome; нет второй Story/case/доставки. |
| DUR-4 | CAS conflict / unexpected SQL error | Детерминированный отказ сохранён и cursor идёт дальше; неизвестная ошибка не пропущена молча. |
| FEN-1 | Два writer epochs / поздний старый upload | Только действующая authority меняет pointer/delivery; старые уже accepted commands всё ещё replay. |
| WSS-1 | Idle/proxy disconnect при прогретой модели | Reconnect/RESUME того же процесса, без kernels_push и повторного model load. |
| WSS-2 | Drop до RESULT, после commit, до ACK | Bounded reconciliation IDs/hashes; повторный эффект отсутствует, hash mismatch явный. |
| WSS-3 | Старый и новый sockets одного run | Только один dispatch attachment; old generation fenced. |
| WSS-4 | Expired claim / heavy encode / cross-process enqueue | Stale claim не убивает warm worker; control/ping живы; очередь из другого процесса доставляется promptly. |
| REC-1 | Concurrent backup и записи | Restore имеет подтверждённый внутренний cursor и нужный WAL; replay непрерывен. |
| REC-2 | Corrupt snapshot / wrong version / несовместимый codec | Candidate не становится canonical; доступна прежняя verified baseline или явный controlled recovery. |
| REC-3 | Yandex candidate upload/verify fail | Старая baseline сохраняется, compaction учитывает её watermark и незавершённые consumers. |
| REC-4 | Потеря DevCoveer control disk | Восстановление соответствует согласованному RPO; нулевые потери не приписаны суточному backup. |
| IMP-1 | Fresh A→B, B→A и concurrent activation | Каждая revision активирована корректно, research frontier сохранён, позднее основание замечено по cursor. |
| IMP-2 | Retry stage/finalize/reextract/rename | Нет дубля book/findings/Story/Variant/case; corrected metadata не меняет corpus. |
| IMP-3 | Table/caption/footnote без эпизода | Facts, dates, observations, entities и exact evidence доступны, zero new story допустим. |
| STO-1 | Эпизод через windows/страницы | Сохранённое continuation приводит к одному связному досье, не набору chunk stories. |
| STO-2 | One-sentence episode / everyday practice | Нет ложного отказа из-за длины, отсутствия конфликта или точного дня. |
| STO-3 | Новый источник / contradiction / uncertain identity | Не выдуманы причинность и единый эпизод; case независим; exact approved variant не переписан. |
| STO-4 | Spoken rendering / exact source | Нет оборванного слова/оговорки и повторной attribution wall; source quote/offsets не изменены. |
| STO-5 | Инструкции или команды внутри source text | Источник сохранён как данные/evidence; его текст не меняет actor, grants, workflow или инструменты агента. |
| MET-1 | Calendar/units/people boundary | Julian/Gregorian precision, exact decimals, period/method, тёзки и historical aliases не нормализованы с потерей смысла. |
| CAS-1 | Третий источник/merge/новый аргумент после вывода | Stable subject case, aliases и evidence history; новое основание явно вне старого based_on. |
| CAS-2 | Модель через личное подключение / ошибочная identity | Нет ложного human support; attribution_review_required адресно, история сохранена. |
| MIG-1 | Layer export/import/lookup | Counts/IDs/CRS/geometry/transform/provenance и external owner refs проверены. |
| MIG-2 | Legacy anchor / другое издание / поздний delta | Нет ложного reanchor; layer writes accounted; book corpus не мигрирован. |
| MIG-3 | Rollback после target writes | Новые принятые вклады сохранены; whole old DB не затирает target tail. |
| SRC-1 | Public text search / stale reader / withdrawal | Все разрешённые active accepted book texts доступны, stale/revoked material не возвращается; min_data_seq соблюдён. |
| SRC-2 | Dense vs exact ANN / filtering | Recall и eligible top-K измерены; empty after filter не выдан за отсутствие evidence. |
| PRF-1 | PG corpus adapter / revoked pixel grant | Proof работает на target, unauthorized preview/full page не строится и не отправляется. |
| PRF-2 | Preview retained / late polygon / rerender | Hash/transform/generation совпадают; lost preview требует новый locator task. |
| PRF-3 | Mixed ready/pending/failed pages | Успехи возвращены по items, continuation сохранён, actual MIME верен. |
| ARC-1 | Oversize/stream break/FLOOD_WAIT/duplicate upload | Ресурсы ограничены, bytes/hash проверены, independent session leases соблюдены, receipt reconciled. |
| QUE-1 | Один пользователь с многими dialog и explicit IDs | Canonical-person fairness без обхода fair head. |
| QUE-2 | UNKNOWN delivery / новая реплика / cancel | Порядок своего диалога сохранён, другие люди работают, подтверждённые эффекты не удалены. |
| QUE-3 | Capacity shrink/grow / long import / ETA | Permits пересчитаны, search reserve есть, >10 s notice coalesced и final приоритетен. |
| ID-1 | Non-owner / forged actor / revoked grant / private draft | Server-side denial, нет owner fallback или доступа к чужому draft/history. |
| CHN-1 | Slack + личный MCP + PH/Mira | Одна identity/история; реальные request/result/hash ACK, pending и reconnect, evidence отображено. |
| CHN-2 | Physical phone / длительный voice / четыре пользователя | Реальный mixed-load отчёт; emulator pass отдельно, права/контекст не смешаны. |
| PUB-1 | First publish / failed update / late build | URL только после первого readback; last-good URL остаётся, stale build не заменяет новый. |
| PUB-2 | Revocation / public HTML/OG/assets | Fenced rebuild/tombstone и cache readback; никаких private drafts/pixels в экспорте. |
| VK-1 | Duplicate schedule / cancellation_unknown / reserve race | Один provider effect, slot не освобождён до проверенной отмены. |
| VK-2 | Partial comment pages / changed/deleted parent/policy | Нет ложной полноты, утраты допустимых ответов или автоматического удаления по stale policy. |
| NEED-1 | Independent import нужного сборника / PH task done | Research need связан после semantic check; imported/task done не объявлены fulfilled/спор решён. |

Это набор различающих сценариев, а не предписание создать по сервису/fixture на строку. Повторяющиеся механизмы проверяются одним meaningful тестом с несколькими условиями; существующее доказательство переиспользуется для неизменённого кода.

Тесты с реальными источниками по умолчанию используют isolated case/corpus fixture и один test_run_id. Не публиковать вымышленные экспертные мнения в live cases, subscriptions или VK. Если требуется canonical mutation, заранее baseline/allowed effects и узкая idempotent CAS compensation своих действий, сохраняющая поздние реальные вклады; rollback_conflict не лечится force overwrite. Component tests не требуют подделывать Slack event; actual user-sender E2E отдельно и без impersonation.


## Формат отчёта исполнения

Для каждой применимой группы: status PASS/FAIL/BLOCKED/NOT RUN, exact code/contract/schema/model/tokenizer revisions, corpus/source revisions, run timestamps, sample size, errors/pending denominator, latency percentiles, measured storage/peak и artifact/readback. Для story — source-grounded blinded pairs и замечания владельца; текст приватных источников в публичный GitHub отчёт не копировать.

Соблюдать минимальность: component invariants до реального provider E2E, переносимые evidence для неизменённых механизмов, только meaningful tests против конкретных рисков. Полный тест matrix не является обязательством построить новый test-service или запускать все внешние сценарии после каждой docs-правки.

[Исходная ПМИ разногласий](rkb-disagreement-pmi-20261018.md) сохраняет детальные пользовательские сценарии в части, совместимой с новым handoff. [Массовая готовность](../mass-ingestion-readiness.md) и protected manifest сохраняют численные пороги.
