# RKB: восстановленные решения владельца — 21.10.2026

Статус: нормативные уточнения целевого контракта, не подтверждение реализации. Основание — [прямой ответ владельца](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792573398928879) и его поручение исправить исходный проект и связанные документы в main. Разбор старого/нового кода — [аудит](../reports/rkb-requirements-code-audit-20261021.md). Этот файл уточняет [единый handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), не создаёт второй проект или отдельный workflow.

## Приоритет и область

При конфликте с прежним target-текстом ниже действуют прямые уточнения владельца. Исторические отчёты не переписываются задним числом; их provider-доказательства относятся к указанной старой версии. Source-only, deployed, fixture и live accepted — разные статусы. Изменение документации не разрешает удаление действующих данных, запуск notebook или смену прав.

Цель — перенести уже работающие функции и оптимизации на PostgreSQL/pgvector+BGE в Kaggle, а не уменьшить продукт до текстового PDF-пилота. Поддержка иллюстраций, DjVu, model source reading, реальных Story bodies, источниковых связей, каталога, reprocess/rechunk и продолжения исследования остаётся в scope. Только GEO content переносится из старого корпуса; identities/grants/configuration не обнуляются.

## Обязательные границы

1. **Модель читает и осмысляет.** Если клиент уже даёт модели реальные страницы вложения, не запускать второй обязательный backend render/native extraction. `book_pages` сохраняется как помощь по запросу для отсутствующих пикселей/продолжения, а не как искусственное условие каждого stage. Не предполагать поддержку визуального PDF/DjVu у любого клиента по его названию. Native text может помогать, но не является авторитетом, с которым обязана совпадать транскрипция модели. Не добавлять backend OCR/VLM/LLM или обязательного второго судью.
2. **Один owner-only import.** Сериализовать фактическое исполнение start/resume/reprocess, а не только один endpoint. Второй конкурентный запуск не получает второй import lease; читать уже принятые источники можно. Это не разрешение удалять отдельно предусмотренные авторизованные редакционные/экспертные функции: они проверяются по собственному scope, не используются как обход import lease.
3. **Domain data и тяжёлая обработка — Kaggle.** PG staging/source graph, токенизация final input, BGE и установка document vectors находятся рядом с PG. Dev — OAuth/MCP, ресурсная оркестрация, короткий bounded control/replay spool и компактные receipts/refs. Такой spool не становится второй рабочей книжной БД; постоянные graph/semantic JSON и vector results там не нужны. Ни query-, ни document-векторы не делают круг через Dev.
4. **Основной persistence — versioned Kaggle Dataset.** Run восстанавливается из точной подтверждённой версии и сохраняет изменения обратно. Dataset — версии согласованных артефактов, не живой writable PGDATA. Legacy SQLite archive Dataset не доказывает target PG persistence. Нельзя silently использовать только S3 вместо выбранного Dataset.
5. **Yandex S3 — weekly independent backup.** Раз в неделю сохраняется копия подтверждённого состояния с датой/version. Это не новый обязательный online primary. Новый bucket не создаётся одним упоминанием в проекте. Не оставлять daily backup default в другом разделе.
6. **Telegram напрямую из Kaggle через Telethon, чтение и запись.** VibePublish не участвует в target archive path, Dev не проксирует оригиналы/загружаемые media. Не копировать весь Vibe framework в notebook; сохранить источник/иллюстрацию/locator и необходимые UNKNOWN/idempotency состояния. Отдельные уже согласованные внешние публикации вроде VK не становятся автоматически Telethon archive и не удаляются этой поправкой.
7. **Контроль Telegram-медиа — наличие доступного объекта по locator.** Проверить ожидаемый photo/document на нужном message/chat/topic. Не скачивать обратно для custom SHA comparison и не объявлять recompressed photo испорченным из-за другого hash. Code/snapshot integrity, входная source edition identity, exact accepted command и auth остаются отдельными требованиями. Проверка Telegram не ослабляет integrity восстановления PG.
8. **Photo по умолчанию; high-resolution document только при необходимости.** Решение опирается на фактическое разрешение source и доступной photo rendition выбранного pinned upload path. Не фиксировать произвольные 1280/2560 как универсальный гарантированный предел. Сохранять оба locator, если действительно понадобились две формы, не умножать каждый crop автоматически.
9. **Media — часть знания и retrieval.** Сохранить chunk↔figure, printed captions, labelled visual_description и точную страницу. Для answer-relevant chunk модель получает изображения и визуально решает, полезны ли они; одно описание не заменяет pixels. Search не ждёт Telegram, media/proof pending видны отдельно.
10. **Реальные истории в том же чтении.** Факты независимы от Story; достаточный эпизод сохраняет model-authored basic Variant.body и default revision, а не summary. Не выдумывать story quotas/минимальную длину/обязательную драматургию. Недостаток контекста сохраняет достижимый frontier, который можно продолжить после принятия источника без изменения immutable принятого payload.
11. **Страйп — отдельный запрос.** PDF/DjVu декодер выдаёт только нужную страницу; bbox/полигон выбирает модель, код проверяет геометрию и рисует. Принятый неизменный locator можно переиспользовать. Не запускать proof автоматически на обычном search или import.
12. **Простая ресурсная модель.** Один uploader и configurable pacing, фактический FloodWait. «20 файлов/min» — не доказанный универсальный лимит Telegram и не основание для новой распределённой квотной системы. Независимые sessions имеют per-run exclusive lease; их наличие/готовность проверяется, а потеря WSS не доказывает остановку прежнего процесса.

## Неослабляемые оптимизации и числа

Смысловые passages около256 BGE tokens, strict512 полного augmented input с применимыми prefix/captions/descriptions; BGE-M3 1024 FP32; без silent truncation. Сохранить source span/footnote/cross-page/context логику вместо механического деления по страницам.

Fused encode+lexical+dense+fusion+hydrate в Kaggle; SQL indexes/keyset/batched hydration вместо full-corpus Python scans. Read-only fast path без обязательного fsync/cloud/Telegram preflight, без ожидания заполнения batch. Bounded async telemetry с counted drops, а не потеря domain audit/accepted work.

Protected gates прежние: ≥100 book-equivalent/100000 chunks; ≥30 held-out fact families; BGE Hit@5≥85%, Hit@10≥90%, critical RU→DE Hit@10≥85%; warm backend p95≤1000ms, public MCP p95≤1500ms, evidence deadline2000ms; storage steady≤80%/peak≤90%. Отдельно ошибки/pending, cold/warm/cache/concurrency и useful-answer time. Один импорт не запрещает representative concurrent reads или sequential A→B/B→A comparison.

Go/Rust — незабываемый рабочий пакет оценки, не обязательная перепись. Нужен actual baseline и обоснованный выбор узкого компонента, при необходимости prototype с полным IPC/lifecycle cost и неизменными auth/model/recovery контрактами. Нет нового произвольного threshold15%, обещания 10× или требования внедрить язык ради наличия бинарника. Отсутствие измеренного решения остаётся NOT RUN.

Warm debugging: stable notebook slug, один existing WSS/controller, exact run/version, SQL/function update и минимальная handler activation без повторной загрузки BGE/PG, где совместимо. UNKNOWN сначала reconcile. Не делать Dataset/new notebook транспортом каждой правки; не заменять live PGDATA.

## Persistence: закрытый выбор и незакрытая техническая граница

Закрыт выбор Dataset primary + weekly Yandex. Требование не выдавать ложный durable accepted/Saved также сохраняется. Старый handoff описывал обязательный online S3 tail перед ACK; это нельзя незаметно продолжить под видом weekly backup. Нужен доказанный Dataset persistence/continuation протокол и явная failure matrix для хвоста после последней версии.

До доказанного внешнего покрытия — received/pending либо applied/persistence_pending, не финальный Saved. После подтверждённого Dataset version/cursor — persisted в пределах проверенной recovery-гарантии. Weekly backup сообщает собственную version/age, а не обещает содержать более свежий хвост. Существующий Dev spool может защитить notebook-only loss, но сам по себе не simultaneous Dev disk loss. При неопределённой гарантии сохраняются pending и исходные IDs; никакого скрытого ослабления RPO или нового online-S3 требования.

Формат consistent snapshot, нужный WAL, code/schema/model pins, ordered replay и integrity PG сохраняются. Копировать в Yandex можно тот же подтверждённый artifact без повторного большого backup/restore. Частоту Dataset/checkpoint проверять по real SDK/account/byte+restore budget; не считать weekly cadence разрешением терять подтверждённый дневной input. Старые accepted tails не удалять до verified replacement. Независимая разработка продолжается, unresolved storage contract блокирует только соответствующий final durability claim/cutover.

## Запреты повторной мутации

Не называть эти решения новыми агентскими предложениями. Не вводить 32page/2MiB/12stories как новый продуктовый предел. Не приравнивать отсутствие native text к отсутствию источника, module existence к target parity, Story seed к body, provider upload ACK к сохранённому PG состоянию. Не выключать старые функции молча ради green CI. Полная [матрица приёмки](../testing/rkb-migration-parity-20261021.md) отделяет code/test/live.
