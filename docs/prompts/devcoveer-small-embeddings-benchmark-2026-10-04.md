# Regional Knowledge Base — DevCoveer small-embedding benchmark

Дата: 2026-10-04

Это **исполнительная задача для Codex**, не архитектурный аудит. Нужны фактические локальные измерения на DevCoveer и воспроизводимый отчёт.

## Цель

Выбрать маленький multilingual embedding encoder для **быстрого / cold-start retrieval** Regional Knowledge Base, который можно постоянно держать готовым на DevCoveer.

Основной точный retrieval позднее будет отдельно тестироваться на **BGE-M3 в Kaggle CPU**. В этой задаче Kaggle и BGE-M3 не внедрять.

Искомый local fallback должен по возможности укладываться в:

- максимум **1 CPU** на inference;
- максимум **1 GiB RAM** для всего encoder-процесса;
- низкую задержку одиночного поискового запроса;
- достаточное качество, чтобы дать предварительный поиск, пока основной Kaggle worker холодный.

Если лучший вариант заметно меньше лимита — это хорошо. Не надо расходовать лимит только потому, что он доступен.

## Жёсткие ограничения

1. **Никаких платных embedding/API inference-вызовов.**
   - не использовать OpenAI embeddings;
   - не использовать Google/Gemini embeddings;
   - не использовать Cloudflare Workers AI;
   - не использовать Jina/Voyage/Mistral/NVIDIA/OpenRouter и другие внешние inference API.
2. Общие API-ключи в `/home/dev/.env` не считать разрешением на использование.
3. Разрешены загрузки **публичных весов/токенизаторов** моделей для локального inference.
4. Не менять production search/embedding config Regional Knowledge Base на кандидата до окончания benchmark.
5. Не переэмбеддивать production corpus и не перезаписывать активные production-векторы.
6. Не менять схему production DB ради эксперимента.
7. Не перезапускать production Regional Knowledge service без реальной необходимости для чтения данных. Для benchmark это не требуется.
8. Не встраивать ML runtime в MCP. Benchmark encoder должен жить отдельно.
9. Не строить новый общий orchestration framework. Это локальный benchmark.
10. Не использовать GPU. Все тесты только CPU.
11. Не занимать все CPU хоста: измеряемый inference ограничивать одним CPU.
12. Не раздувать диск. Кэш моделей вести в одном известном каталоге и после испытаний удалить явно проигравшие/недокачанные дубликаты, сохранив необходимые evidence/report.

## Исходное состояние

Репозиторий:

`onedayonemasterpiece/regional-knowledge-base`

На текущем production уже запрещён неявный fallback на общий `OPENAI_API_KEY`: внешние embeddings требуют отдельной явной `RKB_EMBEDDING_*` конфигурации, иначе поиск деградирует до lexical.

Первая тестовая книга уже импортирована:

- document_id: `7ce738b0-d3d3-4fc2-9a61-aa58b537a0e9`
- ingestion_id: `570f4443-e869-4ad0-8851-7b4093f49e22`
- active revision: 1
- 174 physical pages
- 712 retrieval chunks

Сначала **проверь фактическое live состояние** и наличие документа; не доверяй этому блоку слепо.

## Кандидаты

Протестировать минимум три кандидата.

### A. Multilingual E5 Small — основной первый кандидат

Модель:

`intfloat/multilingual-e5-small`

Предпочтительный runtime для DevCoveer:

- ONNX Runtime CPU;
- INT8 ONNX export, если фактически доступен и корректно грузится;
- ориентир по готовому browser/ONNX export: `Xenova/multilingual-e5-small`, `onnx/model_quantized.onnx`.

Контракт retrieval:

- dimensions: 384;
- query prefix: `query: `;
- document prefix: `passage: `;
- attention-mask mean pooling, если export не даёт уже готовый sentence embedding;
- L2 normalization;
- max 512 tokens.

Не смешивать результат с E5-base или другими E5 spaces только потому, что название похожее.

### B. EmbeddingGemma 300M — компактный альтернативный кандидат

Модель:

`google/embeddinggemma-300m`

Предпочтительный ONNX источник, если публично доступен без нового ручного согласия:

`onnx-community/embeddinggemma-300m-ONNX`

Проверить Q4 CPU вариант. Использовать корректный sentence embedding output / documented pooling, а не произвольный hidden state.

Начать с native/recommended dimension; дополнительно можно проверить Matryoshka 256, если это поддержано данным runtime и после truncation выполняется необходимая нормализация.

Если конкретные веса требуют нового принятия лицензии/условий или недоступны без credentials — **не обходить это**. Зафиксировать blocker и продолжить с остальными кандидатами.

### C. POTION multilingual / Model2Vec

Кандидат:

`minishlab/potion-multilingual-128M`

Проверить как очень дешёвый CPU encoder:

- output dimensions: 256;
- отдельное vector space;
- никакой попытки сравнивать его векторы напрямую с BGE/E5.

Использовать официальный/стандартный Model2Vec runtime.

### Не добавлять модели бесконечно

Если все три измерены, этого достаточно для текущего решения. Четвёртый кандидат добавлять только если:

- один из трёх объективно невозможно запустить;
- или есть очевидный маленький вариант, который уже установлен/реализован в связанных проектах и почти ничего не стоит проверить.

Не превращать задачу в обзор embedding zoo.

## Изоляция runtime

Создай отдельный lab-каталог вне production release, например:

`/home/dev/.local/share/rkb-embedding-lab/`

Там допустимы:

- отдельный venv;
- model cache;
- benchmark scripts/data.

Не добавляй тяжёлые runtime dependencies в production `regional-knowledge-base` без необходимости.

Каждый кандидат тестировать **отдельно**, чтобы память не складывалась.

### Resource envelope

Фактический inference benchmark должен выполняться с ограничением, эквивалентным:

- MemoryMax = 1 GiB;
- CPU quota = 1 CPU;
- без использования GPU.

Если инфраструктура DevCoveer позволяет безопасно задать MemoryMax/CPUQuota через transient user scope/service — используй её.
Если нет, измерь process-tree RSS/PSS/peak и явно укажи, что hard cgroup limit не был применён. Не симулируй PASS.

Для ONNX Runtime выставить intra/inter op threads так, чтобы encoder не расползался на все ядра. Избегать busy spinning, если runtime поддерживает настройку.

## Корпус benchmark

Нужно тестировать **на реально импортированной книге**, но не изменять production-векторы.

Получить активные 712 chunks и их тексты через существующий backend / object-storage projection / безопасный read path.

Не копировать секреты в artifacts и не печатать их в логи.

Сделать локальный benchmark corpus:

- chunk_id;
- title;
- text;
- page provenance, если доступна;
- никаких credentials.

Если corpus snapshot содержит большой объём copyrighted text, не коммитить его в Git. Хранить локально в lab/artifact storage; в Git-отчёте оставлять только hashes/counts/metrics и короткие допустимые фрагменты при необходимости.

## Retrieval benchmark

Нужен **реальный quality benchmark**, а не только latency.

Сделать не меньше **20 вопросов** по книге и source-verified relevant evidence.

Начальный seed из уже проверявшихся запросов:

1. Почему крепость Кёнигсберг получила своё название и какую роль играл король Оттокар?
   - известный релевантный chunk: `9869b831-1ff4-56bd-b304-a154c85d7468`
2. Когда Альтштадт получил городскую грамоту и какие права она закрепляла?
   - `ff936345-f134-57f4-bc17-2738a0b061b7`
3. Когда и на каком праве был основан Лёбенихт?
   - seed evidence: `ad2dd9c8-4cc6-561f-b20a-e24d200946f6`
4. Когда и на каком праве был основан Кнайпхоф?
   - seed evidence: `8441d1dc-825b-562a-a4ba-12bf43afa92e`
5. Почему Кёнигсберг стал резиденцией/столицей орденского государства в 1457 году?
   - `02013e44-7067-561c-a421-145c11e9419f`
6. Когда была основана Альбертина и какие задачи ставил перед университетом герцог Альбрехт?
   - `7220e9ff-5df8-535f-976f-382eb9e5cab1`
7. Как чума 1709–1710 годов повлияла на население Кёнигсберга?
   - `f5018c1c-8e9a-5d38-a921-9bc23349d5a0`
8. Почему три города Кёнигсберга объединили в 1724 году?
   - `aa0d8079-c604-51d7-9061-01cce5136319`
9. Какую роль играли голландские купцы в торговле Кёнигсберга в XVI–XVII веках?
   - `25a0af7c-043e-5e03-be40-b80b16ccc73e`
10. Кто такой Иеронимус Рот и почему конфликт с курфюрстом дошёл до его преследования/ареста?
   - seed evidence: `5ae7d9f9-ed94-5f28-8141-170509f0b7e3` и/или `4a540ada-dd2d-5977-b294-f2ea7d4b842a`

Проверить seed evidence против актуального документа перед benchmark.

Оставшиеся вопросы построить из реального текста книги так, чтобы были представлены:

- прямой факт;
- перефразированный вопрос без совпадения ключевых слов;
- имя/топоним;
- дата;
- причина/следствие;
- составной вопрос, требующий двух фрагментов;
- минимум 2 вопроса, на которые книга не даёт уверенного ответа.

Не делать benchmark слишком лёгким за счёт копирования формулировок из chunk.

Сохранить benchmark questions/evidence в небольшом JSON/JSONL, который можно коммитить только если он не содержит длинных фрагментов книги.

## Retrieval modes

Для каждого кандидата вычислить document vectors локально и сравнить:

1. **vector-only** brute-force cosine по 712 chunks;
2. **lexical-only** baseline существующей системы;
3. **fast-vector + lexical fusion**.

Для fusion не складывать сырые similarity разных моделей. Использовать простой rank fusion (например RRF) и одинаковую формулу для всех кандидатов.

Не менять production RPC ради benchmark — можно воспроизвести fusion локально.

### Метрики качества

Минимум:

- Recall@1;
- Recall@5;
- Recall@10;
- MRR;
- для multi-evidence вопросов — доля вопросов, где все требуемые evidence попали в top-10;
- false-positive behavior на вопросах без ответа.

Отдельно показать результаты:

- vector-only;
- lexical-only;
- fused.

Не объявлять модель победителем по одному среднему числу, если она систематически ломает важный класс запросов.

## Performance benchmark

Для каждого успешно запущенного кандидата измерить минимум:

### Размер

- суммарный размер model/tokenizer файлов;
- размер venv/runtime отдельно;
- размер постоянного model cache.

### Память

- memory after model load;
- peak memory during one query;
- peak memory during document batch encoding;
- весь process tree, а не только Python heap.

### Startup

Разделить:

1. первый download — справочно;
2. cold process start при уже локальных model files;
3. model load → first successful embedding;
4. warm request.

### Query latency

После warmup:

- минимум 100 запросов из benchmark;
- p50;
- p95;
- max.

Отдельно 4 concurrent requests при **одном CPU**, с очередью или bounded concurrency. Цель — понять tail latency, а не заставить модель параллельно жечь несколько CPU.

### Corpus throughput

Измерить время encoding всех 712 chunks:

- разумными batch sizes;
- при том же CPU/RAM envelope;
- зафиксировать лучший безопасный batch size и peak RAM.

Это не означает, что production ingestion будет выполняться на DevCoveer; показатель нужен для сравнения runtime.

## Предварительные product gates

Это экспериментальные пороги, а не заранее объявленный PASS:

- hard/observed peak RAM < 1 GiB;
- желательно steady loaded RAM < 800 MiB;
- warm single-query p95 <= 1 s;
- полный fast retrieval p95 <= 2 s для одного запроса;
- отсутствие crash/OOM;
- качество fused retrieval заметно лучше lexical-only либо по меньшей мере закрывает его семантические провалы.

Если кандидат не проходит latency, но проходит RAM и очень хорош по качеству — не выбрасывать молча, а показать trade-off.

## Согласованность разных runtime

Для кандидата-победителя подготовить небольшой compatibility fixture:

- 10 фиксированных коротких multilingual strings;
- exact preprocessing contract;
- expected vector dimension;
- normalized vector hash/summary или tolerance-based comparison data.

Цель — позже использовать **то же fast vector space**:

- на DevCoveer;
- в Projects Hub PWA/WebView;
- возможно в native Android.

Не делать сейчас Android/PWA inference implementation. Только подготовить данные, позволяющие проверить совместимость следующей задачей.

## Что НЕ входит в эту задачу

- BGE-M3 Kaggle orchestration;
- 30-minute Kaggle lease;
- 11-hour rotation;
- production queue;
- Projects Hub WebGPU/WASM integration;
- Android native inference;
- migration production vector schema;
- dual-vector production storage;
- автоматическое переключение Mira fast → main;
- внешние inference APIs.

Это следующие шаги после фактического выбора small encoder.

## Реализация benchmark

Предпочтение простому, удаляемому test harness.

Можно добавить в repository:

- `scripts/benchmarks/...`;
- маленькие benchmark question/evidence JSON;
- README/report.

Но:

- model binaries не коммитить;
- corpus text snapshot не коммитить;
- временные скачивания не добавлять в Git;
- production dependencies не раздувать без необходимости.

Если существующие reusable embedding contracts/scripts из `my-data-hub` реально помогают — переиспользуй минимально. Не тащи весь my-data-hub runtime.

## Очистка после теста

После сбора доказательств:

- удалить недокачанные/сломанные model copies;
- удалить ненужные временные corpora;
- оставить один понятный lab cache только если он нужен для воспроизводимости/следующего теста;
- показать сколько диска осталось;
- не трогать unrelated untracked artifacts проекта.

## Deliverables

Нужны **фактические результаты**, а не план.

Сохранить:

### 1. Машиночитаемый результат

Например:

`artifacts/embedding-benchmarks/devcoveer-small-embeddings-20261004.json`

В Git не обязательно коммитить runtime artifact, если там большие/чувствительные данные. Но schema/summary должны быть воспроизводимыми.

Для каждого candidate:

- exact model/revision;
- exact runtime/export;
- vector space contract;
- file sizes;
- CPU/RAM restrictions;
- startup timings;
- memory;
- query latency;
- corpus throughput;
- Recall/MRR;
- errors/blockers.

### 2. Итоговый Markdown

Создать:

`docs/reports/devcoveer-small-embedding-benchmark-20261004.md`

Обязательно:

- фактическая таблица E5-small vs EmbeddingGemma vs POTION;
- lexical baseline;
- quality table vector/fused;
- performance/memory table;
- какой кандидат проходит 1 GiB / 1 CPU;
- конкретная рекомендация для fast/cold-start encoder;
- какие кандидаты отвергнуты и почему;
- что нужно тестировать следующим шагом на PWA/Android;
- никаких предположительных цифр в колонках фактических измерений.

### 3. Если benchmark harness полезен

Покрыть тестами:

- preprocessing/prefix contract;
- normalization;
- vector dimensions;
- ranking metric computation;
- RRF;
- benchmark fixture validation.

## Definition of Done

Задача завершена только если:

1. на DevCoveer реально запущен хотя бы E5-small и ещё минимум один из двух других кандидатов; если кандидат объективно blocked, blocker доказан;
2. inference реально ограничен/измерен как 1 CPU / <=1 GiB target;
3. измерена фактическая peak memory;
4. измерены warm latency p50/p95;
5. измерено время кодирования 712 chunks;
6. минимум 20 source-verified вопросов реально прогнаны;
7. посчитаны Recall@1/5/10 и MRR;
8. есть lexical-only baseline;
9. есть одинаковый fused benchmark;
10. production corpus/search config не изменён;
11. ни один платный embedding API не был вызван;
12. полный тестовый suite проекта после изменений зелёный;
13. создан итоговый Markdown report;
14. выбран **один рекомендуемый fast encoder** либо честно доказано, что ни один кандидат пока не подходит;
15. подготовлен compatibility fixture для следующего клиентского теста;
16. временный мусор очищен.

## Стиль работы

- Не начинай с нового большого аудита.
- Сначала проверь current main/runtime и safety invariants, затем переходи к измерениям.
- Не останавливайся после установки моделей.
- Не выдавай synthetic/mock numbers за benchmark.
- Не оптимизируй модель до первого настоящего измерения.
- Не строй универсальную embedding-platform.
- Бритва Оккама: небольшой воспроизводимый harness, реальные цифры, одно решение.

В конце верни:

1. короткий verdict;
2. таблицу ключевых фактических измерений;
3. ссылку на committed Markdown report;
4. exact commit/PR;
5. один короткий prompt для следующего шага **только если он уже очевиден из результатов**.
