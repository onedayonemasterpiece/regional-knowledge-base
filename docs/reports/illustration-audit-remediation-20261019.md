# Иллюстрации RKB: продуктовые исправления по аудиту 19 октября

Это продолжение [приёмки иллюстраций](illustration-archive-live-acceptance-20261019.md)
и [независимого реестра Claude](illustration-critical-audit-claude-20261019.md).
Аудит используется для исправлений; публикация реестра не завершает продуктовую задачу.

## Изменения

| Пункты аудита | Реализация и проверка |
| --- | --- |
| RKB-A01 | По поручению владельца удалено неиспользуемое временное окружение тестов, кэши pip/uv и три чистых worktree слитых PR. Сохранены итоговый лог тестов и manifest зависимостей. Свободное место выросло с 2 114 371 584 до 6 619 009 024 байт; это измерение, а не постоянная квота. |
| RKB-A02 | Новые source PUT используют тот же резерв 2 GiB, что mirror. Сохранённые операции продолжают receipt/readback при низком диске. Проверено на настоящем SQLite compatibility layer с синтетическими байтами. Guard не резервирует место атомарно против всех писателей хоста и не заменяет глобальный бюджет больших файлов. |
| RKB-A04 | Подтверждённый terminal `blocked` с двумя разрешёнными транспортными кодами допускает новый guarded GET. Ключ следующей попытки и cooldown 15 минут записываются атомарно до I/O. Перезапуск, конкурентный старый receipt и потерянный ответ не сбрасывают состояние. UNKNOWN, pending, отказ в правах и остальные terminal failures сохраняют исходную операцию. |
| RKB-A05 | Первый проверочный GET зеркала и исходника получает стабильный ключ от исходного PUT. Потерянный ответ не создаёт второй download. |
| RKB-A06 | Ошибка polling не стирает marker и время последнего transient retry. |
| RKB-A07 | Четыре охлаждающиеся записи не вытесняют готовую работу из ограниченной выборки. |
| RKB-A08 | Отсутствующий provider SHA приводит к отказу; собственный hash скачанных байтов не подставляется вместо независимого якоря. |

Четыре новые проверки до исправления дали **4 failed / 14 passed**;
после исправления эти воспроизведения проходят. Полный локальный набор:
**554 passed / 31 skipped**, одна dependency deprecation warning.
PostgreSQL и отсутствующие локальные optional зависимости проверяются отдельно CI;
локальные skip не объявляются успешным PostgreSQL прогоном.

## Границы поставки

Сохраняются исходные SOURCE/revision/owner/topic/grant/SHA fences, одна очередь
индексации и существующий VibePublish transport. BGE остаётся только на Kaggle.
Новые публикации, переимпорт книг и подмена иллюстраций для проверки не требуются.
Requirements manifest не изменён.

Реестр Claude остаётся полным исходным ответом, включая предложения, которые
требуют дальнейшей оценки. Эта поставка не означает исправление всех 60 пунктов:
в частности, общий бюджет Vibe download cache, пик копирования больших
оригиналов и перенос тяжёлого SQLite I/O остаются отдельными предложениями.
## Выпущенный продукт и повторная приёмка

[PR149](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/149)
слит. Выпущен точный main commit
[`4dd88d79a7030fb852b3620a884004a25d71c6fe`](https://github.com/onedayonemasterpiece/regional-knowledge-base/commit/4dd88d79a7030fb852b3620a884004a25d71c6fe).
[CI ветки](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39236076784)
прошёл на Python 3.12 и 3.13: **585 passed** на каждой версии, включая PostgreSQL.
[CI merge commit](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39236340681)
также успешен. Worktree при выпуске чист, точный commit содержится в origin/main;
все фактически работающие исходные релизы RKB доказанно являются его предками.

Штатный `deploy_story_registry.py --with-indexer` обновил только API и indexer.
Перед переключением SQLite Online Backup создал консистентную копию
114 008 064 байт; проверка копии при повторном открытии дала `integrity_check=ok`,
`foreign_key_check=ok`; SHA-256:
`92cdf2daf41344b37bf49313a595d8b39364a8fe648ad570585c225f25c6e80b`.
Живую БД не подменяли старой копией; миграций Vibe schema11 не выполняли.

| Проверка после выпуска, 19.10.2026 10:17–10:22 UTC | Фактический результат |
| --- | --- |
| RKB API и indexer | PID 421375 / 421379; CWD и PYTHONPATH указывают на `4dd88d7`; public и loopback health OK |
| Весь активный архив | Повторная read-only сверка **151/151**, точные topic4/origin/native document/provider SHA, 0 повторных entry refs; **121** service-owned + **30** legacy-owner; crop pointers естественно отсутствуют |
| Естественный поиск → fetch | План зоопарка, крест-потент, Зелёные ворота: `ready` BGE results по сохранённым job IDs и правильные дескрипторы. Это повтор уже готовых поисков; новая холодная inference на этом этапе не заявляется |
| Реальный MCP → Telegram fallback | **6/6 ImageContent PNG**: три активных legacy-owner и три service-owned. Фактический SHA каждого совпадает с `delivered_crop_sha256`, `provider_crop_sha256` и прежним проверенным provider evidence |
| Старые оригиналы | **4/4 PDF** повторно скачаны через отдельный сервисный principal и existing loopback transport; реальные SHA совпали с exact topic2/source proof. Файлы 15 453 787 / 1 574 088 / 12 752 172 / 17 013 878 байт; хеширование в памяти, без копирования книг в evidence |
| Права | **34/34** exact-entry grants вновь прошли текущий production `require_grant` на read-only соединении; actor остаётся non-owner. Реальный GET непривилегированной owner-записи вновь отклонён `media_store_entry_not_found` |
| Street Story / Vibe | API, worker, Street Story, graph и POI delivery сохранили PID/CWD/PYTHONPATH. Байты двух сохранённых Original и composited assets совпадают с прежними SHA. Исходный POI event, idempotency key, source revision и digest сохранены; sender `delivered`, receiver `unresolved_identity`, без ложного принятия identity |
| Ресурсы и логи | После backup и приёмки около **6.0 GiB** свободно; post-release журнал API/indexer не содержит записей уровня error. Новых PUT, генераций, публичных постов и переимпорта не было |

Продуктовый результат этой задачи подтверждён на выпущенном RKB runtime.
Оставшиеся предложения аудита выше не объявляются исправленными и не заменяют
отдельную приёмку массового импорта, качества всех книг или редактора Street Story.

Локальные воспроизведения, план и результат очистки, логи и runtime evidence:
`/home/dev/artifacts/regional-knowledge-base/20261019T100239Z-illustration-audit-remediation-20261019`.
Это retained evidence; удалённые pytest fixtures не содержали оригинального корпуса.
