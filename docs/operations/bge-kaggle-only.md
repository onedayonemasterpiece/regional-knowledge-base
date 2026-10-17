# BGE-M3 только на Kaggle — действующая архитектурная граница

**Обязательное решение владельца (17.10.2026):** никакого локального инференса BGE на DevCoveer. BGE-M3 вычисляет **и запросы, и документы, включая редакционные истории, только в Kaggle CPU notebook**. В DevCoveer находятся лишь долговечная очередь заданий BGE, проверка идентичности actor/model/revision, контроллер удалённого запуска и результаты векторного поиска. Новые сервисы, локальные копии весов, локальный ONNX BGE, платные API или обход Kaggle при недоступности запрещены.

Эта инструкция имеет приоритет перед прежними пилотами, временными deployment-скриптами и историческими отчётами, в которых могли фигурировать локальная модель или `regional-knowledge-bge-query.service`. Изменять это архитектурное решение без нового согласования нельзя.

## Проверенный runtime после инцидента

- Старый локальный BGE-сервис `regional-knowledge-bge-query.service` остановлен и **замаскирован** через user systemd (`inactive`, `masked`): обычный запуск или `enable` невозможны без осознанного `unmask`.
- Выведен из эксплуатации установленный unit-файл и удалён весь исключительно локальный inference model directory `~/.local/share/regional-knowledge-base/bge-m3-int8/`, **585 569 350 байт**, включая `onnx/model_int8.onnx` (**568 479 395 байт**). Порт `127.0.0.1:8768` не слушает.
- Из закрытого `service.env` удалён устаревший флаг `RKB_BGE_QUERY_LOCAL_ENABLED`; не редактировались остальные ключи и секреты. Ни один runtime fallback не должен активировать BGE на DevCoveer.
- [PR #125](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/125) переводит запросы по книгам и редакционным историям на прежний actor-bound `BgeQueue`/Kaggle. Старые ONNX story-векторы не считаются совместимыми; индексация на Kaggle возобновляется по текущим редакциям и версионированию.
- Код local BGE model loader, loopback-клиента и systemd-unit удалён из актуального `main`. Защитные тесты CI проверяют их отсутствие, корректную маршрутизацию через очередь и запрет локального запуска даже при старой переменной среды.
- Ни графовая служба, ни VibePublish, ни VS Code/Claude Code, ни незавершённые импорты специально не останавливаются; исходные книги и ACL остаются прежними.

## Что **не** является локальным BGE-инференсом

**Локальный E5-small INT8**: ранее принят как быстрый always-ready tier ([архитектурное решение 04.10](../reports/embedding-runtime-decision-20261004.md), [production acceptance](../reports/fast-e5-production-acceptance-20261004.md)). Работает на `127.0.0.1:8767`, размерность 384 и ограниченный CPU/memory. Путь model/runtime был случайно повреждён ссылками на удалённые временные артефакты, а не документированной процедурой вывода E5 из эксплуатации. Восстановлен pinned model `Xenova/multilingual-e5-small` в постоянное хранилище; **не запускать массовую E5-переиндексацию только ради чисел готовности**, так как основной publication gate по-прежнему BGE. E5 полезен как быстрый уровень при холодном запуске Kaggle и как сервис проверки токенизации при импорте.

**Отдельный BGE tokenizer-only файл** `fast-e5/bge-m3-tokenizer.json` — это метаданные токенизации, **не модель BGE**: не содержит ONNX/PyTorch weights и ничего не кодирует в векторы. Он необходим для проверок лимита 512 токенов при импорте. Удалять его или общую среду Python `~/.local/share/rkb-embedding/venv`, которую теперь использует закреплённый E5, запрещено без независимой проверки потребителей.

**Kaggle queue/cache** не являются локальным BGE runtime; модель выполняет операции удалённо. Журнал, старыe immutable SHA и диагностические отчёты могут сохранять *историческое упоминание* ошибочной конфигурации; история/доказательства не переписываются и не служат инструкцией по установке.

## Регламент для следующих агентов и релизов

1. Изменения BGE-маршрутизации допускаются только с сохранением `BgeQueue`, Kaggle controller/worker и actor/idempotency guard. Если Kaggle `starting` или `unavailable`, вернуть честный `pending`/degraded результат, а не запускать локальный BGE.
2. **Не создавать, не скачивать, не восстанавливать** локальные веса `bge-m3-int8`, локальный BGE query service, его installer unit или `LocalBGEQueryEmbedder`. CI должен отклонять повторное появление этих компонентов. Случайный старый deployment-скрипт не является разрешением на `systemctl unmask`.
3. Перед каждым production release проверить: `systemctl --user is-enabled regional-knowledge-bge-query.service` → `masked`; путь `bge-m3-int8` отсутствует; 8768 не слушает; живы RKB MCP, индексатор, E5 и Kaggle controller. Если инвариант нарушен — отказать в релизе вместо «быстрого локального восстановления».
4. При обновлении общего runtime защищать pinned E5 и tokenizer-only assets; не удалять их по признаку `rkb-embedding`/слову `BGE`.
5. После Kaggle/revision migration проверять отдельно `indexing_status`, актуальные story vectors (68/68 для исходного набора), настоящий `story_bge_ready` и исходные естественные контрольные запросы. Временное `story_bge_partial` не является продуктовой приёмкой. Строгие frozen dense-retrieval, latency и mass-ingestion gates сохраняются и не заменяются health HTTP 200.

### Быстрый операторский контроль

```bash
systemctl --user is-active regional-knowledge-bge-query.service   # inactive
systemctl --user is-enabled regional-knowledge-bge-query.service  # masked
test ! -e ~/.local/share/regional-knowledge-base/bge-m3-int8
curl -sf --max-time 2 http://127.0.0.1:8767/health          # E5 ready
curl -sf --max-time 2 http://127.0.0.1:8000/health          # MCP ok
```

Вне этого репозитория произвольные пользовательские эксперименты или истории Git не удаляются массово без отдельной проверки их владельца и ссылок; гарантия относится к **действующему runtime, его модельным весам, установочным файлам и текущему развертываемому коду**.
