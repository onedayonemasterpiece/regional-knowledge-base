# RKB / DevCoveer2: восстановление архитектуры BGE на Kaggle и локального E5

**Инцидент:** 17 октября 2026. **Цель:** освободить ресурсы разработчика и восстановить ранее принятую границу: **BGE-модель запускается только на Kaggle, E5-модель — локально**.

## Подтверждённая причина и границы

17.10 локальная служба `regional-knowledge-bge-query.service` всё ещё загружала ONNX BGE-M3. До правильного увеличения RAM с ~1,7 до ~7,8 ГБ в журнале были `oom-kill` с пиком службы ~1 ГБ и перезапусками. Ранние изменения PR #122/123 подключили редакционные story-векторы к этому локальному энкодеру; локальный путь запросов к BGE существовал и до них. Параллельно `regional-knowledge-e5.service` циклически завершался `203/EXEC`: ссылки на Python-runtime и старый source-release указывали на уже отсутствующие каталоги временных артефактов. Каталог ONNX-модели E5 был пуст. Это факты независимых проверок; точное влияние каждого компонента на конкретное подключение Claude Code в VS Code клиентом не измерялось.

Владелец добавил RAM **именно DevCoveer2**: `MemTotalBytes=7 783 940 096`, swap 4 ГБ. Это не является разрешением держать BGE на этом сервере.

## Исправления и выпуск

1. Служба локального BGE остановлена через пользовательский systemd и **отключена из автозапуска** (`systemctl --user disable --now regional-knowledge-bge-query.service`). Независимое наблюдение: статус `inactive`, `is-enabled=disabled`, TCP 127.0.0.1:8768 закрыт. Исходные MCP, индексатор, Kaggle controller и другие проекты не останавливались. Доступная RAM после отключения выросла ориентировочно на ~1 ГБ, но замеры не были изолированным нагрузочным экспериментом.
2. [PR #125](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/125), **source/served SHA `293e426fb7badabaf88b72d6651a6f4f7961329f`**, CI Python 3.12/3.13 [PASS](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39010116587). Книжные BGE-запросы и индексация редакционных карточек используют только существующую durable очередь с Kaggle worker; старый флаг `RKB_BGE_QUERY_LOCAL_ENABLED` не может загрузить модель локально. Актуальные старые story-векторы ONNX переиндексируются BGE Kaggle по версии, не признаются совместимыми автоматически.
3. Безопасный production rollout: immutable `main` commit, проверенный dry-run, Online Backup SQLite объёмом 82 100 224 байта с `integrity_check=ok` и `foreign_key_check=ok`, PID/env readback одинакового SHA у MCP и индексатора, local/public `/health` OK. Источники книг, рабочую копию с незакоммиченными изменениями, KA10, граф и права доступа не затрагивали.
4. После исправления удалённый Kaggle BGE worker подтверждён в состоянии `ready`. Книжный индекс: BGE **2434/2434** ready. `story_search` при незавершённой повторной индексации явно возвращает `story_bge_partial` (не PASS) и `kaggle_query_state=kaggle_ready`.
5. E5 аварийный restart-loop временно остановлен. Закреплённый `Xenova/multilingual-e5-small` (revision `761b726dd34fb83930e26aab4e9ac3899aa1fa78`) восстановлен из официального источника и проверен по `SHA256=f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193`. ONNX и сопутствующие файлы перенесены в **постоянный** каталог `~/.local/share/regional-knowledge-base/e5-model-761b726...`, не связанный с уборкой временных артефактов. Исправлены symlink-цели `fast-e5/models`, `fast-e5/current`, `fast-e5/runtime`; runtime использует сохранённое Python-окружение `~/.local/share/rkb-embedding/venv`. Этот runtime теперь требуется E5, поэтому **не считать его неиспользуемым BGE-артефактом при будущей уборке**. E5 service `active/running`, `127.0.0.1:8767/health` возвращает `ready=true`, `dimension=384`, `token_budget_ready=true`, PID подтверждён.
6. Восстановительный E5-артефакт загружен в управляемую `retained` папку `/home/dev/artifacts/regional-knowledge-base/20261017T065008Z-e5-recovery-20261017`; постоянная модель находится отдельно от неё и переживает уборку временных артефактов.

## Ограничения

- В середине повторной story-индексации Kaggle **58/68** актуальных vectors; 10 устаревших local-ONNX ещё не заменены. Требуется финальный readback 68/68 и дословный исходный естественный semantic запрос до закрытия RKB story acceptance. Пока не закрыто, `story_bge_partial` — корректный и обязательный degraded status.
- Наблюдался отдельно внешний `mcp-vibepublish` OAuth `502` в необязательном зеркалировании: не относится к запуску BGE и не лечился сменой модели.
- Число обслуженных BGE-запросов и наличие вектора не заменяют frozen dense holdout и строгие массовые требования `.devcoveer/requirements.json`.
- Server-side tunnel DevCoveer и OpenCode active/running с `NRestarts=0` в проверенном периоде; реальный клиентский сеанс VS Code/Claude Code владелец подтверждает отдельно.

**Итог:** лимит нагрузки устранён выключением ненужного local BGE; запросы и книжная индексация перенесены на Kaggle; pinned E5 восстановлен в пределах ресурса сервера. Продуктовая приёмка BGE-историй окончательная после 68/68, без скрытого lexical fallback.
