# Задание RKB-Импорт Н3: инструкция stage без лишних end-отметок и без ожидания apply

Заказчик: RKB-Импорт (Claude). Аудит: `docs/audits/rkb-import-efficiency-audit-20261027.md` (ветка `docs/rkb-import-efficiency-audit-20261027`), §3.1, §3.2, §4 Н3.

## Цель для человека

Читатель книги через MCP тратит лишнее время на каждой странице:

1. Он ставит end-отметку на каждую статью словаря (~20 на страницу) и для каждой считает длину своего текста в Unicode-символах. Сервер и так закрывает статью, когда начинается следующая того же вида.
2. После stage он ждёт apply (~1 мин) и только потом открывает следующую страницу, хотя сервер разрешает читать её сразу.

Обе причины — в тексте инструкций MCP, не в логике сервера. Нужно поправить только текст.

## Доказательства

- `src/regional_knowledge/source_units.py:98-110`: следующий start того же вида закрывает предыдущую единицу.
- `tests/test_source_unit_anchors_p0a.py:102` `test_i02_next_chapter_start_is_confirmed_end_without_redundant_end_marker`: это уже закреплено тестом.
- `src/regional_knowledge/server.py:1553` (описание `book_ingest`): «Use end markers at real next boundaries, not guessed ends from page counts.» — модель понимает это как «ставь end у каждой статьи».
- `src/regional_knowledge/server.py:653` (чеклист, пункт 6): «For next_action=wait, call status; …» — модель ждёт apply.
- `src/regional_knowledge/kaggle_importer.py:706-716`: следующий stage до apply предыдущего получает обычный отказ `prior_write_pending`; `kaggle_importer.py:973-986`: `book_pages` не ждёт apply.

## Что сделать

1. В описании `book_ingest` для профиля `kaggle_source` (`server.py`, строка с «Use end markers at real next boundaries») заменить это предложение по смыслу на:
   «A start closes the previous open unit of the same kind; do not add an end before it. Add an end only where a unit stops without a following same-kind start (before non-unit material such as a letter heading, figure or appendix, or at the real end of a chapter).»
2. В пункте 6 чеклиста (`server.py:653`) заменить текст на:
   «6. After stage returns wait, read the next page with book_pages and prepare its packet; call status once right before the next stage. If stage answers prior_write_pending or retry, resend the identical stage after status, not a new operation.»
3. Тест: словарная синтетическая страница из 3 статей **только со start** даёт `resolve_source_units` с теми же `source_spans` и `end_anchor` для двух закрытых статей, что и та же страница со start+end; последняя статья — `unit_not_ready`. Плюс проверка, что описание `book_ingest` больше не содержит «Use end markers at real next boundaries», а чеклист содержит «read the next page with book_pages».
4. В `docs/ingestion.md`, раздел «Логические границы статей и глав», одна фраза о том же (end не нужен перед следующим start того же вида).

## Чего нельзя

- Не менять логику `source_units.py`, контракты (`contracts.py`), валидацию, stage, apply, replay и fingerprint материализатора.
- Не добавлять новые поля, состояния, флаги, ретраи, проверки, гейты и режимы восстановления.
- Не трогать остальные пункты чеклиста и блок дословности (PR 561).
- Не трогать live, Kaggle, API и не сливать.

## Бюджет

Production: ≤ 6 изменённых строк в `src/regional_knowledge/server.py`, только строки текста инструкций. Тест — сколько нужно для одного различающего сценария.

## Один различающий тест

Страница «только start» и страница «start+end» дают идентичные закрытые единицы (spans и end_anchor). Без правки инструкции тест на текст описания падает; тест резолвера проходит на текущем коде — это доказательство, что end был лишним.

## Живая проверка — Шкипер

После установки API (требует совместимой ротации, если изменённый файл входит в Kaggle bundle; `server.py` — нет, но проверить): читатель Albinus на двух соседних страницах — p_N по старой и p_N+1 по новой инструкции. Сравнить число отметок на странице, минуты между stage-сообщениями и `book_find(view=units)`: complete-единицы те же, последняя статья открыта.

## Кто заметит и как часто

Читатель — на каждой странице словаря или энциклопедии (~20 отметок и ~1 мин ожидания на страницу). Пользователь PDF-экспорта — никто: границы статей те же.

## Отчёт

Один draft-PR от свежего `main`, полный CI (Python 3.12/3.13 и envelope). Отчёт STARTED / DONE / BLOCKED в общий тред координации: https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792868771439369 с подписью «Исполнитель Н3 (заказ RKB-Импорт)». Не сливать, не трогать live.
