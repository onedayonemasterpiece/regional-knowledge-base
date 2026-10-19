# Критический аудит формирования story при импорте

Дата: 2026-10-19. Объект: RKB main `8799f8a01e63070f19fe4bea714777d1be9bbae6`, существующие MCP-выдачи и документация. Это исследование причин и план исправления, не новая extraction acceptance.

## Главный вывод

Одного требования «пиши интереснее» недостаточно. Существующий замысел уже просит извлекать эпизоды, но фактический контракт создаёт Story из одной proposition. Через assertion этой Story при импорте сохраняются даты и observations. Поэтому исправление должно сначала дать полезным фактам самостоятельный source-bound путь, затем использовать существующее досье/Variant для связной подачи. Иначе фильтрация слабых stories теряет знания.

Рабочее решение: улучшенный prompt и renderer плюс ограниченная compose стадия внутри **одного persisted research workflow**. Существующие angle, gaps, assertions/evidence/assessments и VariantBlock переиспользуются. Квоты, обязательная драматическая дуга, второй LLM-судья и новый narrative registry не нужны.

## Evidence и границы выводов

Read-only convenience sample: 10 последних записей, 5 по одному тематическому запросу и 2 по другому; после дедупликации **17 записей**. Открыты **3 полных editorial dossier**: справочный, пространственный и эпизод с несколькими основаниями. Во всех трёх angle отсутствовал, variants/interest_assessments/gaps были пустыми; summary повторял исходное утверждение. В одном случае новое межисточниковое основание не вошло в summary. В search выдаче potential отсутствовал; narrative label встречался у справочного материала. Spoken summary многократно начинался с оговорок и мог обрываться внутри слова.

Это **не случайная выборка, не оценка доли плохих историй в корпусе и не аудит опубликованных текстов**. Исследованные записи были candidates, а не publish_ready variants. Оригинальные книги в дополнительном story-проходе не перечитывались: нельзя утверждать, какие детали источник действительно содержит и импорт потерял. Частные названия, corpus IDs и полные карточки в публичный отчёт не перенесены.

## Подтверждённые механизмы

| Находка | Исходник и следствие |
|---|---|
| Один candidate содержит одну proposition≤500, summary≤1000, 1–4 exact evidence | [story_contracts.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/story_contracts.py#L420-L468). Размер пакета удобен для stage, но не выражает несколько самостоятельных утверждений эпизода. |
| Новый принятый candidate создаёт Story и одну attributed_account assertion | [story_ingestion.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/story_ingestion.py#L242-L278). Семантический критерий связного материала не является условием этой materialization. |
| Accepted-source extraction повторяет тот же механизм | [story_registry.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/story_registry.py#L2432-L2532). Исправить только import недостаточно. |
| Даты/числа идут внутри story_candidates и требуют её assertion | [server.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/server.py#L869-L895), [ingestion_enrichment.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/ingestion_enrichment.py#L76-L112). Запрет «не-историй» до расширения factual contract способен терять календарь/наблюдения. |
| Prompt уже требует эпизод, zero-candidate и pending между окнами | [исходный prompt](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/docs/prompts/scalable-cross-book-story-enrichment-20261009.md#L64-L122). Проблема не сводится к отсутствию слова «эпизод». |
| В fingerprint входят title/material_type | [story_ingestion.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/story_ingestion.py#L67-L77). Редакционное переименование может стать новой identity; нужен стабильный source-bound key и явная связь продолжений. |
| Есть SetAngle/AddGap/RecordInterest и evidence-linked VariantBlock | [story_contracts.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/src/regional_knowledge/story_contracts.py). Отдельный редакционный framework не оправдан. |
| Механические fixtures принимают короткое одиночное сообщение за Story | [test_story_ingestion.py](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/8799f8a01e63070f19fe4bea714777d1be9bbae6/tests/test_story_ingestion.py#L29-L119). Это полезная проверка provenance/idempotency, но не продуктовая граница facts/episode. |

## Что считать качественным материалом

Факт сохраняет исходное сообщение и доказательство присутствия в источнике. Эпизод объединяет действительно связанные действия/изменения/практику/вопрос. Story — исследовательское досье, а Variant — выбранный текст для аудитории. Private draft и independent disagreement case сохраняют собственную семантику.

Change/choice/consequence полезны, но не обязательны для всякого everyday_life/explanation. Связный спокойный рассказ допустим. Точный день, герой, конфликт и длина не являются gates. Таблица может дать ноль новых stories с полностью сохранёнными observations; подпись или сравнение таблиц в достаточном контексте может обосновать эпизод.

Читательская полезность: материал объясняет, что связано и зачем читать именно его, без пустой фразы «значимое место региона». Конкретная деталь-hook берётся из источника или отсутствует. Историческая неопределённость и contested case не понижают автоматически редакционную готовность корректно атрибутированного текста.

## Минимальная реализация

1. Обобщить source-bound factual assertion, dates/observations/evidence без обязательной Story; existing revisions/history/поиск сохраняются.
2. Bounded findings с claims/evidence и routing fact/attach/new-episode/continue. Legacy proposition совместима как один claim.
3. Stable candidate/episode key с exact source locators; titles редактируются отдельно. Pending эпизод сохраняет reviewed ranges и конкретный недостающий контекст.
4. В одном saved workflow выполнить bounded reconcile/compose; не перечитывать всю книгу вторым независимым проходом ради красивого текста.
5. Использовать existing angle/audience value, gaps, Variant blocks и assessments. Существенные временные/причинные связки тоже имеют основания.
6. Не переписывать одобренный variant при новом evidence; показать pending/review в текущей модели. Повтор того же operation не создаёт новую revision.
7. Исправить spoken rendering: понятное содержание с нужной краткой атрибуцией, dedup оговорок, полная мысль в budget, экранная bibliography. Exact quotes/offsets не менять ради пробелов.
8. Разногласия сохранять на уровне сопоставимых assertions, независимо от существования и художественного качества Story.

Готовые промпты импорта, композиции и самопроверки, синтетические before/after и все функции сохранения включены **в [единый файл кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md)**. При реализации обновить typed schemas/MCP descriptions и оба extraction paths одновременно; текстовый prompt поверх старого 1:1 контракта не закрывает дефект.

## Конкурентный аудит Opus 5.5

Task `dvt_29da98bf906e406cb8871f8c4e74b472`, actualModel `claude-opus-5-5`, provided_only, отдельная read-only консультация. [Полный ответ и disposition](rkb-opus55-story-quality-20261019.md).

| Подход | Итог |
|---|---|
| A: prompt, existing fields, renderer | Нужен и дешёв для подачи; не устраняет 1:1 activation и multi-page composition без изменения контракта. |
| B: один persisted workflow с отбором эпизода, bounded context и evidence-linked Variant | Минимально достаточный целевой путь поверх A. |
| C: многоагентная фабрика | Не принят: больше состояния, повторного чтения и конфликтов, без гарантии доказательности. |

Критические уточнения к Opus: не вводить обязательный change_or_choice; текущий standalone assertion путь ещё требуется реализовать; contested не readiness; canonical fact preservation не требует перечислить все числа в каждом коротком голосовом ответе; отсутствие упоминания не доказывает противоречие; private draft не равен общему Variant; retry не создаёт новую revision. Полный ответ сохранён без выдачи этих непроверенных предложений за нормативную спецификацию.

## Приёмка и нерешённое

Нужен небольшой фиксированный source-grounded набор, ориентир15–20 фрагментов, включая narrative, справочник, table/caption, everyday practice, multi-page и cross-book случаи. Часть оставить held-out. Владелец видит слепые A/B пары вместе с оригиналами и выбирает более полезный исследовательский материал/рассказ.

Проверять ложное создание/отказ story, split эпизода, потерю связи/metadata, неподтверждённую причинность/мотив/диалог, атрибуцию, реальные и ложные disagreements, retry/rename duplicates, readable spoken summary. Численный score интересности, длина и quota не нужны. Независимые retrieval/source-quality gates сохраняются.

Фактический A/B и переимпорт **не выполнены**. Dot получил вопрос о2–3 образцах удачных историй и предпочтительном основном представлении. Dot подтвердил, что это ещё предложение, не решение Максима. Пока рабочая гипотеза: досье с ясным кратким рассказом сверху, сохранив исходные facts/evidence. Нельзя обещать измеренный прирост качества до этого сравнения.
