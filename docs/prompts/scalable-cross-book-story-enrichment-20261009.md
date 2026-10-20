# Исторический prompt межкнижного обогащения, 09.10.2026

**Этот prompt выведен из активного использования 20.10.2026.** Передавать кодовому агенту следует [единый актуальный handoff](rkb-scalable-kaggle-implementation-20261019.md). Контракты текста и чтения — в [Story Registry](../story-registry.md), самостоятельных дел — в [дизайне разногласий](../design/rkb-disagreement-journal-design-20261018.md), deployment — в [архитектуре](../designs/rkb-scalable-kaggle-architecture-20261019.md).

[Полный неизменяемый прежний prompt на a8cf41b](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/prompts/scalable-cross-book-story-enrichment-20261009.md) сохранён для истории решений, включая прежние ограничения кода и приёмку. Он не является дополнительной инструкцией поверх handoff. Его SQLite/Supabase/E5 deployment, перенос прежних редакционных данных, per-book owner text ACL и выдача компактной аннотации вместо рассказа не применяются к новой реализации.

## Что сохраняется в актуальном задании

Сохраняются bounded межкнижный поиск по Stories и первоисточникам, точная атрибуция, source-family/independence, типы отношений и направленный phase_of, явный merge, исправление/отзыв/восстановление связей, assessments и курсоры, pair-decision dedup, angle/audience value, редакционные варианты, source-bound blocks, личные черновики, полный audit/readback, история дат/календаря и числовых observations, публикационные роли и внешние receipts. Протокол рассмотренного среза не обещает полноты всех будущих книг; semantic fallback не называется выполненным BGE-поиском.

Новое обязательное поведение: только GEO-перенос и fresh MCP imports; independent facts без принудительной Story; достаточный эпизод автоматически получает сохранённый читаемый basic narrative в existing Variant.body и exact default_variant_ref. Обычно 2–6 предложений, без жёсткого лимита, обязательного героя, драмы или вымышленной причинности. Недостаточные данные остаются facts/candidates с none/needs_context. Summary/rating/bibliography не заменяют body. Get/voice/read-export читают сохранённый текст; search отдаёт настоящий помеченный excerpt + body ref либо короткий body. Read-export отделён от approved publication export; imported shared basic draft наследует Story read scope, личный draft не продвигается автоматически.

Архитектура — единый PostgreSQL/pgvector на Kaggle, BGE-only, один writer и существующие queue/controller/outbox. Принятый активный книжный текст и bibliography ищутся без owner filter; иллюстрации публичны; scan_proof_read выдаёт страницу/полосу owner/demo/expert/local_historian, full PDF частный. Withdrawal, текущая revision, аудитория, expiry и права частных черновиков проверяются на чтении и доставке.

Наличие этого entry point не означает выполнение импорта, перенос данных, успешную приёмку либо внедрение новых MCP-контрактов.

Этот pinned архив используется как baseline в слепом сравнении 15–20 одинаковых свежих originals с новым контрактом: сравнивается actual saved body (его отсутствие отмечается явно), судит владелец/назначенный человек; без человеческого ответа human pilot NOT_RUN. Это разовая проверка изменения, не ручной gate каждой книги.
