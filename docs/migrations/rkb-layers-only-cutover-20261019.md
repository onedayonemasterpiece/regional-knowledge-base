# Перенос исторических и картографических слоёв; книги через fresh MCP import

Дата: 2026-10-19. Статус: план, данные не переносились этой документационной задачей.

Основной [файл кодовому агенту](../prompts/rkb-scalable-kaggle-implementation-20261019.md) самодостаточен. Ниже сохранён его операционный контракт переноса; read-only inventory и isolated dry-run предшествуют переключению production.

## 12. Перенос слоёв и запуск с заново импортируемыми книгами

### 12.1. Manifest переносимых данных

До переноса создать проверяемый inventory: категории, canonical owner, count, IDs, hashes/versions, dependencies и ожидаемый target. Не предполагать, что «карта» — только изображение.

Обязательны:
- historical map/layer IDs, raster/vector sources, CRS, axis order, units, geometry, georeferencing points/transform, validity periods и исходные объяснения датировки;
- топонимы/aliases/языки/периоды, historical address relations, source-specific observations, unresolved states;
- provenance, source reference/edition/SHA, physical/printed page, exact locators и авторство ручной интерпретации;
- подтверждённые external POI IDs/refs с сохранённым владельцем Street Story и Cartography;
- необходимая ручная работа/история зависимостей: сначала сохранить inventory/export, active transfer только в согласованной области.

Старые book text/chunks/vectors/import receipts в target не попадают. Минимальные старые bibliography/hash/page/archive refs, нужные слою или ручному доказательству, сохраняются как **legacy_unanchored** историческая ссылка, а не как принятая новая книга или searchable corpus.

После fresh import reanchor разрешён только при совпадающем оригинале/издании и проверенном evidence mapping. Тот же title/page number, близкая цитата или похожее имя недостаточны. Другой edition/scan/offset остаётся visibly unanchored до проверки. Не менять старое ручное заключение так, будто автор видел новую revision.

### 12.2. Порядок cutover

1. Export/inventory и restore dry-run слоёв на изолированной БД; проверить counts, IDs, geometry/provenance и реальные lookup/overlay сценарии.
2. Isolated A+B fresh book import/research/story/case сценарий можно проверять отдельно от всей карты.
3. Для production bootstrap **сначала устойчивые layer/place identities**, затем новые книги и их ссылки. Не импортировать заново уже принятые в target книги только потому, что позднее приехал большой layer export.
4. Перед финальным переносом согласовать короткую остановку layer writes или доказуемый delta watermark. Не обещать delta, если источник его не поддерживает. Зафиксировать, какие внешние canonical stores вообще не копируются.
5. Переключить routing/readiness после проверенного snapshot/replay и applicable gates. Старое состояние сохранить как обратимый reference, а не вторую live write authority.
6. Rollback до новых записей может переключить маршрут к прежнему состоянию. **После новых подтверждённых вкладов** rollback обязан сохранить и replay target tail. Восстановить старую целую SQLite поверх новых работ или заменить target пустой БД нельзя.
7. Удаление старых копий/ручных объектов — отдельный последующий шаг после inventory и проверенного cutover, не часть данного документационного изменения.

Неопределённые смешанные manual stories, drafts, expert contributions и слойные evidence не уничтожать и не переносить молча в public search. Сохранить export и список решений по mapping. Это ограниченный открытый вопрос владельцу, а не причина отложить всю реализацию.


## Проверка manifest

Для каждой категории указать source owner/store, count и устойчивые IDs, export SHA/revision, dependencies, target owner/store, transform/CRS/axis/units/time, provenance и статусы migrated/preserved_unanchored/left_with_owner/pending_decision. Статусы не должны скрывать потерянные manual records.

Минимальные readback: выбор реального historical layer; lookup известного/неразрешённого места; отрисовка геометрии; чтение provenance и авторства; historic alias по времени; external POI reference без дубликата; legacy evidence с правильным неизвестным anchor; fresh source reanchor только по подтверждённому оригиналу.

Согласованный scope слоёв не даёт права менять Cartography/Street Story canonical IDs или переносить чужие базы. Изменения API владельцев реализуются отдельно через их обычные контракты.

[Приёмка MIG/REC/IMP](../prompts/rkb-scalable-kaggle-implementation-20261019.md#14-приёмка-обязательные-численные-gates-и-конкретные-отказы) включает конкурирующие записи, failed snapshot, неточный reanchor и rollback после нового вклада. Единственный base snapshot не разрешает удалить accepted хвост или заменить новую базу старой.
