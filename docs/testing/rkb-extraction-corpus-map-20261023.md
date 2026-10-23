# RKB — карта источников первого multi-source ПМИ (23.10.2026)

Это **список**, связывающий [реестр ПМИ](rkb-extraction-quality-registry-20261023.md) и [76 существующих кейсов](rkb-extraction-quality-cases-20261023.jsonl) с конкретными оригиналами. Он **не устанавливает условий импорта, релиза или новых критериев приёмки**; методика ПМИ и [защищённый `14](rkb-scalable-kaggle-acceptance-20261019.md) остаются без изменений.

| Источник (fixture ID) | Фиксированная редакция / oldid | Held-out families | Импортирован в RKB? |
|---|---|---|---|
| «Тевтонский орден» (`ruwiki-teutonic-order`), RU | [ru.wikipedia, oldid=154951202](https://ru.wikipedia.org/w/index.php?oldid=154951202&title=Тевтонский_орден) | **25**, EQ26-022–046 | **Нет** |
| *Hochmeister* (`dewiki-hochmeister`), DE | [de.wikipedia, oldid=270545055](https://de.wikipedia.org/w/index.php?oldid=270545055&title=Hochmeister) | **8**, EQ26-047–054 | **Нет** |
| *Deutschmeister (Deutscher Orden)* (`dewiki-deutschmeister`), DE | [de.wikipedia, oldid=250168686](https://de.wikipedia.org/w/index.php?oldid=250168686&title=Deutschmeister_(Deutscher_Orden)) | **5**, EQ26-055–059 | **Нет** |
| Ernst Wichert, *Schuster Lange* (`wichert-schuster-lange-1873`), DE | [Wikisource, oldid=3821109](https://de.wikisource.org/w/index.php?oldid=3821109&title=Schuster_Lange); *Die Gartenlaube* 1873, H.32–35, с.511–514, 527–530, 543–548, 559–565 | **21**, EQ26-001–021 | **Нет** |

«Нет» — состояние этих четырёх веб-оригиналов по последней подтверждённой передаче; в данном задании import/readback **не выполнялся**. Три статьи дают **38** held-out families (25+8+5); Wichert добавляет **21**, всего **59**. Исходные **76** кейсов не изменяются; native `document/revision/page/region/passage IDs` для этих четырёх пока `null`, все будущие измерения `NOT_RUN`. Это ограничивает лишь **будущий оцениваемый прогон ПМИ**, а не его подготовку, обычный импорт или релиз.

**PDF-статья идёт тем же постраничным импортом (`book_ingest(start) → book_pages → stage`), что PDF-книга.** Веб-ссылка `oldid` сама по себе не является PDF/DjVu-вложением; действующую проверку `kind != book` нельзя обходить ложным библиографическим типом. Отдельный импортёр или дополнительные функции каталога для ПМИ не требуются.

**Не смешивать источники:** Ernst **Wichert**, *Schuster Lange* (1873), — не Ernst **Wiechert** (OCR PDF 1975). Гаузе, русское издание 1994, — отдельная частично принятая книга (страницы **0–10**, последний подтверждённый статус), **не** одна из четырёх строк и не часть этих 59 families. Эта карта не изменяет действующие права доступа и авторские ограничения и **не разрешает** импорт или публикацию чужих материалов.
