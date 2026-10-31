# Задание: native_text в book_pages — черновой OCR (решение Продукта А3, 30.10)

## Зачем
Brünneck (`Brünneck_W_…_Westpreussen.pdf`, https://t.me/c/4368830579/120) содержит текстовый слой PDF с ошибками OCR.
Окно получает `native_text` прямо в ответе `book_pages` и правит его вместо дословного чтения изображения.
Принятый текст повторяет ошибки слоя: «Besizers» вместо «Beſitzers», пробелы перед запятыми («fand ,»), разрывы по слову на строку (с. 236, 280).
Это нарушение правила дословности (PR 561).

Решение Продукта 30.10 22:50 UTC — вариант (б): одна фраза в описании `book_pages`.
Окна видят только описания инструментов, а не `instructions=` сервера (так было с PR 652/658).

## Изменение (одно)
`src/regional_knowledge/server.py`, описание `book_pages` (на main d104c675 строка ~1842):
после фразы `"Native extraction never proves visual completeness. "` добавить:

```
"native_text is a draft OCR layer: take every word, ſ/tz and other letter forms, punctuation and line breaks from the issued image, never from native_text. "
```

Больше ничего не менять. Новых полей, режимов и проверок нет.
`server.py` не входит в 13 файлов `materializer_fingerprint()`, notebook push не нужен.
Если описание проверяется тестом, обновить этот тест.

## Установка
API-only установка вместе с ближайшей. После неё Chrome Refresh tools, иначе окна не увидят новое описание. Chrome сейчас offline.

## Приёмка (Импорт)
- На следующих 20 принятых страницах Brünneck нет «Besizer» и пробелов перед запятыми.
- Скорость ≥ 35 стр/ч.
- Если ошибки останутся — вариант (в): `native_text` окнам не отдаём.
- Уже принятые страницы не переимпортируем, это долг.

Исполнитель: РКБ-Шкипер (единственный RKB WT). Ревью/merge: RKB-Opus.
