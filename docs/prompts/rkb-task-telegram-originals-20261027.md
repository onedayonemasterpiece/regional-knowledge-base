# Задание: оригиналы книг — в Telegram topic «Книги исходники» напрямую из Kaggle

Заказчик: RKB-Импорт (Claude). Решение владельца 27.10: вернуть архив к исходному требованию, без отступлений.

## Цель для человека

Каждая загружаемая книга (PDF, DjVu) должна оказаться документом в Telegram-топике «Книги исходники» (topic 2), отправленным напрямую из Kaggle через Telethon. Сейчас с 20.10 оригинал «архивируется» только в Yandex S3, а статус `source_archive_status=verified` значит «объект в S3». В topic 2 последнее сообщение — от 18.10, хотя после этого загружено 8 оригиналов.

## Требования (нормативные)

- [owner alignment №4, №6, №7](../designs/rkb-owner-alignment-20261021.md): Telegram напрямую из Kaggle через Telethon — чтение и запись; без VibePublish и прокси через Dev; нельзя молча использовать только S3; Yandex — еженедельная резервная копия.
- [`docs/operations/telegram-archive.md`](../operations/telegram-archive.md): архивная идентичность — точный chat/topic/message/media locator; успех — доступный ожидаемый документ по locator, без повторного скачивания и сверки хешей.
- [AGENTS.md](../../AGENTS.md): «Target RKB archive upload and download go directly through Telethon in Kaggle».

## Доказательства (main `ffe178b0`)

- Архив оригинала — условный PUT в S3: `src/regional_knowledge/original_intake_controller.py:144-180`; документ `docs/operations/kaggle-native-original-intake.md:40`.
- `src/regional_knowledge/telegram_original.py` — только скачивание («never … sends messages»).
- Telethon-клиент и аренда сессии `TELEGRAM_RKB1` в live уже есть (`telegram_runtime=true` на run 174, `kaggle_target_runtime.py:43,121,513-515`), `telegram_runtime_binding.py`, `telegram_runtime_channel.py`.
- Факты Шкипера 27.10: бакет `rkb-corpus-…` (Yandex), 8 подтверждённых оригиналов (6 PDF, 2 DjVu) под `originals/v1/…`.

## Что сделать

1. В существующем приёме оригинала (`original_intake_controller` → native) после получения файла в Kaggle — **отправить его документом в настроенный chat/topic 2** тем же арендованным Telethon-клиентом. Зарегистрировать locator (chat, topic, message_id, document id) в существующей записи приёма с существующей семантикой UNKNOWN: неизвестный исход не отправлять вслепую повторно, а сверять по locator/подписи.
2. **«Архив проверен»** = есть доступный документ по зарегистрированному locator в нужном топике (presence-only). Без скачивания и сверки хешей.
3. **S3** остаётся только как еженедельная резервная копия; не основной архив и не условие `verified`. Если что-то в чтении страниц сейчас читает оригинал из S3 — читать из Telegram существующим `NativeTelegramOriginalDownloader` или временной рабочей копией в Kaggle. Решение обоснуй в PR.
4. **Перенос 8 существующих оригиналов** из S3 в topic 2: идемпотентная команда или скрипт (один документ на оригинал, повтор не дублирует, по каждой книге locator записывается в существующую запись). Запуск — Шкипер, после слияния.
5. Документация: `docs/operations/kaggle-native-original-intake.md` (раздел про архив) и `docs/operations/telegram-archive.md` — что реализовано.

## Чего нельзя

- Новые сервисы, прокси через Dev, VibePublish, второй загрузчик, новые очереди. Новые состояния — только если без них нельзя записать locator, с обоснованием.
- Скачивание после загрузки и сверка SHA с Telegram.
- Публикация оригиналов куда-либо, кроме настроенного приватного топика. Никаких секретов в коде и логах.
- Трогать live, Kaggle, запускать перенос, сливать PR.

## Бюджет

Production ≤ 150 строк. Скрипт переноса ≤ 80 строк. Если не укладывается — BLOCKED с расчётом, без урезания проверок.

## Один различающий тест

Синтетический приём PDF с поддельным Telethon-клиентом (существующие фикстуры): после приёма зарегистрирован locator topic 2, статус архива `verified` только при наличии locator. При отказе отправки статус не `verified`, при неизвестном исходе нет второй отправки. На текущем main тот же приём даёт `verified` без единого вызова Telegram.

## Живая проверка — Шкипер

После слияния (RKB-Opus) и установки: новый оригинал появляется в topic 2. Перенос 8 оригиналов — 8 документов в topic 2, у каждой книги locator. Владелец смотрит топик.

## Кто заметит

Владелец — в топике «Книги исходники» после каждой загрузки книги.

## Отчёт

Один draft-PR от свежего `main`, полный CI (Python 3.12/3.13 и envelope). STARTED / DONE / BLOCKED — в общий тред https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792868771439369 с подписью «Исполнитель TG-оригиналы (заказ RKB-Импорт)». Перед планом прочитай и соблюдай https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/skills/devserver-simplicity/SKILL.md . Не сливать, не трогать live.
