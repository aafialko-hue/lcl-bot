# Final package: Suzhou Jiangsu → Moscow Telegram Bot

## Что внутри
- `bot.py` — финальный бот
- `Suzhou_Bot_Final_Model.xlsx` — обновленный Excel
- `rates.xlsx` — копия Excel для бота
- `requirements.txt`
- `.env.example`
- `Procfile` и `Dockerfile` — для запуска на Railway/Render

## Что исправлено
- Автодоставка считается даже если в Excel USD-ячейки содержат формулы
- Кнопка `Заказать` после расчета
- После `Заказать` бот спрашивает:
  - имя
  - e-mail
- Заказ и расчет отправляются на `sales@gfeng.ru`
- Города авто отображаются в формате `English / 中文`
- Поддержан ручной ввод города авто, если кнопка не нажата

## Команды клиента
- `/start`
- `/calc`
- `/rates`
- `/cities`

## Админ-команды
- `/setcontainer 7800 68`
- `/setcube 1-5 170`
- `/setdocs 150`
- `/setagent 200`
- `/setpickup 250 350`
- `/setautousd Shanghai 45 60`
- `/setautocny Shanghai 320 280`
- `/showauto Shanghai`

## Логика автодоставки
`MAX(расчетный_объем × USD/м³; вес_т × USD/т) + комиссия агента + pickup charge`

Если USD в строке авто не заданы числом, бот берет CNY-ставки и переводит их по курсу из `Controls`.

## Запуск локально
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python bot.py
```

## Настройка e-mail
Заполните SMTP-переменные в `.env`:
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `SMTP_FROM`

После этого заказы будут уходить на `ORDER_EMAIL_TO`, по умолчанию `sales@gfeng.ru`.

## Запуск без компьютера
### Railway
1. Создайте новый проект
2. Загрузите содержимое этой папки
3. Добавьте переменные окружения из `.env.example`
4. Стартовая команда: `python bot.py`

### Render
1. Создайте Background Worker
2. Подключите репозиторий с этими файлами
3. Build command: `pip install -r requirements.txt`
4. Start command: `python bot.py`

## Важно
Перевыпустите токен бота в BotFather, если старый токен попадал в лог или на скриншот.
