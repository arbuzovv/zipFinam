# zipFinam

<a target="new" href="https://pypi.python.org/pypi/zipfinam"><img border=0 src="https://img.shields.io/badge/python-3.12+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/zipfinam"><img border=0 src="https://img.shields.io/pypi/v/zipfinam?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://pypi.python.org/pypi/zipfinam"><img border=0 src="https://img.shields.io/pypi/dm/zipfinam.svg?maxAge=2592000&label=installs&color=%2327B1FF" alt="PyPi downloads"></a>
<a target="new" href="https://github.com/arbuzovv/zipfinam"><img border=0 src="https://img.shields.io/github/stars/arbuzovv/zipfinam.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>

**zipFinam** — дополнение к библиотеке [ziplime](https://github.com/Limex-com/ziplime) для алгоритмической торговли и бэктестинга на российском рынке (MOEX) через gRPC API.

## Возможности

- Загрузка исторических данных по акциям MOEX через gRPC
- Поддержка таймфреймов от 1 минуты до квартала
- Параллельная загрузка данных по нескольким символам
- Полная интеграция с фреймворком ziplime

## Установка

```bash
pip install zipfinam
```

## Быстрый старт

Все готовые файлы находятся в папке [`quickstart/`](quickstart/):

```
quickstart/
├── .env.example            # шаблон конфигурации
├── step1_ingest_assets.py  # загрузка списка инструментов
├── step2_ingest_data.py    # загрузка исторических данных
├── algo.py                 # торговый алгоритм
└── step3_run_backtest.py   # запуск бэктеста
```

### 1. Настройте доступ к API

```bash
cd quickstart
cp .env.example .env
# отредактируйте .env — вставьте токен и адрес сервера
export $(cat .env | xargs)
```

### 2. Загрузите список инструментов

> Выполняется один раз. Повторять только при обновлении доступных инструментов.

```bash
python step1_ingest_assets.py
```

### 3. Загрузите исторические данные

По умолчанию: **SBER, LKOH, GAZP** за **2024 год**, дневной таймфрейм.

```bash
python step2_ingest_data.py
```

### 4. Запустите бэктест

```bash
python step3_run_backtest.py
```

Алгоритм [`algo.py`](quickstart/algo.py) реализует равновзвешенный портфель с ежедневной ребалансировкой.

---

### Что менять под себя

| Что изменить          | Где                                                     |
|-----------------------|---------------------------------------------------------|
| Список акций          | `SYMBOLS` в `step2_ingest_data.py` и `algo.py`         |
| Период бэктеста       | `start_date` / `end_date` в `step2` и `step3`          |
| Стартовый капитал     | `total_cash` в `step3_run_backtest.py`                  |
| Логика торговли       | `algo.py`                                               |

---

## Использование в коде

```python
from zipfinam import GrpcDataSource, GrpcAssetDataSource

# Из переменных окружения (GRPC_TOKEN, GRPC_SERVER_URL)
data_source = GrpcDataSource.from_env()
asset_source = GrpcAssetDataSource.from_env()

# Или напрямую
data_source = GrpcDataSource(
    authorization_token="ваш_токен",
    server_url="api.finam.ru:443",
)
```

## Конфигурация

| Переменная окружения   | Описание                           | Обязательная |
|------------------------|------------------------------------|:------------:|
| `GRPC_TOKEN`           | Токен авторизации gRPC API         | Да           |
| `GRPC_SERVER_URL`      | Адрес gRPC сервера (`host:port`)   | Да           |
| `GRPC_MAXIMUM_THREADS` | Макс. число параллельных запросов  | Нет          |

## Поддерживаемые таймфреймы

| Частота  | `data_frequency`                  |
|----------|-----------------------------------|
| 1 минута | `datetime.timedelta(minutes=1)`   |
| 5 минут  | `datetime.timedelta(minutes=5)`   |
| 1 час    | `datetime.timedelta(hours=1)`     |
| 1 день   | `datetime.timedelta(days=1)`      |
| 1 неделя | `datetime.timedelta(weeks=1)`     |

## Требования

- Python >= 3.12
- [ziplime](https://github.com/Limex-com/ziplime) >= 1.11.11

