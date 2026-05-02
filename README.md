# Проект ELT: Citibike Data Pipeline

## 📋 Описание

Проект автоматизирует загрузку данных о поездках Citibike из Yandex Cloud Storage в MinIO, с последующей обработкой в ClickHouse.

## 🏗️ Архитектура

```
Yandex Cloud Storage (S3) 
         ↓
    Airflow DAG (Extract)
         ↓
      MinIO (Load)
         ↓
   ClickHouse (Transform)
```

## 🚀 Быстрый старт

### Требования
- Docker & Docker Compose
- Python 3.12+

### Установка

```bash
# Клонировать репозиторий
git clone <repository>
cd pet_elt

# Запустить контейнеры
docker-compose up -d

# Доступ к Airflow UI
# http://localhost:8080
# Логин: airflow / Пароль: airflow
```

## 📁 Структура проекта

```
pet_elt/
├── dags/
│   ├── s3_load_dag.py       # Загрузка данных из Yandex Cloud
│   └── s3_to_ch_dag.py      # Загрузка и трансформация данных в ClickHouse
├── docker-compose.yml       # Конфигурация сервисов
├── Dockerfile              # Образ Airflow
└── README.md
```

## 🔗 Подключения Airflow

| Название | Тип | Назначение |
|----------|-----|-----------|
| `citibike` | AWS S3 | Yandex Cloud Storage |
| `minio` | AWS S3 | Локальное s3 хранилище |
| `clickhouse` | ClickHouse | Локальный dwh |

## ⚙️ Сервисы

- **Airflow**: Оркестрация DAG-ов
- **PostgreSQL**: Метаданные Airflow
- **Redis**: Очередь задач
- **MinIO**: S3-совместимое хранилище
- **ClickHouse**: Аналитическое хранилище данных

## 📊 DAG-ы

### `st1_load_to_s3_dag`
Загрузка ZIP-файлов с данными из Yandex Cloud в MinIO

### `st2_load_to_clickhouse_dag`
Загрузка и трансформация данных в ClickHouse

## 📝 Логирование

Логи доступны в директории `/logs` и через UI Airflow.