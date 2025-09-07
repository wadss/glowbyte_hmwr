# glowbyte_hmwr
Тестовое задание в компанию GlowByte

**Как стартовать:**
Склонировать репозиторий на локальный компьютер.
Развернуть виртуальное окружение Python:
```
python3 -m venv venv
```

Создать файл .env в котором должны быть следующие поля с заполненными значениями:
```
AIRFLOW_UID= (свой UID можно узнать выполнив команду ls -l в bash, он находится в 3 столбце)
CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
```

Далее следует установить все необходимые зависимости из файла requirements.txt
```
pip install requirements.txt
```

Выполнить команду инициализации контейнеров:
```
docker compose up airflow-init
```

Поднять контейнеры:
```
docker compose up -d
```

После запуска контейнеров необходимо последовательно для каждой базы данных выполнить SQL-скрипты из файла init_scripts.sql

# Описание проекта:
В данном проекте реализован ETL по выгрузке данных из Greenplume в Clickhouse с реализацией SCD 4.
В таблице orders_story в Clickhouse хранятся все версии строк из исходной системы Greenplume.
В витрине orders_mart содержаться только актуальные записи заказов.
