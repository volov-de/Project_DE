# Проект: Создание витрины данных для аналитики товаров на маркетплейсе

## Описание проекта

Проект демонстрирует реализацию сквозного ETL/ELT-пайплайна для обработки данных о товарах маркетплейса и создания аналитической витрины. Основная цель — применение ключевых технологий Data Engineering (Apache Spark, Apache Airflow, Greenplum, S3) в связке для решения типовой задачи по построению аналитических отчетов.

---

## Техническое Задание (ТЗ)

### Цель

Разработать и внедрить пайплайн обработки данных, который на основе сырых данных о товарах (из Data Lake S3) создаст обогащенную витрину данных в аналитическом хранилище Greenplum. На основе этой витрины должны быть построены два аналитических представления (view).

### Входные данные

- **Источник:** `s3a://raw/raw_items` (формат: Parquet)
- **Описание полей входных данных:**

  | Поле                     | Тип      | Описание |
  |--------------------------|----------|----------|
  | sku_id                   | bigint   | Уникальный идентификатор товара |
  | title                    | string   | Название товара |
  | category                 | string   | Категория товара |
  | brand                    | string   | Название бренда |
  | seller                   | string   | Идентификатор продавца |
  | group_type               | string   | Группа товара |
  | country                  | string   | Страна производства |
  | availability_items_count | bigint   | Количество товара в наличии |
  | ordered_items_count      | bigint   | Общее количество заказанных единиц |
  | warehouses_count         | bigint   | Количество складов |
  | item_price               | bigint   | Цена товара |
  | goods_sold_count         | bigint   | Количество проданных единиц |
  | item_rate                | double   | Рейтинг товара |
  | days_on_sell             | bigint   | Дней на платформе |
  | avg_percent_to_sold      | bigint   | Средний процент выкупа |

### Результаты

#### 1. Отчет `seller_items` (Parquet в S3)

Обогащенный отчет, сохраненный в `s3a://volov/seller_items`. Включает все исходные поля и новые вычисляемые метрики:

| Поле                   | Тип      | Описание |
|------------------------|----------|----------|
| ... (все исходные поля) | ...      | ... |
| returned_items_count   | int      | Количество оформленных возвратов |
| potential_revenue      | bigint   | Потенциальный доход (остатки + заказы) |
| total_revenue          | bigint   | Доход с учетом возвратов |
| avg_daily_sales        | double   | Средние продажи в день |
| days_to_sold           | double   | Дней до продажи остатков |
| item_rate_percent      | double   | Процентиль рейтинга товара |

#### 2. Витрина `seller_items` (Внешняя таблица в Greenplum)

Создается внешняя таблица `"vj-volov".seller_items` в Greenplum, указывающая на Parquet-файл в S3 через PXF.

#### 3. View `unreliable_sellers_view` (Greenplum)

Аналитическое представление для выявления ненадежных продавцов.

| Поле                         | Тип    | Описание |
|------------------------------|--------|----------|
| seller                       | TEXT   | Продавец |
| total_overload_items_count   | BIGINT | Общее количество остатков |
| is_unreliable                | BOOL   | Признак ненадежности (дней > 100 и остатки > заказы) |

#### 4. View `item_brands_view` (Greenplum)

Агрегированный отчет по брендам.

| Поле              | Тип    | Описание |
|-------------------|--------|----------|
| brand             | TEXT   | Бренд |
| group_type        | TEXT   | Группа товаров |
| country           | TEXT   | Страна |
| potential_revenue | FLOAT8 | Суммарный потенциальный доход |
| total_revenue     | FLOAT8 | Суммарный фактический доход |
| items_count       | BIGINT | Количество позиций |

---

## Используемые технологии

- **Data Lake:** VK Cloud Object Storage (S3)
- **Обработка данных:** Apache Spark (на Kubernetes)
- **Оркестрация:** Apache Airflow
- **Хранилище данных:** Greenplum
- **Интеграция:** PXF (для внешних таблиц Greenplum -> S3)
- **Управление кодом:** Git (GitLab)

---

## Реализация

### 1. Spark Job (`items-spark-job.py`)

- **Чтение:** Данные читаются из `s3a://raw/raw_items`.
- **Обработка:**
  - Расчет новых метрик (`returned_items_count`, `potential_revenue`, `total_revenue`, `avg_daily_sales`, `days_to_sold`, `item_rate_percent`).
  - Приведение всех типов данных к схеме витрины.
- **Запись:** Результат записывается в Parquet-файл по адресу `s3a://volov/seller_items`.

### 2. Airflow DAG (`project-volov-dag.py`)

DAG `project-volov-dag` выполняет весь пайплайн последовательно и параллельно, где это возможно.

**Этапы DAG:**

1. **`start`** (EmptyOperator) - Начало.
2. **`job_submit`** (SparkKubernetesOperator) - Запуск Spark-приложения через манифест `spark_submit.yaml`.
3. **`job_sensor`** (SparkKubernetesSensor) - Ожидание завершения Spark-задачи.
4. **`items_datamart`** (SQLExecuteQueryOperator) - Создание внешней таблицы `seller_items` в Greenplum.
5. **`create_unreliable_sellers_report_view`** (SQLExecuteQueryOperator) - Создание представления `unreliable_sellers_view`.
6. **`create_brands_report_view`** (SQLExecuteQueryOperator) - Создание представления `item_brands_view`.
7. **`end`** (EmptyOperator) - Завершение.

### 3. Kubernetes Манифест (`spark_submit.yaml`)

Описывает запуск Spark-приложения в кластере Kubernetes, включая:
- Клонирование репозитория с кодом.
- Запуск скрипта `items-spark-job.py`.

---

## Результаты и выводы

Проект успешно демонстрирует:

- Построение современного ETL/ELT-пайплайна с использованием промышленных инструментов.
- Интеграцию между Data Lake (S3), вычислительным движком (Spark), оркестратором (Airflow) и хранилищем данных (Greenplum).
- Навыки работы с форматом Parquet, внешними таблицами и аналитическими представлениями.