##  Таск 1: Использование Yandex DataTransfer для выгрузки данных

###  Выполненные действия:

1. **Подготовка структуры в YDB**  
   Была вручную создана таблица в базе данных YDB.  
   *(SQL-запрос в migration.sql)*

2. **Импорт данных в таблицу**  
   С помощью утилиты командной строки (CLI) в таблицу загружен CSV-файл `transactions`.  
   *(Скрипт импорта — load)*

3. **Настройка потока передачи данных**  
   Настроен DataTransfer между YDB (в роли источника) и Object Storage (в роли приёмника).  
   Результирующие данные экспортируются по адресу:  
   `s3a://etl-data-transform/transactions.parquet`  
   *(Скриншоты конфигурации трансфера доступны в приложении)*

---

## Таск 2: Автоматизация обработки данных через Apache Airflow и Yandex Data Proc

###  Инфраструктурная часть:
- Задействован Managed-сервис от Yandex Cloud для запуска Apache Airflow.
- Также подготовлены сервисные аккаунты, объектное хранилище и кластер Data Proc.

### Описание DAG-процесса `DATA_INGEST`:

- **Шаг 1:** Запуск временного Data Proc кластера.
- **Шаг 2:** Выполнение скрипта на PySpark, который:
  - Загружает данные из `transactions.parquet`
  - Приводит типы полей к нужным (`String`, `Int`, `Boolean`, `Date`)
  - Преобразует поля дат (`transaction_date`, `membership_expire_date`) из строкового формата `yyyyMMdd` в тип `Date`
  - Удаляет строки, содержащие пропущенные значения
  - Сохраняет очищенный датафрейм в формате Parquet по пути:  
    `s3a://etl-data-transform/transactions_сlean.parquet`

- **Шаг 3:** Завершает работу, удаляя кластер.

### Вложения:
- Код DAG-а (Airflow): data-proc.py
- Код PySpark-обработки: clean.py
