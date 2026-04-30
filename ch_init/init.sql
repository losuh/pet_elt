-- Сначала создаем базы
CREATE DATABASE IF NOT EXISTS ext;
CREATE DATABASE IF NOT EXISTS dwh;

-- Потом создаем таблицу, явно указывая базу данных
CREATE TABLE IF NOT EXISTS ext.my_test_table (
    id UInt64,
    name String
) ENGINE = MergeTree() ORDER BY id;