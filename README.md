# de03-apache-kyuuby

## Prepare infrastructure
```bash
make build
make up
```

## Prepare MySQL data

```sql
# copy CSV data to mysql container
# cd path/to/brazilian-ecommerce/
docker cp brazilian-ecommerce/ de_mysql:/tmp/
docker cp mysql_schemas.sql de_mysql:/tmp/

# login to mysql server as root
make to_mysql_root
CREATE DATABASE brazillian_ecommerce;
USE brazillian_ecommerce;
GRANT ALL PRIVILEGES ON *.* TO admin;
SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';
SET GLOBAL LOCAL_INFILE=TRUE;
# exit

# run commands
make to_mysql

source /tmp/mysql_schemas.sql;
show tables;

LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_items_dataset.csv' INTO TABLE olist_order_items_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_payments_dataset.csv' INTO TABLE olist_order_payments_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_orders_dataset.csv' INTO TABLE olist_orders_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_products_dataset.csv' INTO TABLE olist_products_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/product_category_name_translation.csv' INTO TABLE product_category_name_translation FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

SELECT * FROM olist_order_items_dataset LIMIT 10;
SELECT * FROM olist_order_payments_dataset LIMIT 10;
SELECT * FROM olist_orders_dataset LIMIT 10;
SELECT * FROM olist_products_dataset LIMIT 10;
SELECT * FROM product_category_name_translation LIMIT 10;
```


# Prepare data PostgreSQL
```bash
make to_psql

create database metabaseappdb;
create database ecom_analytics;
```

# Prepare delta-table on warehouse Data lake
```sql
SHOW catalogs;

SHOW SCHEMAS FROM warehouse;

CREATE SCHEMA IF NOT EXISTS warehouse.bronze WITH (location='s3a://warehouse/bronze');
DROP table if EXISTS warehouse.bronze.mytable;
CREATE TABLE warehouse.bronze.mytable (name varchar, id integer);
INSERT INTO warehouse.bronze.mytable VALUES ( 'John', 1), ('Jane', 2);
SELECT * FROM warehouse.bronze.mytable;

CREATE SCHEMA IF NOT EXISTS warehouse.silver WITH (location='s3a://warehouse/silver');

-- https://docs.getdbt.com/reference/resource-properties/external
-- https://github.com/dbt-labs/dbt-external-tables
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
```

# Run DBT
```bash
cd ecom_analytics
make run_bronze

make run_external
make run_silver
make run_gold
```