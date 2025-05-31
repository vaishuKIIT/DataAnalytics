-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Source File Details
-- MAGIC CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
-- MAGIC
-- MAGIC JSON Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
-- MAGIC
-- MAGIC PARQUET Source  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
-- MAGIC
-- MAGIC
-- MAGIC ###### Spark Session Methods
-- MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">SparkSessionMethods</a> :**`read`**,**`write`**,  **`sql`** ,  **`table`** ,  **`createDataFrame`**
-- MAGIC
-- MAGIC ###### SQL On Files
-- MAGIC - <a href="https://spark.apache.org/docs/2.2.1/sql-programming-guide.html#run-sql-on-files-directly" target="_blank">DirectSQLOnFiles</a> :**`select`** ,**`view`**  ,**`temp view`** ,**`Common Table Expressions*CTE)`** , **`external Tables`**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storageAccountKey= <storageAccountKey>
-- MAGIC spark.conf.set("fs.azure.account.key.azuredatalakevrs.dfs.core.windows.net",storageAccountKey)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sourceCSVFilePath = 'abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/csv'
-- MAGIC sourceJSONFilePath = 'abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/json'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(sourceJSONFilePath)

-- COMMAND ----------

SELECT * FROM json.'abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/json/part-00000-tid-6367450538325107613-335a958b-70d4-4efd-bd66-752fe9e36386-128-1-c000.json'-- Check the path for correctness

-- COMMAND ----------

SELECT * FROM json.`abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/json/`

-- COMMAND ----------

CREATE VIEW daily_pricing_json_external_view_vrs AS
SELECT * FROM json.`abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/json/`

-- COMMAND ----------

SELECT COUNT(*) FROM daily_pricing_json_external_view_vrs

-- COMMAND ----------

DESCRIBE EXTENDED daily_pricing_json_external_view_vrs

-- COMMAND ----------

CREATE TABLE daily_pricing_json_external_table_vrs AS
SELECT * FROM json.`abfss://working-labs@azuredatalakevrs.dfs.core.windows.net/bronze/daily-pricing/json/`

-- COMMAND ----------

DROP TABLE daily_pricing_json_external_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(sourceJSONFilePath)
-- MAGIC

-- COMMAND ----------

CREATE TABLE daily_pricing_csv_external_table
(
DATE_OF_PRICING	string,
ROW_ID	bigint,
STATE_NAME	string,
MARKET_NAME	string,
PRODUCTGROUP_NAME	string,
PRODUCT_NAME	string,
VARIETY	string,
ORIGIN	string,
ARRIVAL_IN_TONNES	double,
MINIMUM_PRICE	string,
MAXIMUM_PRICE	string,
MODAL_PRICE	string
)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ","
)
LOCATION "${sourceCSVFilePath}"
