# Databricks notebook source
# MAGIC %md
# MAGIC ##Ugly Import Work - Start with CSV

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from openpyxl import load_workbook

# Loading an Excel workbook
wb = load_workbook('/Volumes/azcinnover_adb_dev/excel_import/raw_data/Propeller_Data.xlsx', data_only=True, read_only=True)

# COMMAND ----------

for w in wb: print (w.title)

# COMMAND ----------

data = wb["Shopify Order Data"].values
columns = next(data)  # Assumes the first row is the header
#df = pd.DataFrame(data, columns=list[columns], dtype=str)

# COMMAND ----------

data = wb["7.01-12.12 Shipbob Orders Updat"].values

# COMMAND ----------

ndata = [w for w in wb["7.01-12.12 Shipbob Orders Updat"].values]

# COMMAND ----------

newdata = [[x for x in n] for n in ndata[1:]]

# COMMAND ----------

columns = next(data)

# COMMAND ----------

coll = list(columns)

# COMMAND ----------

coll

# COMMAND ----------

import joblib
joblib.dump(coll, '/Volumes/azcinnover_adb_dev/excel_import/raw_data/coll2.pkl')

# COMMAND ----------

joblib.dump(newdata, '/Volumes/azcinnover_adb_dev/excel_import/raw_data/newdata2.pkl')

# COMMAND ----------

# MAGIC %fs ls /Volumes/azcinnover_adb_dev/excel_import/raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hack around Memory Issue

# COMMAND ----------


newdata = joblib.load('/Volumes/azcinnover_adb_dev/excel_import/raw_data/newdata2.pkl')

# COMMAND ----------

coll = joblib.load('/Volumes/azcinnover_adb_dev/excel_import/raw_data/coll2.pkl')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = [(l,StringType()) for l in coll]

# COMMAND ----------

schema

# COMMAND ----------

schema = [(col_name if col_name is not None else 'None', col_type) for col_name, col_type in schema]

nschema = StructType([
    StructField(col_name, col_type, True) 
    for col_name, col_type in schema
])

# COMMAND ----------

coll[8] = 'Payment Reference_'

# COMMAND ----------

coll

# COMMAND ----------

sdf = spark.createDataFrame(newdata, nschema)

# COMMAND ----------

# import pyspark.pandas as pd
# df = pd.DataFrame(newdata, columns=coll, dtype=str)

# COMMAND ----------

#import pyspark.pandas as pd

# Importing an Excel file
#df = pd.read_excel('/Volumes/azcinnover_adb_dev/excel_import/raw_data/Propeller_Data.xlsx')

# COMMAND ----------

#sdf = df.to_spark()

# COMMAND ----------

sdf.write.mode("overwrite").format("delta").option("delta.columnMapping.mode", "name").saveAsTable("azcinnover_adb_dev.excel_import.Shipbob_order_data", )

# COMMAND ----------

size(newdata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azcinnover_adb_dev.excel_import.shopify_order_data limit 100

# COMMAND ----------

#df.to_csv("/Volumes/azcinnover_adb_dev/excel_import/raw_data/shopify_order_data.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE TABLE azcinnover_adb_dev.excel_import.shopify_order_data AS
# MAGIC --SELECT * from csv.`/Volumes/azcinnover_adb_dev/excel_import/raw_data/shopify_order_data.csv` LIMIT 100

# COMMAND ----------

type(sdf)

# COMMAND ----------

##display(sdf)

# COMMAND ----------

#dbutils.data.summarize(sdf.head(100), precise=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC use azcinnover_adb_dev.excel_import;
# MAGIC select * from Shipbob_order_data limit 100

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

ship = set(spark.table("Shipbob_order_data").columns)
shop = set(spark.table("shopify_order_data").columns)

# COMMAND ----------

ship & shop

# COMMAND ----------

dbutils.data.summarize(spark.table("shopify_order_data").take(1000), True)

# COMMAND ----------

spark.table("shopify_order_data").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes from Excel
# MAGIC
# MAGIC ```
# MAGIC Shopify Lookup=+_xlfn.XLOOKUP(1,('Shopify Order Data'!P:P=D87340)*('Shopify Order Data'!AK:AK=P87340),'Shopify Order Data'!AJ:AJ,"")
# MAGIC ```
# MAGIC * AJ = Lineitem quantity (base updated)   
# MAGIC * AK= Lineitem sku (base updated)
# MAGIC * 'Shopify Order Data'!P😝=”Name”
# MAGIC * D87340=”Store Order ID”
# MAGIC * P87340=”CIN7 SKU”
# MAGIC
# MAGIC  
# MAGIC ```
# MAGIC Variance=+IF(A8="","",S8-A8)
# MAGIC ```
# MAGIC  
# MAGIC
# MAGIC * S8= Line Item Qty
# MAGIC * A8= Shopify Lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE azcinnover_adb_dev.excel_import;
# MAGIC
# MAGIC ALTER VIEW order_ship AS 
# MAGIC   (
# MAGIC       SELECT 
# MAGIC       ship_date `Ship Date`
# MAGIC       ,`Source`
# MAGIC       ,`Order Status`
# MAGIC       ,`Line Item Name`
# MAGIC       ,`CIN7 SKU` as Name_ship
# MAGIC       ,`Store Order ID` as Order_ship
# MAGIC       , `Line Item Qty` as Quantity_ship
# MAGIC       FROM 
# MAGIC       azcinnover_adb_dev.excel_import.Shipbob_order_data)
# MAGIC ;
# MAGIC ALTER VIEW order_shop AS 
# MAGIC   (
# MAGIC       SELECT 
# MAGIC       `Financial`
# MAGIC       ,fulfilled_date as `Fulfilled Date`
# MAGIC       ,`Lineitem quantity`
# MAGIC       ,`Lineitem name`
# MAGIC       ,`Lineitem sku`
# MAGIC       ,`Lineitem sku (base updated)`
# MAGIC       ,cancelled_at as `Cancelled at`
# MAGIC       ,`Source`
# MAGIC       ,`Name` as Order_shop
# MAGIC       ,`Lineitem sku (base updated)` as Name_shop
# MAGIC       ,`Lineitem quantity (base updated)` as Quantity_shop
# MAGIC       FROM 
# MAGIC       azcinnover_adb_dev.excel_import.shopify_order_data
# MAGIC   )  

# COMMAND ----------

# MAGIC %sql
# MAGIC select `item mapping`, count(*)
# MAGIC  FROM 
# MAGIC       azcinnover_adb_dev.excel_import.shopify_order_data
# MAGIC       group by `item mapping`

# COMMAND ----------

# MAGIC %sql
# MAGIC use  azcinnover_adb_dev.excel_import;
# MAGIC select Fulfillment, count(*) from shopify_order_data 
# MAGIC where `Fulfilled Date` IS NOT NULL
# MAGIC group by Fulfillment

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matching Orders in Shopify and ShipBob
# MAGIC SELECT COUNT(*) FROM order_ship i
# MAGIC join order_shop o
# MAGIC ON i.Name_ship = o.Name_shop
# MAGIC AND i.Order_ship = o.Order_shop

# COMMAND ----------

# MAGIC %sql
# MAGIC --Orders in Shopify not in ShipBob
# MAGIC SELECT * FROM order_ship i
# MAGIC left anti join order_shop o
# MAGIC ON i.Order_ship = o.Order_shop

# COMMAND ----------

# MAGIC %sql
# MAGIC --Orders in Shopify not in ShipBob
# MAGIC SELECT COUNT(*) FROM order_ship i
# MAGIC left anti join order_shop o
# MAGIC ON i.Name_ship = o.Name_shop
# MAGIC AND i.Order_ship = o.Order_shop

# COMMAND ----------

# MAGIC %sql
# MAGIC --Orders not in Shopify but in ShipBob
# MAGIC SELECT COUNT(*) FROM order_shop o
# MAGIC left anti join order_ship i
# MAGIC ON i.Name_ship = o.Name_shop
# MAGIC AND i.Order_ship = o.Order_shop

# COMMAND ----------

# MAGIC %sql
# MAGIC create view shop_ship_comparison
# MAGIC as
# MAGIC
# MAGIC  SELECT
# MAGIC  i.quantity_ship - o.quantity_shop as variance 
# MAGIC   FROM order_ship i
# MAGIC join order_shop o
# MAGIC ON i.Name_ship = o.Name_shop
# MAGIC AND i.Order_ship = o.Order_shop
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH order
# MAGIC AS
# MAGIC (
# MAGIC   SELECT o.order_shop
# MAGIC   ,o.name_shop
# MAGIC   ,SUM(o.quantity_shop) quantity_shop_t
# MAGIC   from order_shop o
# MAGIC   GROUP BY o.order_shop
# MAGIC   ,o.name_shop
# MAGIC )
# MAGIC ,
# MAGIC ship
# MAGIC AS
# MAGIC (
# MAGIC   SELECT  o.order_ship
# MAGIC   ,o.name_ship
# MAGIC   ,SUM(o.quantity_ship) quantity_ship_t
# MAGIC   from order_ship o
# MAGIC     GROUP BY o.order_ship
# MAGIC   ,o.name_ship
# MAGIC )
# MAGIC , base
# MAGIC AS (
# MAGIC  SELECT
# MAGIC  i.quantity_ship_t - o.quantity_shop_t as variance 
# MAGIC   FROM ship i
# MAGIC join shop o
# MAGIC ON 
# MAGIC i.Name_ship = o.Name_shop
# MAGIC AND 
# MAGIC i.Order_ship = o.Order_shop
# MAGIC )
# MAGIC select variance, count(*) as occurences
# MAGIC FROM base 
# MAGIC group by variance
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order AS (
# MAGIC   SELECT 
# MAGIC     o.order_shop,
# MAGIC     o.name_shop,
# MAGIC     SUM(o.quantity_shop) AS quantity_shop_t
# MAGIC   FROM order_shop o
# MAGIC   GROUP BY o.order_shop, o.name_shop
# MAGIC ),
# MAGIC ship AS (
# MAGIC   SELECT 
# MAGIC     o.order_ship,
# MAGIC     o.name_ship,
# MAGIC     SUM(o.quantity_ship) AS quantity_ship_t
# MAGIC   FROM order_ship o
# MAGIC   GROUP BY o.order_ship, o.name_ship
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT
# MAGIC     i.quantity_ship_t - o.quantity_shop_t AS variance
# MAGIC   FROM ship i
# MAGIC   JOIN order o
# MAGIC   ON i.name_ship = o.name_shop
# MAGIC   AND i.order_ship = o.order_shop
# MAGIC )
# MAGIC SELECT 
# MAGIC   variance, 
# MAGIC   COUNT(*) AS occurrences
# MAGIC FROM base 
# MAGIC GROUP BY variance

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order AS (
# MAGIC   SELECT 
# MAGIC     o.order_shop,
# MAGIC     o.name_shop,
# MAGIC     SUM(o.quantity_shop) AS quantity_shop_t
# MAGIC   FROM order_shop o
# MAGIC   GROUP BY o.order_shop, o.name_shop
# MAGIC ),
# MAGIC ship AS (
# MAGIC   SELECT 
# MAGIC     o.order_ship,
# MAGIC     o.name_ship,
# MAGIC     SUM(o.quantity_ship) AS quantity_ship_t
# MAGIC   FROM order_ship o
# MAGIC   GROUP BY o.order_ship, o.name_ship
# MAGIC ),
# MAGIC base AS (
# MAGIC   SELECT *,
# MAGIC     i.quantity_ship_t - o.quantity_shop_t AS variance
# MAGIC   FROM ship i
# MAGIC   JOIN order o
# MAGIC   ON i.name_ship = o.name_shop
# MAGIC   AND i.order_ship = o.order_shop
# MAGIC )
# MAGIC SELECT *
# MAGIC
# MAGIC FROM base 
# MAGIC where variance != 0

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH base
# MAGIC AS (
# MAGIC  SELECT
# MAGIC  i.quantity_ship - o.quantity_shop as variance 
# MAGIC   FROM order_ship i
# MAGIC join order_shop o
# MAGIC ON 
# MAGIC i.Name_ship = o.Name_shop
# MAGIC AND 
# MAGIC i.Order_ship = o.Order_shop
# MAGIC )
# MAGIC select variance, count(*) as occurences
# MAGIC FROM base 
# MAGIC group by variance
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base
# MAGIC AS (
# MAGIC  SELECT
# MAGIC  i.quantity_ship - o.quantity_shop as variance 
# MAGIC   FROM order_ship i
# MAGIC join order_shop o
# MAGIC ON 
# MAGIC --i.Name_ship = o.Name_shop AND 
# MAGIC i.Order_ship = o.Order_shop
# MAGIC )
# MAGIC select variance, count(*) as occurences
# MAGIC FROM base 
# MAGIC group by variance

# COMMAND ----------


