# Databricks notebook source
# MAGIC %md
# MAGIC <img src = "https://sguptasa.blob.core.windows.net/random/iiot_blog/end_to_end_architecture.png">
# MAGIC In this notebook we are going to Ingest, Process and Analyse IoT data
# MAGIC
# MAGIC Tasks:
# MAGIC * Stream real-time raw sensor data from Azure IoT Hubs into the Delta format in Azure Storage **(Data Ingestion)**
# MAGIC * Stream process sensor data from raw (Bronze) to silver (aggregated) to gold (enriched) Delta tables on Azure Storage **(Data Processing)**

# COMMAND ----------

# Configuration variables
dbutils.widgets.text("Storage Account", "dbstr4pos", "Storage Account")
dbutils.widgets.text("Event Hub Name", "iothub-ehub-db-iot-hub-24993086-0b83783c15", "Event Hub Name")

# COMMAND ----------

# DBTITLE 1,Setting Up Environment
# creadentials
iothub_cs = "Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15"
adls_key = "DefaultEndpointsProtocol=https;AccountName=dbstr4pos;AccountKey=DAL49gMkMur+B9cVcmTfGzAL/HtOPOOC3pOhEGwkJc9NS1wxYtIFnPEaDxNEd3kntYdqyR/U/fZP+AStCa3sfg==;EndpointSuffix=core.windows.net"

# COMMAND ----------

# Setting up access to storage account
# storage_account = dbutils.widgets.get("Storage Account")
# spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", dbutils.secrets.get("iot","adls_key"))

# Setup storage locations for all data
ROOT_PATH = "dbfs:/mnt/pos/IoT Data/"
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# COMMAND ----------

# Setting up IoT Hub
iothub_cs = "Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15"
ehConf = { 
  'eventhubs.connectionString':'Endpoint=sb://ihsuprodpnres003dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=0+WQRcCx5rdskmGhCGxlNSMsLoEL9b8NiJyfpFouKcQ=;EntityPath=iothub-ehub-db-iot-hub-24993086-0b83783c15',
  'ehName':'iothub-ehub-db-iot-hub-24993086-0b83783c15'
}

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

# COMMAND ----------

# DBTITLE 1,Importing Required Libraries
import os, json, requests
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

dbutils.fs.ls(ROOT_PATH)

# COMMAND ----------

# making sure root is empty
dbutils.fs.rm(ROOT_PATH, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleaning up tables & views
# MAGIC DROP TABLE IF EXISTS turbine_raw;
# MAGIC DROP TABLE IF EXISTS weather_raw;
# MAGIC DROP TABLE IF EXISTS turbine_agg;
# MAGIC DROP TABLE IF EXISTS weather_agg;
# MAGIC DROP TABLE IF EXISTS turbine_enriched;
# MAGIC DROP TABLE IF EXISTS turbine_power;
# MAGIC DROP TABLE IF EXISTS turbine_maintenance;
# MAGIC DROP VIEW IF EXISTS turbine_combined;
# MAGIC DROP VIEW IF EXISTS feature_view;
# MAGIC DROP TABLE IF EXISTS turbine_life_predictions;
# MAGIC DROP TABLE IF EXISTS turbine_power_predictions;

# COMMAND ----------

# DBTITLE 1,Data Ingest from IoT Hubs
# MAGIC %md
# MAGIC Azure Databricks provides a native connector to IoT and Event Hubs. Below, we will use PySpark Structured Streaming to read from an IoT Hub stream of data and write the data in it's raw format directly into Delta.
# MAGIC
# MAGIC We have two separate types of data payloads in our IoT Hub:
# MAGIC * **Turbine Sensor readings** - this payload contains date,timestamp,deviceid,rpm and angle fields
# MAGIC * **Weather Sensor readings** - this payload contains date,timestamp,temperature,humidity,windspeed, and winddirection fields
# MAGIC
# MAGIC We split out the two payloads into separate streams and write them both into Delta locations on Azure Storage. We are able to query these two Bronze tables immediately as the data streams in.

# COMMAND ----------

# schema of incoming data from iot hub
schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"

# read directly from IoT hub using the eventhubs library for databricks
iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
    .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
    .load()                                                                          # Load the data
    .withColumn('reading', F.from_json(F.col('body').cast('string'), schema))        # Extract the "body" payload from the messages
    .select('reading.*', F.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
)

# spliting our IoT Hub stream into separte streams and write them both into their own Dleta locations
write_turbine_to_delta = (
  iot_stream.filter('temperature is null')                                           # Filter out turbine telemetry from other data streams
    .select('date','timestamp','deviceId','rpm','angle')                             # Extract the fields of interest
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "turbine_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "turbine_raw")                                              # Stream the data into an ADLS Path
)

write_weather_to_delta = (
  iot_stream.filter(iot_stream.temperature.isNotNull())                              # Filter out weather telemetry only
    .select('date','deviceid','timestamp','temperature','humidity','windspeed','winddirection') 
    .writeStream.format('delta')                                                     # Write our stream to the Delta format
    .partitionBy('date')                                                             # Partition our data by Date for performance
    .option("checkpointLocation", CHECKPOINT_PATH + "weather_raw")                   # Checkpoint so we can restart streams gracefully
    .start(BRONZE_PATH + "weather_raw")                                              # Stream the data into an ADLS Path
)

# Create the external tables once data starts to stream in
while True:
  try:
    spark.sql(f'CREATE TABLE IF NOT EXISTS turbine_raw USING DELTA LOCATION "{BRONZE_PATH + "turbine_raw"}"')
    spark.sql(f'CREATE TABLE IF NOT EXISTS weather_raw USING DELTA LOCATION "{BRONZE_PATH + "weather_raw"}"')
    break
  except:
    pass

# COMMAND ----------


