# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_path)

from utils.transformations import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **AutoLoader**

# COMMAND ----------

# Streaming Read using Auto Loader
df_user = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimUser/checkpoint")
    .option("schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@storageazureprojecthim.dfs.core.windows.net/DimUser"))

# COMMAND ----------

# Apply reusable class transformations
df_user_obj = reusable()

df_user = df_user.withColumn("user_name",upper(col("user_name")))
df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])
df_user = df_user.dropDuplicates(["user_id"])

# COMMAND ----------

(df_user.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimUser/checkpoint")
    .trigger(once=True)
    .option("path","abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimUser/data")
    .toTable("spotify_cata.silver.DimUser"))

# COMMAND ----------

df_silver = (spark.read
    .format("delta")
    .load("abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimUser/data"))
display(df_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

# Streaming Read using Auto Loader
df_art = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimArtist/checkpoint")
    .option("schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@storageazureprojecthim.dfs.core.windows.net/DimArtist"))

# COMMAND ----------

# Apply reusable class transformations
df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art, ["_rescued_data"])
df_art = df_art.dropDuplicates(["artist_id"])

# COMMAND ----------

df_art.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

df_silver = (spark.read
    .format("delta")
    .load("abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimArtist/data"))
display(df_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

# Streaming Read using Auto Loader
df_track = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimTrack/checkpoint")
    .option("schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@storageazureprojecthim.dfs.core.windows.net/DimTrack"))

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col("duration_sec") <150, "low").when(col("duration_sec") < 300, "medium").otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), "-", " "))

df_track = reusable().dropColumns(df_track, ["_rescued_data"])


# COMMAND ----------

df_track.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

df_silver = (spark.read
    .format("delta")
    .load("abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimTrack/data"))
display(df_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**

# COMMAND ----------

# Streaming Read using Auto Loader
df_date = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimDate/checkpoint")
    .option("schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@storageazureprojecthim.dfs.core.windows.net/DimDate"))

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ["_rescued_data"])

df_date.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")



# COMMAND ----------

df_silver = (spark.read
    .format("delta")
    .load("abfss://silver@storageazureprojecthim.dfs.core.windows.net/DimDate/data"))
display(df_silver.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

# Streaming Read using Auto Loader
df_fact = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/FactStream/checkpoint")
    .option("schemaEvolutionMode", "addNewColumns")
    .load("abfss://bronze@storageazureprojecthim.dfs.core.windows.net/FactStream"))

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ["_rescued_data"])

df_fact.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@storageazureprojecthim.dfs.core.windows.net/FactStream/data")\
    .table("spotify_cata.silver.FactStream")

# COMMAND ----------

df_silver = (spark.read
    .format("delta")
    .load("abfss://silver@storageazureprojecthim.dfs.core.windows.net/FactStream/data"))
display(df_silver.limit(20))

# COMMAND ----------

