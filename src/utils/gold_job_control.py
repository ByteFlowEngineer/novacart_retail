from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import (StructType,StructField,StringType,TimestampType,LongType)


"""
    This Function will upsert the gold tables
"""

def upsert_to_gold(spark,df_source,target_table,join_key):
    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark,target_table)
        dt.alias("target") \
          .merge(df_source.alias("source"),f"target.{join_key} = source.{join_key}") \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
    else:
        df_source.write.format("delta").mode("append").saveAsTable(target_table)
   


"""
    This Function will return the last succesfully processed silver rows from the silver control table
"""

def get_last_processed_silver_ts(spark,table_name):
    ctrl = ( 
          spark.table("novacart_catalog.audit.gold_processing_control") 
              .filter(
                    (F.col("layer") == "gold") &
                    (F.col("table_name") == table_name) &
                    (F.col("run_status") == "success")
                    ) 
              .orderBy(F.col("updated_at").desc()) 
              .limit(1)
        )
    rows = ctrl.collect()
    if not rows:
        return None
    else:
        return rows[0]['last_processed_silver_ingested_at']

    
    


"""
    This Function wll upsert the gold control table
"""


schema = StructType([
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("last_processed_silver_run_id", StringType(), False),
    StructField("last_processed_silver_ingested_at", StringType(), False),
    StructField("rows_merged", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("gold_run_id",StringType(),False),
    StructField("updated_at", TimestampType(), False)
])

def upsert_gold_control(spark,table_name,last_processed_silver_run_id,last_processed_silver_ingested_at,rows_merged,gold_run_id):

    ctrl_df = spark.createDataFrame(
            [(
                "gold",
                table_name,
                last_processed_silver_run_id,
                last_processed_silver_ingested_at,
                int(rows_merged),
                "success",
                gold_run_id,
                datetime.now()
            )],
        schema=schema
        )
    
    dt = DeltaTable.forName(spark, "novacart_catalog.audit.gold_processing_control")
    (dt.alias("target")
       .merge(ctrl_df.alias("source"),f"target.table_name = source.table_name AND target.layer = source.layer")
       .whenMatchedUpdate(set={
            "last_processed_silver_run_id": "source.last_processed_silver_run_id",
            "last_processed_silver_ingested_at": "source.last_processed_silver_ingested_at",
            "rows_merged": "source.rows_merged",
            "run_status": "source.run_status",
            "gold_run_id": "source.gold_run_id",
            "updated_at": "source.updated_at"
       })
       .whenNotMatchedInsertAll()
       .execute())
    
    






















