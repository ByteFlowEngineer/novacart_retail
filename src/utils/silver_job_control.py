from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import (StructType,StructField,StringType,TimestampType,LongType)


"""
    This Function will upsert the silver tables
"""

def upsert_to_silver(spark,df_source,target_table,join_key):
    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark,target_table)
        (dt.alias("target").merge(
            df_source.alias("spurce"),f"target.{join_key} = spurce.{join_key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        df_source.write.format("delta").mode("append").saveAsTable(target_table)
        



"""
    This Function will return the last succesfully processed bronze rows from the bronze tables
"""

def get_last_processed_bronze_ingested_at(spark,table_name):
    ctrl = (spark.table("novacart_catalog.audit.processing_control") 
                .filter(
                    (F.col("layer") == "silver") &
                    (F.col("table_name") == table_name) &
                    (F.col("run_status") == "success")
                )
                .orderBy(F.col("updated_at").desc())
                .limit(1)
                )
    rows = ctrl.collect()

    if not rows:
        return None
    
    return rows[0]["last_processed_bronze_ingested_at"]





"""
    This Function wll upsert the silver control table
"""


schema = StructType([
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("last_processed_bronze_run_id", StringType(), False),
    StructField("last_processed_bronze_ingested_at", StringType(), False),
    StructField("rows_merged", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("silver_run_id",StringType(),False),
    StructField("updated_at", TimestampType(), False)
])


def upsert_silver_control(spark,table_name,last_processed_bronze_run_id,last_processed_bronze_ingested_at,rows_merged,silver_run_id):

    ctrl_df = spark.createDataFrame(
            [(
                "silver",
                table_name,
                last_processed_bronze_run_id,
                last_processed_bronze_ingested_at,
                int(rows_merged),
                "success",
                silver_run_id,
                datetime.now()
            )],
        schema=schema
        )
    
    dt = DeltaTable.forName(spark, "novacart_catalog.audit.processing_control")

    (
        dt.alias("target")
        .merge(
            ctrl_df.alias("source"),
            "target.table_name = source.table_name AND target.layer = source.layer"
        )
        .whenMatchedUpdate(set={
            "last_processed_bronze_run_id" : "source.last_processed_bronze_run_id",
            "last_processed_bronze_ingested_at" : "source.last_processed_bronze_ingested_at",
            "rows_merged" : "source.rows_merged",
            "run_status" : "source.run_status",
            "silver_run_id" : "source.silver_run_id",
            "updated_at" : "source.updated_at"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    

"""
    This function will get the incremental bronze data
"""


def get_incremental_bronze(spark,silver_table_name,bronze_table_name):
    
    last_ingested_at = get_last_processed_bronze_ingested_at(spark,silver_table_name)

    bronze_df = spark.read.table(bronze_table_name)
    
    if last_ingested_at is None:
        return bronze_df, last_ingested_at
    
    return bronze_df.filter(F.col("bronze_ingested_at") > last_ingested_at), last_ingested_at




















