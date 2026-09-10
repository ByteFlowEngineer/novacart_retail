from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import (StructType,StructField,StringType,TimestampType,LongType)



def get_last_successful_timestamp(spark,tablename : str,layer:str="bronze"):
    """
    This function returns the last successful timestamp 
    from the last run of the job from ingestion_control_table table
    """

    df = spark.read.table("novacart_catalog.audit.ingestion_control_table")
    df_maxt_timestamp = (
        df.filter(
            (F.col("layer") == layer) &
            (F.col("table_name") == tablename) &
            (F.col("run_status") == "success")
        )
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )

    if not df_maxt_timestamp:
        return None, None
    
    return df_maxt_timestamp[0]["last_ingested_ts"],df_maxt_timestamp[0]["last_ingested_pk"]
    

schema = StructType([
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("timestamp_col", StringType(), False),
    StructField("primary_col", StringType(), False),
    StructField("last_ingested_ts", TimestampType(), True),
    StructField("last_ingested_pk", LongType(), True),
    StructField("last_run_id", StringType(), False),
    StructField("rows_written", LongType(), False),
    StructField("run_status", StringType(), False),
    StructField("updated_at", TimestampType(), False)
])

def upsert_ingestion_control(spark,table_name,timestamp_col,primary_col,last_ingested_ts,last_ingested_pk,last_run_id,rows_written):
    """
    This function upserts the ingestion_control_table table with the last successful timestamp 
    from the last run of the job
    """
    control_df = spark.createDataFrame(
        [(
            "bronze",
            table_name,
            timestamp_col,
            primary_col,
            last_ingested_ts,
            int(last_ingested_pk) if last_ingested_pk is not None else None,
            last_run_id,
            int(rows_written),
            "success",
            datetime.now(),
    )],
        
        schema=schema
    )

    dt = DeltaTable.forName(spark, "novacart_catalog.audit.ingestion_control_table")
    
    dt.alias("t") \
      .merge(
        control_df.alias("s"),
        "t.table_name = s.table_name AND t.layer = s.layer",
            ) \
      .whenMatchedUpdate(set = {
          "last_ingested_ts": F.col("s.last_ingested_ts"),
          "last_ingested_pk": F.col("s.last_ingested_pk"),
          "last_run_id": F.col("s.last_run_id"),
          "rows_written": F.col("s.rows_written"),
          "run_status": F.col("s.run_status"),
          "updated_at": F.col("s.updated_at")
      }) \
      .whenNotMatchedInsertAll() \
      .execute()        






       
                        
