from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, sum as _sum

# Configuration
#DB_NAME = "b2b_db"
#RAW_PATH = "dbfs:/mnt/b2b_sales/raw/"
#spark.sql(f"USE {DB_NAME}")

# As we have file ready with us and can be upload directly to the src (bronze layer) we can skip ingest_to_bronze() functions
'''
def ingest_to_bronze():
    """Step 1: Ingest raw CSVs into Bronze Delta Tables (Raw Data)"""
    sources = {
        "blinkit": "01-31-jan-2026-Blinkit-Sales.csv",
        "zepto": "01-31-jan-2026-Zepto.csv",
        "nykaa": "1-31-Jan-Naykaa-online.csv",
        "myntra": "myntra-jan26.csv"
    }
    
    for table_name, file_name in sources.items():
        path = f"{RAW_PATH}{file_name}"
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            df.write.format("delta").mode("overwrite").saveAsTable(f"bronze_{table_name}")
            print(f"Successfully ingested {table_name} to Bronze.")
        except Exception as e:
            print(f"Error ingesting {table_name}: {e}")
'''

def process_to_silver():
    """Step 2: Clean and Standardize to Silver Table"""
    bronze_blinkit = 'b2b_db.01_31_jan_2026_blinkit_sales'
    bronze_zepto = 'b2b_db.01_31_jan_2026_zepto'
    bronze_nykaa = 'b2b_db.1_31_jan_naykaa_online'
    bronze_myntra = 'b2b_db.myntra_jan_26'
    
    
    # BLINKIT
    blinkit = spark.table(bronze_blinkit).select(
        to_date(col("date"), "dd-MM-yyyy").alias("date"),
        col("item_id").cast("string").alias("sku"),
        col("qty_sold").cast("int").alias("total_units"),
        col("mrp").cast("double").alias("total_revenue"),
        lit("Blinkit").alias("data_source")
    )

    # ZEPTO
    zepto = spark.table(bronze_zepto).select(
        to_date(col("Date"), "dd-MM-yyyy").alias("date"),
        col("SKU Number").cast("string").alias("sku"),
        col("Sales (Qty) - Units").cast("int").alias("total_units"),
        col("Gross Merchandise Value").cast("double").alias("total_revenue"),
        lit("Zepto").alias("data_source")
    )

    # NYKAA
    nykaa = spark.table(bronze_nykaa).select(
        to_date(col("date"), "dd-MM-yyyy").alias("date"),
        col("SKU Code").cast("string").alias("sku"),
        col("Total Qty").cast("int").alias("total_units"),
        col("Selling Price").cast("double").alias("total_revenue"),
        lit("Nykaa").alias("data_source")
    )

    # MYNTRA (Handles YYYYMMDD integer dates)
    myntra = spark.table(bronze_myntra).select(
        to_date(col("order_created_date").cast("string"), "yyyyMMdd").alias("date"),
        col("style_id").cast("string").alias("sku"),
        col("sales").cast("int").alias("total_units"),
        col("mrp_revenue").cast("double").alias("total_revenue"),
        lit("Myntra").alias("data_source")
    )

    # Combine and Deduplicate
    silver_df = blinkit.unionByName(zepto).unionByName(nykaa).unionByName(myntra)
    silver_df = silver_df.dropna(subset=["date", "sku"])
    
    silver_df.write.format("delta").mode("overwrite").saveAsTable("b2b_silver.silver_standardized_sales")
    print("Silver Transformation Complete.")

def finalize_to_gold():
    """Step 3: Aggregate to Gold (Final Business Table)"""
    gold_df = spark.table("b2b_silver.silver_standardized_sales") \
        .groupBy("date", "sku", "data_source") \
        .agg(
            _sum("total_units").alias("total_units"),
            _sum("total_revenue").alias("total_revenue")
        )
    
    gold_df.write.format("delta").mode("overwrite").saveAsTable("b2b_gold.gold_daily_sales_fact")
    print("Gold Aggregation Complete. Pipeline Finished.")

# Execute Pipeline
#ingest_to_bronze()   --> No need to ingest as we are leveraging Databricks csv load option which create delta table
process_to_silver()
finalize_to_gold()

# Validation
display(spark.sql("SELECT * FROM b2b_gold.gold_daily_sales_fact LIMIT 10"))