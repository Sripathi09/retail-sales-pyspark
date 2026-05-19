"""
Retail Sales Analysis — PySpark
================================
End-to-end pipeline:
  1. Ingest raw CSV
  2. Data quality: validate, cleanse, deduplicate
  3. Data modelling: normalised schema (orders / products / customers)
  4. ETL transformations
  5. KPI queries (revenue trends, top products, customer segments, seasonal)
  6. Save outputs as Parquet + CSV

Run:
  pip install pyspark
  python data/generate_data.py      # create sample data once
  python src/retail_analysis.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import expr
from pyspark.sql.types import DoubleType, IntegerType
import os

# ── 0. Spark session ───────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RetailSalesAnalysis")
    .config("spark.sql.shuffle.partitions", "4")   # small dataset — keep it fast
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BASE   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT  = os.path.join(BASE, "data",    "sample_sales_data.csv")
OUTPUT = os.path.join(BASE, "outputs")
os.makedirs(OUTPUT, exist_ok=True)

print("\n" + "="*60)
print("  RETAIL SALES ANALYSIS — PySpark Pipeline")
print("="*60)


# ══════════════════════════════════════════════════════════════
# 1. INGEST
# ══════════════════════════════════════════════════════════════
print("\n[1] Ingesting raw data...")

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT)
)

print(f"    Raw rows loaded : {raw_df.count():,}")
raw_df.printSchema()


# ══════════════════════════════════════════════════════════════
# 2. DATA QUALITY — validate, cleanse, deduplicate
# ══════════════════════════════════════════════════════════════
print("\n[2] Data quality & cleansing...")

# 2a. Null report before cleansing
print("\n    Null counts per column (before cleanse):")
raw_df.select(
    [
        F.count(
            F.when(
                F.col(c).isNull() | (F.col(c).cast("string") == ""),
                c
            )
        ).alias(c)
        for c in raw_df.columns
    ]
).show(truncate=False)

# 2b. Remove rows with null/empty customer_id or order_id
clean_df = raw_df.filter(
    F.col("customer_id").isNotNull() &
    (F.col("customer_id") != "") &
    F.col("order_id").isNotNull()
)
nulls_dropped = raw_df.count() - clean_df.count()
print(f"    Rows dropped (null customer/order): {nulls_dropped:,}")

# 2c. Deduplicate on order_id (keep first occurrence)
dedup_df = clean_df.dropDuplicates(["order_id"])
dupes_dropped = clean_df.count() - dedup_df.count()
print(f"    Duplicate rows dropped            : {dupes_dropped:,}")

# 2d. Cast & standardise types
clean_df = (
    dedup_df

    .withColumn(
        "quantity",
        expr("try_cast(quantity as int)")
    )

    .withColumn(
        "unit_price",
        expr("try_cast(unit_price as double)")
    )

    .withColumn(
        "revenue",
        expr("try_cast(revenue as double)")
    )

    .withColumn(
        "discount_pct",
        expr("try_cast(discount_pct as int)")
    )

    .withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("month_name", F.date_format("sale_date", "MMMM"))
    .withColumn("year", F.year("sale_date"))
    .withColumn("month", F.month("sale_date"))
    .withColumn("quarter", F.quarter("sale_date"))
)
print(f"\n    Clean rows for analysis: {clean_df.count():,}")
clean_df.show(5, truncate=False)


# ══════════════════════════════════════════════════════════════
# 3. NORMALISED DATA MODEL
#    orders_df      → fact table
#    products_df    → dimension: product master
#    customers_df   → dimension: customer master
# ══════════════════════════════════════════════════════════════
print("\n[3] Building normalised schema (fact + dimensions)...")

orders_df = clean_df.select(
    "order_id","sale_date","year","month","quarter","month_name",
    "customer_id","product_id","region",
    "quantity","unit_price","discount_pct","revenue"
)

products_df = clean_df.select(
    "product_id","product_name","category"
).dropDuplicates(["product_id"])

customers_df = clean_df.select(
    "customer_id","region"
).dropDuplicates(["customer_id"])

print(f"    orders_df   rows : {orders_df.count():,}")
print(f"    products_df rows : {products_df.count():,}")
print(f"    customers_df rows: {customers_df.count():,}")

# Cache fact table — used by multiple KPI queries
orders_df.cache()


# ══════════════════════════════════════════════════════════════
# 4. ETL — enrich fact table with dimension attributes
# ══════════════════════════════════════════════════════════════
print("\n[4] ETL — joining dimensions onto fact table...")

enriched_df = (
    orders_df
    .join(products_df,  on="product_id",  how="left")
    .join(customers_df.select("customer_id").dropDuplicates(), on="customer_id", how="left")
)
enriched_df.cache()
print(f"    Enriched fact rows: {enriched_df.count():,}")


# ══════════════════════════════════════════════════════════════
# 5. KPI QUERIES
# ══════════════════════════════════════════════════════════════

# ── KPI 1: Monthly Revenue Trend ──────────────────────────────
print("\n[KPI-1] Monthly Revenue Trend")
monthly_revenue = (
    enriched_df
    .groupBy("year","month","month_name")
    .agg(
        F.round(F.sum("revenue"),  2).alias("total_revenue"),
        F.count("order_id")         .alias("total_orders"),
        F.round(F.avg("revenue"),  2).alias("avg_order_value"),
    )
    .orderBy("year","month")
)
monthly_revenue.show(12, truncate=False)


# ── KPI 2: Top 10 Products by Revenue ────────────────────────
print("\n[KPI-2] Top 10 Products by Revenue")
top_products = (
    enriched_df
    .groupBy("product_id","product_name","category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity")           .alias("units_sold"),
        F.count("order_id")         .alias("order_count"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)
top_products.show(truncate=False)


# ── KPI 3: Revenue by Category ────────────────────────────────
print("\n[KPI-3] Revenue by Category")
category_revenue = (
    enriched_df
    .groupBy("category")
    .agg(
        F.round(F.sum("revenue"),  2).alias("total_revenue"),
        F.round(F.avg("revenue"),  2).alias("avg_order_value"),
        F.count("order_id")          .alias("total_orders"),
    )
    .orderBy(F.desc("total_revenue"))
)
category_revenue.show(truncate=False)


# ── KPI 4: Regional Sales Performance ────────────────────────
print("\n[KPI-4] Regional Sales Performance")
regional = (
    enriched_df
    .groupBy("region")
    .agg(
        F.round(F.sum("revenue"),  2).alias("total_revenue"),
        F.count("order_id")          .alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("revenue"),  2).alias("avg_order_value"),
    )
    .orderBy(F.desc("total_revenue"))
)
regional.show(truncate=False)


# ── KPI 5: Seasonal Trends (Quarterly) ───────────────────────
print("\n[KPI-5] Seasonal Revenue by Quarter")
seasonal = (
    enriched_df
    .groupBy("year","quarter")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("order_id")         .alias("orders"),
    )
    .orderBy("year","quarter")
)
seasonal.show(truncate=False)


# ── KPI 6: Customer Segmentation (RFM-style) ─────────────────
print("\n[KPI-6] Customer Segmentation by Spend")

# Total spend per customer
customer_spend = (
    enriched_df
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_spend"),
        F.count("order_id")         .alias("order_count"),
        F.max("sale_date")          .alias("last_purchase"),
    )
)

# Assign segment using spend percentiles
spend_quantiles = customer_spend.approxQuantile("total_spend", [0.33, 0.66], 0.01)
low_thresh, high_thresh = spend_quantiles

customer_segments = customer_spend.withColumn(
    "segment",
    F.when(F.col("total_spend") >= high_thresh, "High Value")
     .when(F.col("total_spend") >= low_thresh,  "Mid Value")
     .otherwise("Low Value")
)

segment_summary = (
    customer_segments
    .groupBy("segment")
    .agg(
        F.count("customer_id")              .alias("customers"),
        F.round(F.avg("total_spend"),  2)   .alias("avg_spend"),
        F.round(F.sum("total_spend"),  2)   .alias("total_revenue"),
        F.round(F.avg("order_count"),  1)   .alias("avg_orders"),
    )
    .orderBy(F.desc("total_revenue"))
)
segment_summary.show(truncate=False)


# ── KPI 7: Discount Impact Analysis ──────────────────────────
print("\n[KPI-7] Discount Impact on Revenue")
discount_impact = (
    enriched_df
    .groupBy("discount_pct")
    .agg(
        F.count("order_id")              .alias("orders"),
        F.round(F.sum("revenue"),   2)   .alias("total_revenue"),
        F.round(F.avg("quantity"),  2)   .alias("avg_qty"),
    )
    .orderBy("discount_pct")
)
discount_impact.show(truncate=False)


# ══════════════════════════════════════════════════════════════
# 6. SAVE OUTPUTS
# ══════════════════════════════════════════════════════════════
print("\n[6] Saving outputs...")

def save(df, name):
    rows = df.collect()

    if len(rows) == 0:
        return

    columns = df.columns

    with open(f"./outputs/{name}.csv", "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")

        for row in rows:
            f.write(",".join([str(x) if x is not None else "" for x in row]) + "\n")

    print(f"Saved: {name}.csv")
save(clean_df,          "clean_sales_data")
save(monthly_revenue,   "kpi_monthly_revenue")
save(top_products,      "kpi_top_products")
save(category_revenue,  "kpi_category_revenue")
save(regional,          "kpi_regional_performance")
save(seasonal,          "kpi_seasonal_trends")
save(customer_segments, "kpi_customer_segments")
save(discount_impact,   "kpi_discount_impact")

print("\n" + "="*60)
print("  Pipeline complete. All KPIs saved to /outputs/")
print("="*60 + "\n")

spark.stop()
