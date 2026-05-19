# Retail Sales Analysis — PySpark

End-to-end data engineering and analytics pipeline built with **Apache PySpark** on a retail sales dataset.

## What This Project Covers

| Concept | Implementation |
|---|---|
| Data Ingestion | CSV → Spark DataFrame |
| Data Quality | Null detection, deduplication, type casting |
| Data Modelling | Normalised schema — fact table + dimension tables |
| ETL | Extract → Transform → Load with joins and enrichment |
| Analytics | 7 KPI queries (revenue, segmentation, seasonal trends) |
| Data Storage | Parquet (columnar) + CSV outputs |

## KPIs Computed

1. **Monthly Revenue Trend** — total revenue, orders, avg order value per month
2. **Top 10 Products** — by revenue and units sold
3. **Category Performance** — Electronics vs Furniture vs Clothing vs Groceries
4. **Regional Sales** — revenue, unique customers, avg order value by region
5. **Seasonal Trends** — quarterly revenue analysis
6. **Customer Segmentation** — High / Mid / Low value customers using spend percentiles
7. **Discount Impact** — how discount % affects revenue and quantity

## Project Structure

```
retail_sales_pyspark/
├── data/
│   ├── generate_data.py        # generates sample_sales_data.csv
│   └── sample_sales_data.csv   # ~5000 rows retail sales data
├── src/
│   └── retail_analysis.py      # main PySpark pipeline
├── outputs/                    # auto-created on run
│   ├── clean_sales_data/
│   ├── kpi_monthly_revenue/
│   ├── kpi_top_products/
│   └── ...
├── requirements.txt
└── README.md
```

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate sample data
python data/generate_data.py

# 3. Run the full pipeline
python src/retail_analysis.py
```

## Key PySpark Concepts Used

- `SparkSession` — entry point to PySpark
- `DataFrame` API — `select`, `filter`, `groupBy`, `agg`, `join`, `withColumn`
- `Window` functions — for ranking and percentile calculations
- `F.when / F.otherwise` — conditional logic (customer segmentation)
- `approxQuantile` — spend threshold calculation
- `.cache()` — persist frequently queried DataFrames in memory
- `.write.parquet()` — efficient columnar output format
- `dropDuplicates`, `isNull`, type casting — data quality operations

## Tech Stack

- Python 3.8+
- Apache PySpark 3.x
- SQL Server concepts applied in Spark SQL
