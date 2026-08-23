from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
START_DATE = "2024-01-01"
END_DATE = "2027-12-31"
 
 
@dp.materialized_view(name="uber_project.gold.dim_date")
def dim_date():
    days = spark.sql(
        f"""
        SELECT explode(
            sequence(DATE'{START_DATE}', DATE'{END_DATE}', INTERVAL 1 DAY)
        ) AS calendar_date
        """
    )
 
    return days.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.date_format("calendar_date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
        F.dayofweek("calendar_date").alias("day_of_week"),  # 1 = Sunday
        F.date_format("calendar_date", "EEEE").alias("day_name"),
        F.weekofyear("calendar_date").alias("week_of_year"),
        F.date_trunc("week", F.col("calendar_date")).cast("date").alias("week_start_date"),
        F.dayofweek("calendar_date").isin(1, 7).alias("is_weekend"),
    )
 