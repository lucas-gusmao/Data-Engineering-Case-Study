from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import os

# ============================================
# SPARK CONFIGURATION
# ============================================
spark = SparkSession.builder \
    .appName("Billups Case Study") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ============================================
# 1. DATA LOADING AND CLEANING
# ============================================
def load_and_clean_data():
    """
    Loads and cleans data according to the case specification.
    
    Cleaning rules:
    1. Removes duplicate merchant_id entries from merchants file
    2. Replaces null categories with "Unknown category"
    3. Uses merchant_id as name when merchant_name is null
    4. Creates auxiliary columns: month_year and hour
    
    Returns:
        DataFrame: Cleaned data ready for analysis
    """
    
    # Load data
    transactions = spark.read.parquet("input/transactions/")
    merchants = spark.read.csv("input/merchants/", header=True, inferSchema=True).dropDuplicates(["merchant_id"])

    # Handle null categories
    transactions = transactions.withColumn(
        "category",
        when(col("category").isNull(), "Unknown category").otherwise(col("category"))
    )
    
    # Join with merchants and handle null merchant_name
    full_data = transactions.join(
        merchants.select("merchant_id", "merchant_name"),
        "merchant_id",
        "left"
    ).withColumn(
        "merchant_name",
        when(col("merchant_name").isNull(), col("merchant_id").cast("string"))
        .otherwise(col("merchant_name"))
    )
    
    
    # Create auxiliary columns
    full_data = full_data.withColumn(
        "month_year", date_format("purchase_date", "MMM yyyy")
    ).withColumn(
        "hour", hour("purchase_date")
    )
    
    return full_data

# ============================================
# 2. NUMBER FORMATTING FUNCTION
# ============================================
def format_number_with_commas(df, *columns):
    """
    Formats numeric columns with thousand separators.
    
    Args:
        df: DataFrame to be formatted
        *columns: Names of columns to format
    
    Returns:
        DataFrame: DataFrame with formatted columns
    """
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                format_number(col(col_name), 0)
            )
    return df

# ============================================
# 3. QUESTION 1 - TOP 5 MERCHANTS
# ============================================
def q1_top_merchants(data):
    """
    Generates the ranking of the top 5 merchants by month and city.
    
    Methodology:
    1. Groups by month, city, and merchant
    2. Calculates Total_Sales (sum of purchase_amount) and No_of_sales (count)
    3. Uses window function with row_number() to rank within each group
    4. Filters only the top 5 from each group
    
    Output:
        | Month | City | Merchant | Total_Sales | No_of_sales |
        |-------|------|----------|-------------|-------------|
        | Mar 2017 | 69 | Merchant A | 143,315,102 | 7,156 |
    
    Returns:
        DataFrame: Top 5 merchants by month and city
    """
    
    # Aggregate by month, city, and merchant
    aggregated = data.groupBy("month_year", "city_id", "merchant_id", "merchant_name").agg(
        sum("purchase_amount").alias("Total_Sales"),
        count("*").alias("No_of_sales")
    )
    
    # Window function for ranking within each month-city group
    window = Window.partitionBy("month_year", "city_id").orderBy(desc("Total_Sales"))
    
    # Apply ranking and filter top 5
    result = aggregated.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5) \
        .select(
            col("month_year").alias("Month"),
            col("city_id").alias("City"),
            col("merchant_name").alias("Merchant"),
            col("Total_Sales"),
            col("No_of_sales")
        ) \
        .orderBy("Month", "City", "rank")
    
    # Format numbers with commas
    result = format_number_with_commas(result, "Total_Sales", "No_of_sales")
    
    return result

# ============================================
# 4. QUESTION 2 - AVERAGE BY STATE
# ============================================
def q2_avg_by_state(data):
    """
    Calculates the average purchase amount of each merchant in each state.
    
    Methodology:
    1. Groups by merchant_name and state_id
    2. Calculates average of purchase_amount
    3. Orders by highest average first
    
    Output:
        | Merchant | State_ID | Average_Amount |
        |----------|----------|----------------|
        | Merchant A | 2 | 123,000,000 |
    
    Returns:
        DataFrame: Average sales by merchant and state
    """
    result = data.groupBy("merchant_name", "state_id").agg(
        avg("purchase_amount").alias("Average_Amount")
    ).select(
        col("merchant_name").alias("Merchant"),
        col("state_id").alias("State_ID"),
        col("Average_Amount")
    ).orderBy(desc("Average_Amount"))
    
    result = format_number_with_commas(result, "Average_Amount")
    
    return result

# ============================================
# 5. QUESTION 3 - TOP 3 HOURS
# ============================================
def q3_top_hours(data):
    """
    Identifies the top 3 hours with the largest sales for each product category.
    
    Methodology:
    1. Groups by category and hour
    2. Calculates total sales per group
    3. Uses window function with row_number() to rank hours within each category
    4. Filters only the top 3 hours per category
    
    Output:
        | Product_Category | Hour |
        |------------------|------|
        | A | 1300 |
        | A | 1400 |
        | A | 1900 |
    
    Returns:
        DataFrame: Top 3 hours by product category
    """
    
    hourly = data.groupBy("category", "hour").agg(
        sum("purchase_amount").alias("total_sales")
    )
    
    window = Window.partitionBy("category").orderBy(desc("total_sales"))
    
    result = hourly.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 3) \
        .select(
            col("category").alias("Product_Category"),
            # Format hour as 4-digit (e.g., 13 -> 1300, 8 -> 0800)
            format_string("%02d00", col("hour")).alias("Hour")
        ) \
        .orderBy("Product_Category", "rank")
    
    return result

# ============================================
# 6. QUESTION 4 - CORRELATION ANALYSIS
# ============================================
def q4_correlation_analysis(data):
    """
    Analyzes the correlation between city location and product categories.
    
    Methodology:
    1. Identifies top 20 merchants by total transaction volume (using merchant_id)
    2. For each merchant: finds their most frequent city and top-selling category
    3. For each city: identifies the dominant category (most transactions)
    4. Compares merchant's top category with city's dominant category
    5. Classifies as "YES" if they match, "NO" otherwise
    
    Output:
        | merchant_id | total_sales | top_city | merchant_top_category | city_dominant_category | correlation |
        |-------------|-------------|----------|----------------------|----------------------|-------------|
        | 12345 | 279,377 | 69 | A | A | YES - Same category |
    
    Returns:
        DataFrame: City-category correlation analysis
    """
    
    # 1. Top 20 merchants by total sales (using merchant_id only)
    top_merchants = data.groupBy("merchant_id").agg(
        count("*").alias("total_sales")
    ).orderBy(desc("total_sales")).limit(20)
    
    # 2. For each merchant, find the city where they sell the most
    merchant_city_sales = data.groupBy("merchant_id", "city_id").agg(
        count("*").alias("sales_in_city")
    )
    
    window_city = Window.partitionBy("merchant_id").orderBy(desc("sales_in_city"))
    merchant_top_city = merchant_city_sales.withColumn(
        "rank", row_number().over(window_city)
    ).filter(col("rank") == 1) \
    .select(
        col("merchant_id"),
        col("city_id").alias("top_city"),
        col("sales_in_city").alias("top_city_sales")
    )
    
    # 3. For each merchant, find the category they sell the most
    merchant_category_sales = data.groupBy("merchant_id", "category").agg(
        count("*").alias("category_sales")
    )
    
    window_category = Window.partitionBy("merchant_id").orderBy(desc("category_sales"))
    merchant_top_category = merchant_category_sales.withColumn(
        "rank", row_number().over(window_category)
    ).filter(col("rank") == 1) \
    .select(
        col("merchant_id"),
        col("category").alias("merchant_top_category"),
        col("category_sales").alias("merchant_category_sales")
    )
    
    # 4. For each city, find the dominant category (all merchants)
    city_category_sales = data.groupBy("city_id", "category").agg(
        count("*").alias("total_category_sales")
    )
    
    window_city_category = Window.partitionBy("city_id").orderBy(desc("total_category_sales"))
    city_dominant_category = city_category_sales.withColumn(
        "rank", row_number().over(window_city_category)
    ).filter(col("rank") == 1) \
    .select(
        col("city_id"),
        col("category").alias("city_dominant_category"),
        col("total_category_sales").alias("city_category_sales")
    )
    
    # 5. Join everything and check correlation
    result = top_merchants \
        .join(merchant_top_city, "merchant_id") \
        .join(merchant_top_category, "merchant_id") \
        .join(broadcast(city_dominant_category), merchant_top_city.top_city == city_dominant_category.city_id) \
        .select(
            col("merchant_id"),
            col("total_sales"),
            col("top_city"),
            col("top_city_sales"),
            col("merchant_top_category"),
            col("merchant_category_sales"),
            col("city_dominant_category"),
            col("city_category_sales"),
            when(col("merchant_top_category") == col("city_dominant_category"),
                 "YES - Same category")
            .otherwise("NO - Different category").alias("correlation")
        ) \
        .orderBy(desc("total_sales"))
        
    
    # 6. Format numbers
    result = format_number_with_commas(
        result,
        "total_sales",
        "top_city_sales",
        "merchant_category_sales",
        "city_category_sales"
    )
    
    return result

# ============================================
# 7. QUESTION 5 - RECOMMENDATIONS
# ============================================
def q5_recommendations(data):
    """
    Provides comprehensive recommendations for a new merchant.
    
    Sub-questions:
    a) Which cities to focus on and why
    b) Which categories to sell
    c) Seasonal patterns (months with interesting behaviors)
    d) Recommended operating hours
    e) Installment recommendation with financial analysis
    
    Returns:
        Tuple of DataFrames: (cities_analysis, categories, seasonality, hours, installment_analysis)
    """
    
    # ============================================
    # 5a) CITY ANALYSIS WITH TOP CATEGORY
    # ============================================
    # 1. Calculate metrics by city
    cities_base = data.groupBy("city_id").agg(
        sum("purchase_amount").alias("Revenue"),
        count("*").alias("Transactions"),
        avg("purchase_amount").alias("Avg_Ticket")
    )
    
    # 2. Find the most sold category in each city
    city_category_sales = data.groupBy("city_id", "category").agg(
        count("*").alias("category_transactions"),
        sum("purchase_amount").alias("category_revenue")
    )
    
    window_category = Window.partitionBy("city_id").orderBy(desc("category_transactions"))
    
    top_category_by_city = city_category_sales.withColumn(
        "rank", row_number().over(window_category)
    ).filter(col("rank") == 1) \
    .select(
        col("city_id"),
        col("category").alias("Top_Category"),
        col("category_transactions").alias("Top_Category_Transactions"),
        col("category_revenue").alias("Top_Category_Revenue")
    )
    
    # 3. Join analyses
    cities_analysis = cities_base \
        .join(top_category_by_city, "city_id") \
        .select(
            col("city_id").alias("City"),
            col("Revenue"),
            col("Transactions"),
            col("Avg_Ticket"),
            col("Top_Category"),
            col("Top_Category_Transactions"),
            col("Top_Category_Revenue")
        ) \
        .orderBy(desc("Revenue"))
    
    cities_analysis = format_number_with_commas(
        cities_analysis, 
        "Revenue", 
        "Transactions", 
        "Avg_Ticket",
        "Top_Category_Transactions",
        "Top_Category_Revenue"
    )
    
    # ============================================
    # 5b) CATEGORY RECOMMENDATIONS
    # ============================================
    categories = data.groupBy("category").agg(
        sum("purchase_amount").alias("Revenue"),
        count("*").alias("Transactions"),
        avg("purchase_amount").alias("Avg_Ticket")
    ).select(
        col("category").alias("Category"),
        col("Revenue"),
        col("Transactions"),
        col("Avg_Ticket")
    ).orderBy(desc("Revenue"))
    
    categories = format_number_with_commas(categories, "Revenue", "Transactions", "Avg_Ticket")
    
    # ============================================
    # 5c) SEASONALITY PATTERNS
    # ============================================
    seasonality = data.groupBy("month_year").agg(
        sum("purchase_amount").alias("Revenue"),
        count("*").alias("Transactions"),
        avg("purchase_amount").alias("Avg_Ticket")
    ).select(
        col("month_year").alias("Month"),
        col("Revenue"),
        col("Transactions"),
        col("Avg_Ticket")
    ).orderBy("Month")
    
    seasonality = format_number_with_commas(seasonality, "Revenue", "Transactions", "Avg_Ticket")
    
    # ============================================
    # 5d) HOUR RECOMMENDATIONS
    # ============================================
    hours = data.groupBy("hour").agg(
        sum("purchase_amount").alias("Revenue"),
        count("*").alias("Transactions"),
        avg("purchase_amount").alias("Avg_Ticket")
    ).select(
        col("hour").alias("Hour"),
        col("Revenue"),
        col("Transactions"),
        col("Avg_Ticket")
    ).orderBy("hour")
    
    hours = format_number_with_commas(hours, "Revenue", "Transactions", "Avg_Ticket")
    
    # ============================================
    # 5e) INSTALLMENT ANALYSIS
    # ============================================
    # Analysis with and without installments
    installment_impact = data.withColumn(
        "is_installment", when(col("installments") > 1, 1).otherwise(0)
    ).groupBy("is_installment").agg(
        count("*").alias("Transactions"),
        sum("purchase_amount").alias("Total_Revenue"),
        avg("purchase_amount").alias("Avg_Ticket")
    ).withColumn(
        "Installment_Type",
        when(col("is_installment") == 1, "With Installments")
        .otherwise("Without Installments")
    )
    
    # Calculate gross profit (25% of sales)
    installment_profit = installment_impact.withColumn(
        "Gross_Profit",
        col("Total_Revenue") * 0.25
    )
    
    # Calculate default loss (22.9% per month)
    installment_loss = installment_profit.withColumn(
        "Default_Loss",
        when(col("is_installment") == 1,
             # Expected loss = Gross_Profit * 22.9% * 50% (default after half payments)
             col("Gross_Profit") * 0.229 * 0.5
        ).otherwise(lit(0))
    )
    
    # Calculate net profit
    final_installment_analysis = installment_loss.withColumn(
        "Net_Profit",
        col("Gross_Profit") - col("Default_Loss")
    ).withColumn(
        "Profit_Margin_%",
        round((col("Net_Profit") / col("Total_Revenue")) * 100, 1)
    ).select(
        col("Installment_Type"),
        col("Transactions"),
        col("Total_Revenue"),
        col("Avg_Ticket"),
        col("Gross_Profit"),
        col("Default_Loss"),
        col("Net_Profit"),
        col("Profit_Margin_%")
    )
    
    # Format numbers
    final_installment_analysis = format_number_with_commas(
        final_installment_analysis,
        "Transactions",
        "Total_Revenue",
        "Avg_Ticket",
        "Gross_Profit",
        "Default_Loss",
        "Net_Profit"
    )

    return (cities_analysis, categories, seasonality, hours, 
            final_installment_analysis)

# ============================================
# 8. RESULT SAVING FUNCTION
# ============================================
def save_results(df, output_path):
    """
    Saves DataFrame to CSV with header.
    
    Args:
        df: DataFrame to save
        output_path: Path where CSV will be saved
    """
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

# ============================================
# 9. MAIN EXECUTION
# ============================================
def main():
    """
    Main execution function.
    
    Runs all analyses and saves results to CSV files.
    
    Output files:
        | File | Description |
        |------|-------------|
        | q1_top_merchants.csv | Top 5 merchants by month and city |
        | q2_avg_by_state.csv | Average sales by merchant and state |
        | q3_top_hours.csv | Top 3 hours by product category |
        | q4_correlation_analysis.csv | City-category correlation analysis |
        | q5_cities_analysis.csv | City rankings with top category |
        | q5_recommended_categories.csv | Category recommendations |
        | q5_seasonality.csv | Monthly sales patterns |
        | q5_recommended_hours.csv | Hourly sales distribution |
        | q5_installment_analysis.csv | Installment financial analysis |
    """
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = f"reports/run_{timestamp}"
    os.makedirs(reports_dir, exist_ok=True)
    
    # Load data
    data = load_and_clean_data()
    print(f"Data loaded: {data.count():,} records")
    
    # Q1
    q1_result = q1_top_merchants(data)
    
    # Q2
    q2_result = q2_avg_by_state(data)
    
    # Q3
    q3_result = q3_top_hours(data)
    
    # Q4
    q4_result = q4_correlation_analysis(data)
    
    # Q5
    q5_cities, q5_categories, q5_seasonality, q5_hours, q5_installment_analysis = q5_recommendations(data)
    
    # Save results
    results = {
        "q1_top_merchants": q1_result,
        "q2_avg_by_state": q2_result,
        "q3_top_hours": q3_result,
        "q4_correlation_analysis": q4_result,
        "q5_cities_analysis": q5_cities,
        "q5_recommended_categories": q5_categories,
        "q5_seasonality": q5_seasonality,
        "q5_recommended_hours": q5_hours,
        "q5_installment_analysis": q5_installment_analysis
    }
    
    for name, df in results.items():
        output_path = f"{reports_dir}/{name}"
        save_results(df, output_path)

if __name__ == "__main__":
    main()