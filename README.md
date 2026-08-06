# Billups Data Engineering Case Study

## Overview

This repository contains my solution for the Billups Data Engineering Case Study. The solution analyzes historical transaction data to provide business insights and recommendations for a new merchant entering the market.

## Executive Summary

```
================================================================================
                    BILLUPS DATA ENGINEERING CASE STUDY
                               EXECUTIVE SUMMARY
================================================================================

DATA OVERVIEW
-------------
Total Records: 7,274,367 transactions
Time Period: 2017-2018
Data Volume: 261 MB (parquet) + 39 MB (CSV)

================================================================================
QUESTION 1: TOP 5 MERCHANTS BY MONTH AND CITY
================================================================================

Top merchants by month and city, ranked by Total Sales.

Key Finding: City 69 consistently dominates with merchants achieving significantly
higher sales volumes than other cities.

================================================================================
QUESTION 2: AVERAGE SALES BY MERCHANT AND STATE
================================================================================

Average purchase amount of each merchant in each state, ordered by highest average first.

Key Finding: State 69 shows consistently higher average ticket sizes (~$20,000).

================================================================================
QUESTION 3: TOP 3 HOURS BY PRODUCT CATEGORY
================================================================================

Top 3 hours with largest sales volume for each product category.

Key Finding: Peak hours vary by category, concentrated in late morning and evening.

================================================================================
QUESTION 4: CITY-CATEGORY CORRELATION ANALYSIS
================================================================================

Analyzes whether top merchants sell the dominant category in their primary city.

Key Finding: 20 out of 20 top merchants (100%) sell the dominant category in their city.
Strong correlation exists between merchant location and categories they sell.

================================================================================
QUESTION 5: RECOMMENDATIONS FOR NEW MERCHANT
================================================================================

5a) CITY RECOMMENDATIONS
------------------------

| Rank | City | Revenue | Transactions | Avg Ticket | Top Category | Rationale |
|------|------|---------|--------------|------------|--------------|-----------|
| 1    | 69   | $24.27B | 1,207,966    | $20,090    | A            | Highest revenue, strong category match |
| 2    | 1    | $12.98B | 645,719      | $20,108    | B            | High volume, less competition |
| 3    | 19   | $5.80B  | 287,854      | $20,140    | A            | Growing market with good potential |

Recommendation: Primary focus on City 69, secondary on City 1.

5b) CATEGORY RECOMMENDATIONS
----------------------------

| Category | Revenue | Transactions | Avg Ticket | Rationale |
|----------|---------|--------------|------------|-----------|
| A        | Highest | High         | ~$20,090   | Dominant in top cities |
| B        | High    | High         | ~$20,108   | Strong in secondary cities |

Recommendation: Primary: Category A. Secondary: Category B.

5c) SEASONALITY PATTERNS
------------------------

| Month | Revenue | Pattern |
|-------|---------|---------|
| Dec   | Highest | PEAK - Holiday season (+190% vs slowest) |
| Nov   | High    | Pre-holiday |
| Oct   | High    | Start of holiday season |
| Feb   | Lowest  | Slowest month |

Key Observations:
- December is peak month (+190% vs February)
- Avg Ticket remains stable (~$20,000) across all months
- Sales growth comes from transaction volume, not price

Recommendation: Invest heavily in October-December. Build customer base in slow months.

5d) OPERATING HOURS RECOMMENDATIONS
-----------------------------------

| Window | Hours | Revenue | % of Daily | Revenue/Hour |
|--------|-------|---------|------------|--------------|
| 8h-22h | 14    | $113.22B| 82.6%      | $8.09B       |
| 9h-21h | 12    | $101.34B| 74.0%      | $8.45B       |
| 10h-20h| 10    | $98.33B | 71.8%      | $9.83B       |

Recommendation 1: MAXIMIZE REVENUE (14 hours)
------------------------------------------------
OPEN: 8:00 AM - CLOSE: 10:00 PM

Justification:
- Captures 82.6% of daily revenue ($113.22B)
- Covers all peak periods (10h-20h)
- Includes morning hours (8h-9h) with $3.9B in sales
- Includes evening hour (21h-22h) with $6.98B in sales
- Best total revenue coverage
- Recommended for high-volume, full-service operations

Recommendation 2: SINGLE SHIFT EFFICIENCY (10 hours)
----------------------------------------------------
OPEN: 10:00 AM - CLOSE: 8:00 PM

Justification:
- Captures 71.8% of daily revenue ($98.33B)
- Highest revenue per hour ($9.83B)
- Single 10-hour shift simplifies staffing
- Covers the absolute peak period (10h-19h)
- Avoids low-volume early morning and late evening hours
- Most cost-effective option for lean operations

Final Recommendation: 
Start with 10h-20h (10 hours) for operational efficiency.
If demand grows, expand to 8h-22h (14 hours) to capture additional revenue.

5e) INSTALLMENT ANALYSIS
------------------------

| Metric | Without Installments | With Installments |
|--------|---------------------|-------------------|
| Transactions | 6,814,614 | 459,753 |
| Total Revenue | $136.98B | $9.25B |
| Avg Ticket | $20,101 | $20,116 |
| Gross Profit (25%) | $34.24B | $2.31B |
| Default Loss (22.9%) | $0 | $264.7M |
| Net Profit | $34.24B | $2.05B |
| Profit Margin | 25.0% | 22.1% |

Recommendation: YES - ACCEPT INSTALLMENTS

Justification:
- Only 6.3% of transactions use installments
- Adds $2.05B in additional profit
- Default risk is low (11.4% of installment profit)
- Does not cannibalize cash sales
- Ticket size remains stable

================================================================================
SUMMARY OF RECOMMENDATIONS
================================================================================

| Area | Recommendation | Rationale |
|------|----------------|-----------|
| Primary City | City 69 | Highest revenue concentration |
| Secondary City | City 1 | Good volume, less competition |
| Primary Category | Category A | Dominant in top cities |
| Secondary Category | Category B | Strong in secondary cities |
| Seasonal Strategy | Invest Oct-Dec | Peak sales period (+190%) |
| Operating Hours | 9:00 AM - 9:00 PM | Optimal revenue per hour |
| Installments | Accept | $2.05B additional profit |

================================================================================
KEY ASSUMPTIONS
================================================================================

General:
- Historical data (2017-2018) is representative of current patterns
- New merchant has flexibility in choosing cities, categories, and hours
- Dataset is a representative sample of the full business

Installment Analysis:
- Default rate of 22.9% per month is constant
- Defaults occur after 50% of payments are made
- All installments are equal in value

================================================================================
TECHNICAL IMPLEMENTATION
================================================================================

Framework: PySpark (Apache Spark)
Language: Python 3
Data Format: Parquet (transactions), CSV (merchants)
Output: CSV with headers

Key Design Decisions:
- Used merchant_id internally to handle duplicate anonymized names
- Maintained decimal precision for monetary values
- Used row_number() for Top N queries
- Broadcast joins for small lookup tables
```

## Project Structure

```
.
├── src/
│   └── main.py                 # Main application code
├── input/                      # Data directory (not included in repo)
│   ├── transactions/           # Parquet files
│   └── merchants.csv           # Merchant data
├── reports/                    # Generated reports (created at runtime)
│   └── run_YYYYMMDD_HHMMSS/    # Timestamped output directory
│       ├── q1_top_merchants.csv
│       ├── q2_avg_by_state.csv
│       ├── q3_top_hours.csv
│       ├── q4_correlation_analysis.csv
│       ├── q5_cities_analysis.csv
│       ├── q5_recommended_categories.csv
│       ├── q5_seasonality.csv
│       ├── q5_recommended_hours.csv
│       └── q5_installment_analysis.csv
├── README.md                   # This file
└── requirements.txt            # Python dependencies
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- Apache Spark (or PySpark installed)
- Java 8 or 11 (required for Spark)

### Installation

1. Clone the repository:

```bash
git clone https://github.com/your-username/billups-case-study.git
cd billups-case-study
```

2. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

### Data Setup

Place the data files in the `input/` directory:
- `input/transactions/` - Parquet files (historical transactions)
- `input/merchants/` - Merchant csv data

### Running the Application

```bash
python src/main.py
```

The application will:
1. Load and clean the data
2. Run all five analyses
3. Save results to `reports/run_YYYYMMDD_HHMMSS/`

## Output Files Description

| File | Description |
|------|-------------|
| `q1_top_merchants.csv` | Top 5 merchants by month and city |
| `q2_avg_by_state.csv` | Average sales by merchant and state |
| `q3_top_hours.csv` | Top 3 hours by product category |
| `q4_correlation_analysis.csv` | City-category correlation analysis |
| `q5_cities_analysis.csv` | City rankings with top category |
| `q5_recommended_categories.csv` | Category recommendations |
| `q5_seasonality.csv` | Monthly sales patterns |
| `q5_recommended_hours.csv` | Hourly sales distribution |
| `q5_installment_analysis.csv` | Installment financial analysis |

## Technologies Used

- **Python 3.x**
- **PySpark** (Apache Spark)
- **Apache Parquet** (data storage)
- **CSV** (data input/output)

## Key Design Decisions

### 1. Data Cleaning
- Removed duplicate `merchant_id` entries from merchants file
- Replaced null categories with "Unknown category"
- Used `merchant_id` as name when `merchant_name` is null
- Maintained decimal precision for monetary values

### 2. Performance Optimizations
- Configured shuffle partitions for local execution
- Enabled adaptive query execution
- Used broadcast joins for small lookup tables

### 3. Precision Handling
- Kept `purchase_amount` with full decimal precision
- Applied rounding only at the presentation layer
- Used 2 decimal places for monetary values in reports

### 4. Window Functions
- Used `row_number()` for Top N queries (exactly N results per group)
- Avoided `rank()` which could return more than N rows due to ties

## Performance Metrics

- **Total Records**: ~7.2M transactions
- **Data Volume**: 261 MB (parquet) + 39 MB (CSV)
- **Execution Time**: ~30-50 seconds (local environment)

## License

This project is for evaluation purposes only.
