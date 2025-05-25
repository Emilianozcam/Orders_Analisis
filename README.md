# 🛒 Retail Orders Sales Analysis Project 

This project demonstrates a complete ELT (Extract, Load, Transform) pipeline using Python and SQL to analyze a retail dataset downloaded directly from Kaggle. The goal was to extract key business insights such as top-selling products, revenue trends, and category performance across time.

We begin by using the Kaggle API to programmatically download the dataset titled "Retail Orders". This dataset contains detailed sales data including product information, order dates, pricing, categories, and regional breakdowns.

```python
!kaggle datasets download ankitbansal06/retail-orders -f orders.csv

```
## 📦 Dataset

- **Source**: [Retail Orders - Kaggle](https://www.kaggle.com/datasets/ankitbansal06/retail-orders)
- **File Used**: `orders.csv`

Once the dataset is downloaded, the ETL process begins in Python using pandas. Load the CSV into a DataFrame, clean it by removing null values, and standardize column names to lowercase with underscores for SQL compatibility. We also engineer new columns such as "discount", "sale price", and "profit" based on provided pricing information ("list_price", "cost_price", and "discount_percent"). The "order_date" is also converted to datetime format for time-based analysis.

The cleaned and transformed DataFrame is the written into a PostgreSQL database using SQLAlchemy, where it will be queried using SQL for business analysis. Here's a simplified view of the Python ETL script:

```python 
import pandas as pd
import sqlalchemy as sal

data_df = pd.read_csv("orders.csv")
data_df = data_df.dropna()
data_df.columns = data_df.columns.str.strip().str.lower().str.replace(' ', '_')
data_df['discount'] = data_df['list_price'] * data_df['discount_percent']
data_df['sale_price'] = data_df['list_price'] - data_df['discount']
data_df['profit'] = data_df['sale_price'] - data_df['cost_price']
data_df['order_date'] = pd.to_datetime(data_df['order_date'])
data_df = data_df.drop(columns=['cost_price', 'list_price', 'discount_percent'])

engine = sal.create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/SQL_Python_Project")
data_df.to_sql("df_orders", con=engine, index=False, if_exists="append")
```
On the PostgreSQL side, the df_orders table is created with a schema matching the transformed DataFrame. Data types are carefully chosen (e.g., DECIMAL for prices, VARCHAR for strings, DATE for dates). After confirming successful data loading, we proceed to extract insights via SQL.


## Top 10 products with the highest revenue

```SQL
SELECT product_id, SUM(sale_price) AS sales
FROM df_orders
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;
```

## Top 5 highest products in each region

Next, we want to know the best-performing products per region. Using a CTE (common table expression) combined with ROW_NUMBER() partitioned by region, we select the top 5 selling products in each region. This gives insight into regional preferences and performance.


```SQL
WITH cte AS (
    SELECT region, product_id, SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY region, product_id
)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY region ORDER BY sales DESC) AS rn
    FROM cte
) A
WHERE rn <= 5;
```


## Compare Month-over-Month growth between 2022 and 2023

Next, to compare performance across time, we aggregate total monthly sales for 2022 and 2023 using the EXTRACT() function and group the data by month and year. This enables a side-by-side comparison to track month-over-month growth or decline between years.

```SQL
WITH cte AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS order_year, 
        EXTRACT(MONTH FROM order_date) AS order_month,
        SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY order_year, order_month
)
SELECT
    order_month,
    SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
    SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
FROM cte
GROUP BY order_month
ORDER BY order_month;
```


## Month with the highest sales for each category

In another query, we determine which month recorded the highest sales for each product category. This is helpful to identify seasonal trends or promotional effectiveness. We format dates as YYYYMM to group them at the month level and rank them to extract the top month per category.

```SQL
WITH cte AS (
    SELECT 
        category,
        TO_CHAR(order_date, 'YYYYMM') AS order_year_month,
        SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY category, order_year_month
)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) AS rn
    FROM cte
) a
WHERE rn = 1;
```


## Subcategory with the highest sales growth between 2023 and 2022

Lastly, we investigate which subcategory experienced the highest growth from 2022 to 2023. We aggregate sales by subcategory and year, then calculate the percentage change year-over-year. This helps identify breakout performers.

```SQL
WITH cte AS (
    SELECT 
        sub_category, 
        EXTRACT(YEAR FROM order_date) AS order_year,
        SUM(sale_price) AS sales
    FROM df_orders
    GROUP BY sub_category, order_year
),
cte2 AS (
    SELECT 
        sub_category,
        SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
        SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
    FROM cte
    GROUP BY sub_category
)
SELECT *,
    (sales_2023 - sales_2022) * 100.0 / NULLIF(sales_2022, 0) AS profit_change
FROM cte2
ORDER BY profit_change DESC
LIMIT 1;
```


Through this combined use of Python for data transformation and PostgreSQL for querying, we built a flexible and scalable analytical workflow. Tools used include Kaggle API for data sourcing, pandas and SQLAlchemy for ETL, PostgreSQL for storage and querying, and Jupyter Notebook for development and visualization. This pipeline supports rapid iteration and deep insights into sales performance across time, product lines, and geographies.
