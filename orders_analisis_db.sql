CREATE TABLE df_orders(
	order_id INT PRIMARY KEY,
	order_date DATE,
	ship_mode VARCHAR(20),
	segment VARCHAR(20),
	country VARCHAR(20),
	city VARCHAR(20),
	state VARCHAR(20),
	postal_code VARCHAR(20),
	region VARCHAR(20),
	category VARCHAR(20),
	sub_category VARCHAR(20),
	product_id VARCHAR(20),
	quantity INT,
	discount DECIMAL(7,2),
	sale_price DECIMAL(7,2),
	profit DECIMAL(7,2)
);

SELECT *
FROM df_orders;

-- Find top 10 highest revenue generating products
SELECT 
	product_id,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;

-- Find top 5 highest selling products in each region
WITH cte AS (
SELECT 
	region, product_id,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY region, product_id)
SELECT * 
FROM (
	SELECT *,
	row_number ()
	OVER(
		PARTITION BY region
		ORDER BY sales desc) AS rn
	FROM cte) A
WHERE rn <= 5;

-- Find month over month growth comparison for 2022 and 2023 sales 

WITH cte AS(
SELECT 
	EXTRACT(YEAR FROM order_date) AS order_year, 
	EXTRACT(MONTH FROM order_date) AS order_month,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY
	EXTRACT(YEAR FROM order_date),
	EXTRACT(MONTH FROM order_date)
--ORDER BY 
	--order_year, 
	--order_month
)
SELECT
	order_month,
	SUM(CASE WHEN order_year= 2022 THEN sales ELSE 0 end) AS sales_2022,
	SUM(CASE WHEN order_year= 2023 THEN sales ELSE 0 end) AS sales_2023
FROM cte
GROUP BY order_month
ORDER BY order_month

-- For each category which month had highest sales
WITH cte as (
SELECT 
	category,
	TO_CHAR(order_date, 'YYYYMM') AS order_year_month,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY 
	category,
	TO_CHAR(order_date, 'YYYYMM')
)
SELECT *
FROM (
	SELECT *,
	row_number() OVER(PARTITION BY category ORDER BY sales DESC) as rn
FROM cte
) a
WHERE rn = 1


-- Which sub category has highest growth by rpofit in 2023 compare to 2022
WITH cte as (
SELECT 
	sub_category, 
	EXTRACT(YEAR FROM order_date) AS order_year,
	SUM(sale_price) AS sales
FROM df_orders
GROUP BY 
	sub_category,
	EXTRACT(YEAR FROM order_date)
),
cte2 as (
SELECT 
	sub_category,
	SUM(CASE WHEN order_year = 2022 THEN sales ELSE 0 END) AS sales_2022,
	SUM(CASE WHEN order_year = 2023 THEN sales ELSE 0 END) AS sales_2023
FROM cte
GROUP BY sub_category
)
SELECT *,
	(sales_2023 - sales_2022) * 100.0 / NULLIF(sales_2022,0) AS profit_change
FROM cte2
ORDER BY profit_change DESC
LIMIT 1;