
DROP TABLE IF EXISTS warehouse_db.dim_date;

CREATE TABLE IF NOT EXISTS warehouse_db.dim_date (
	date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT
);

INSERT INTO warehouse_db.dim_date (date, year, month, day, quarter)
SELECT d, YEAR(d), MONTH(d), DAY(d), QUARTER(d)
FROM (
  SELECT DISTINCT
    CASE
      WHEN o.deliveryDate LIKE '__/__/____'
        THEN STR_TO_DATE(o.deliveryDate, '%m/%d/%Y')
      WHEN o.deliveryDate LIKE '____-__-__'
        THEN STR_TO_DATE(o.deliveryDate, '%Y-%m-%d')
      ELSE NULL
    END AS d
  FROM sales_src.orders o
) t
ORDER BY d