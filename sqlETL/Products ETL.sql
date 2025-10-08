
DROP TABLE IF EXISTS warehouse_db.dim_products;

CREATE TABLE IF NOT EXISTS warehouse_db.dim_products (
	productID INT PRIMARY KEY,
    category varchar(255),
    productName varchar(255),
    price float
);

INSERT INTO warehouse_db.dim_products (
    productID, category, productName, price 
)
SELECT 
    p.id,
    CASE 
		WHEN LOWER(p.category) IN ('toy','toys') THEN 'toys'
		WHEN LOWER(p.category) IN ('makeup','make up') THEN 'makeup'
		WHEN LOWER(p.category) IN ('bag','bags') THEN 'bags'
		WHEN LOWER(p.category) IN ('electronics','gadgets','laptops') THEN 'electronics'
		WHEN LOWER(p.category) IN ('men''s apparel','clothes') THEN 'apparel'
		ELSE LOWER(p.category)
	END,
    p.name,
    p.price
FROM sales_src.products p
