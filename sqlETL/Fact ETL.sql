DROP TABLE IF EXISTS warehouse_db.fact_orderItems;
DROP TABLE IF EXISTS warehouse_db.stg_orderItems;


-- Create staging table
CREATE TABLE IF NOT EXISTS warehouse_db.stg_orderItems (  
    productID INT NOT NULL,          
    quantity INT NOT NULL,
    userID INT NOT NULL,              
    deliveryDate VARCHAR(20),     
    riderID INT                       
);

-- Insert into staging table first
INSERT INTO warehouse_db.stg_orderItems (
	productID, quantity, userID, deliveryDate, riderID
)
SELECT
    oi.ProductId   AS productID,
    oi.quantity    AS quantity,
    o.userId       AS userID,
    o.deliveryDate AS deliveryDate,  
    o.deliveryRiderId AS riderID
FROM sales_src.orderItems oi
JOIN sales_src.orders o ON oi.orderId = o.id;

-- Create fact table
CREATE TABLE IF NOT EXISTS warehouse_db.fact_orderItems (
    orderitemID INT AUTO_INCREMENT PRIMARY KEY,
    productID INT,
    quantity INT,
    date DATE,
    userID INT,
    riderID INT,
    revenue FLOAT,
    FOREIGN KEY (productID) REFERENCES warehouse_db.dim_products(productID),
    FOREIGN KEY (date) REFERENCES warehouse_db.dim_date(date),
    FOREIGN KEY (userID) REFERENCES warehouse_db.dim_users(userID),
	FOREIGN KEY (riderID) REFERENCES warehouse_db.dim_riders(riderID)
);

-- Insert into fact table
INSERT INTO warehouse_db.fact_orderItems (
    productID, quantity, date, userID, riderID, revenue
)
SELECT 
    dp.productID,                  
    s.quantity,
    CASE
        WHEN s.deliveryDate LIKE '__/__/____' 
          THEN STR_TO_DATE(s.deliveryDate, '%m/%d/%Y')
        WHEN s.deliveryDate LIKE '____-__-__'
          THEN STR_TO_DATE(s.deliveryDate, '%Y-%m-%d')
        ELSE NULL
	END,                  
    s.userID,              
    s.riderID,                 
    s.quantity * dp.price AS revenue
FROM warehouse_db.stg_orderItems s
JOIN warehouse_db.dim_products dp ON dp.productID = s.productID


