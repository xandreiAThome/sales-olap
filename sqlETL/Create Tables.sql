CREATE TABLE IF NOT EXISTS warehouse_db.dim_riders (
    riderID INT PRIMARY KEY,
    firstName VARCHAR(255),
    lastName VARCHAR(255),
    vehicleType VARCHAR(255),
    courierId INT,
    age INT,
    gender ENUM('male', 'female'),
    courier_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS warehouse_db.dim_products (
	productID INT PRIMARY KEY,
    category varchar(255),
    productName varchar(255),
    price float
);

CREATE TABLE IF NOT EXISTS warehouse_db.dim_users (
	userID INT PRIMARY KEY,
	username varchar(255),
    city varchar(255),
    country varchar(255),
    gender enum('male', 'female')
);

CREATE TABLE IF NOT EXISTS warehouse_db.dim_date (
	date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT
);

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

CREATE TABLE IF NOT EXISTS warehouse_db.stg_orderItems (  
    productID INT NOT NULL,          
    quantity INT NOT NULL,
    userID INT NOT NULL,              
    deliveryDate VARCHAR(20),     
    riderID INT                       
);