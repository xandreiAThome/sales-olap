DROP TABLE IF EXISTS warehouse_db.dim_riders;

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

INSERT INTO warehouse_db.dim_riders (
    riderID, firstName, lastName, vehicleType, courierId, age, gender, courier_name
)
SELECT 
    r.id AS riderID,
    r.firstName,
    r.lastName,
    CASE 
        WHEN r.vehicleType = 'motorcycle' THEN 'motorbike'
        WHEN r.vehicleType = 'bicycle' THEN 'bike'
        ELSE r.vehicleType
    END AS vehicleType,
    r.courierId,
    r.age,
    CASE LOWER(r.gender)
        WHEN 'm' THEN 'male'
        WHEN 'f' THEN 'female'
        ELSE r.gender
    END,
    c.name AS courier_name
FROM sales_src.riders r
LEFT JOIN sales_src.couriers c 
    ON c.id = r.courierId;
