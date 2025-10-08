
DROP TABLE IF EXISTS warehouse_db.dim_users;

CREATE TABLE IF NOT EXISTS warehouse_db.dim_users (
	userID INT PRIMARY KEY,
	username varchar(255),
    city varchar(255),
    country varchar(255),
    gender enum('male', 'female')
);

INSERT INTO warehouse_db.dim_users (
    userID, username, city, country, gender 
)
SELECT 
    u.id,
    u.username,
    u.city,
    u.country,
    CASE LOWER(u.gender)
        WHEN 'm' THEN 'male'
        WHEN 'f' THEN 'female'
        ELSE u.gender
    END
FROM sales_src.users u
