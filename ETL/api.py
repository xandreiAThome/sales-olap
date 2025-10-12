from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from typing import List, Optional
from dotenv import load_dotenv
from util.db_warehouse import Session_db_warehouse
from models.Dim_Products import Dim_Products
from models.Dim_Users import Dim_Users
from models.Dim_Riders import Dim_Rider
from models.Dim_Date import Dim_Date
from models.Fact_Order_Items import Fact_Order_Items
from util.logging_config import setup_logging, get_logger
import uvicorn


load_dotenv()

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Sales OLAP API",
    description="API for querying sales data warehouse",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency to get database session
def get_db():
    db = Session_db_warehouse()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/rollup")
def run_raw_query(db: Session = Depends(get_db)):
    try:
        sql = text("""
            SELECT dd.`Year`, dd.Quarter , dd.`Month`,  SUM(foi.Total_Revenue) as revenue
            FROM fact_order_items foi
            JOIN dim_date dd on dd.Date_ID  = foi.Delivery_Date_ID
            GROUP BY dd.`Year` , dd.Quarter , dd.`Month` WITH ROLLUP
        """)
        result = db.execute(sql)
        rows = result.fetchall()

        return [dict(row._mapping) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/drillDown")
def run_raw_query(db: Session = Depends(get_db)):
    try:
        sql = text("""
            SELECT dr.Courier_Name, dr.Vehicle_Type, dr.First_Name, dr.Last_Name ,SUM(foi.Total_Revenue) as total_revenue
            FROM fact_order_items foi
            JOIN dim_riders dr ON dr.Rider_ID  = foi.Delivery_Rider_ID
            GROUP BY dr.Courier_Name, dr.Vehicle_Type, dr.First_Name, dr.Last_Name 
            ORDER BY total_revenue DESC
        """)
        result = db.execute(sql)
        rows = result.fetchall()

        return [dict(row._mapping) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/slice")
def run_raw_query(db: Session = Depends(get_db)):
    try:
        sql = text("""
            SELECT du.city, dp.Name ,SUM(foi.Total_Revenue ) as total_revenue
            FROM fact_order_items foi
            JOIN dim_users du  ON du.Users_ID = foi.User_ID
            JOIN dim_products dp ON foi.Product_ID = dp.Product_ID
            WHERE du.City = 'East Kobe'
            GROUP BY dp.Product_ID
            ORDER BY total_revenue DESC
        """)
        result = db.execute(sql)
        rows = result.fetchall()

        return [dict(row._mapping) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/api/dice")
def run_raw_query(db: Session = Depends(get_db)):
    try:
        sql = text("""
            SELECT du.City, dp.Category, dd.`Year`, dd.Quarter, SUM(foi.Total_Revenue) as total_revenue
            FROM fact_order_items foi
            JOIN dim_users du ON du.Users_ID = foi.User_ID
            JOIN dim_products dp ON dp.Product_ID = foi.Product_ID
            JOIN dim_date dd  ON dd.Date_ID = foi.Delivery_Date_ID
            WHERE du.City IN ("East Kobe", "Parkerside")
                AND dp.Category IN ("electronics", "toys")
                AND dd.`Year` = "2025"
                AND dd.Quarter = 2
            GROUP BY du.City, dp.Category, dd.`Year`, dd.Quarter
            ORDER BY total_revenue DESC;
        """)
        result = db.execute(sql)
        rows = result.fetchall()

        return [dict(row._mapping) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

if __name__ == "__main__":
    logger.info("Starting FastAPI server...")
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=4000,
        reload=True,
        log_level="info"
    )
