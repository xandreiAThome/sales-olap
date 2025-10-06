from fastapi import FastAPI, HTTPException, Query
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
    allow_origins=["*"],  # Configure appropriately for production
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


@app.get("/")
def read_root():
    """Root endpoint with API information"""
    return {
        "message": "Sales OLAP API",
        "version": "1.0.0",
        "endpoints": {
            "products": "/api/products",
            "users": "/api/users",
            "riders": "/api/riders",
            "orders": "/api/orders",
            "sales-summary": "/api/sales-summary"
        }
    }


@app.get("/api/products")
def get_products(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    category: Optional[str] = None
):
    """Get products with optional category filter"""
    try:
        db = next(get_db())
        query = db.query(Dim_Products)
        
        if category:
            query = query.filter(Dim_Products.Category == category)
        
        products = query.offset(skip).limit(limit).all()
        
        return {
            "count": len(products),
            "data": [
                {
                    "product_id": p.Product_ID,
                    "product_code": p.Product_Code,
                    "name": p.Name,
                    "category": p.Category,
                    "description": p.Description,
                    "price": float(p.Price)
                }
                for p in products
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users")
def get_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    city: Optional[str] = None,
    country: Optional[str] = None
):
    """Get users with optional city and country filters"""
    try:
        db = next(get_db())
        query = db.query(Dim_Users)
        
        if city:
            query = query.filter(Dim_Users.City == city)
        if country:
            query = query.filter(Dim_Users.Country == country)
        
        users = query.offset(skip).limit(limit).all()
        
        return {
            "count": len(users),
            "data": [
                {
                    "user_id": u.Users_ID,
                    "username": u.Username,
                    "first_name": u.First_Name,
                    "last_name": u.Last_Name,
                    "city": u.City,
                    "country": u.Country,
                    "phone_number": u.Phone_Number,
                    "gender": u.Gender
                }
                for u in users
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/riders")
def get_riders(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    courier_name: Optional[str] = None
):
    """Get riders with optional courier name filter"""
    try:
        db = next(get_db())
        query = db.query(Dim_Rider)
        
        if courier_name:
            query = query.filter(Dim_Rider.Courier_Name == courier_name)
        
        riders = query.offset(skip).limit(limit).all()
        
        return {
            "count": len(riders),
            "data": [
                {
                    "rider_id": r.Rider_ID,
                    "first_name": r.First_Name,
                    "last_name": r.Last_Name,
                    "vehicle_type": r.Vehicle_Type,
                    "age": r.Age,
                    "gender": r.Gender,
                    "courier_name": r.Courier_Name
                }
                for r in riders
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching riders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/orders")
def get_orders(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    order_num: Optional[str] = None
):
    """Get order items with optional order number filter"""
    try:
        db = next(get_db())
        query = db.query(
            Fact_Order_Items,
            Dim_Products.Name.label("product_name"),
            Dim_Users.Username.label("username"),
            Dim_Rider.First_Name.label("rider_first_name"),
            Dim_Rider.Last_Name.label("rider_last_name")
        ).join(
            Dim_Products, Fact_Order_Items.Product_ID == Dim_Products.Product_ID
        ).join(
            Dim_Users, Fact_Order_Items.User_ID == Dim_Users.Users_ID
        ).join(
            Dim_Rider, Fact_Order_Items.Delivery_Rider_ID == Dim_Rider.Rider_ID
        )
        
        if order_num:
            query = query.filter(Fact_Order_Items.Order_Num == order_num)
        
        orders = query.offset(skip).limit(limit).all()
        
        return {
            "count": len(orders),
            "data": [
                {
                    "order_item_id": o.Fact_Order_Items.Order_Item_ID,
                    "order_num": o.Fact_Order_Items.Order_Num,
                    "product_name": o.product_name,
                    "quantity": o.Fact_Order_Items.Quantity,
                    "total_revenue": float(o.Fact_Order_Items.Total_Revenue),
                    "username": o.username,
                    "rider_name": f"{o.rider_first_name} {o.rider_last_name}",
                    "notes": o.Fact_Order_Items.Notes
                }
                for o in orders
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sales-summary")
def get_sales_summary(
    group_by: str = Query("category", regex="^(category|product|user|rider)$")
):
    """Get sales summary grouped by category, product, user, or rider"""
    try:
        db = next(get_db())
        
        if group_by == "category":
            results = db.query(
                Dim_Products.Category,
                func.sum(Fact_Order_Items.Total_Revenue).label("total_revenue"),
                func.sum(Fact_Order_Items.Quantity).label("total_quantity"),
                func.count(Fact_Order_Items.Order_Item_ID).label("order_count")
            ).join(
                Dim_Products, Fact_Order_Items.Product_ID == Dim_Products.Product_ID
            ).group_by(
                Dim_Products.Category
            ).all()
            
            return {
                "group_by": "category",
                "data": [
                    {
                        "category": r.Category,
                        "total_revenue": float(r.total_revenue) if r.total_revenue else 0,
                        "total_quantity": r.total_quantity,
                        "order_count": r.order_count
                    }
                    for r in results
                ]
            }
        
        elif group_by == "product":
            results = db.query(
                Dim_Products.Name,
                Dim_Products.Category,
                func.sum(Fact_Order_Items.Total_Revenue).label("total_revenue"),
                func.sum(Fact_Order_Items.Quantity).label("total_quantity"),
                func.count(Fact_Order_Items.Order_Item_ID).label("order_count")
            ).join(
                Dim_Products, Fact_Order_Items.Product_ID == Dim_Products.Product_ID
            ).group_by(
                Dim_Products.Product_ID, Dim_Products.Name, Dim_Products.Category
            ).order_by(
                func.sum(Fact_Order_Items.Total_Revenue).desc()
            ).limit(50).all()
            
            return {
                "group_by": "product",
                "data": [
                    {
                        "product_name": r.Name,
                        "category": r.Category,
                        "total_revenue": float(r.total_revenue) if r.total_revenue else 0,
                        "total_quantity": r.total_quantity,
                        "order_count": r.order_count
                    }
                    for r in results
                ]
            }
        
        elif group_by == "user":
            results = db.query(
                Dim_Users.Username,
                Dim_Users.City,
                Dim_Users.Country,
                func.sum(Fact_Order_Items.Total_Revenue).label("total_revenue"),
                func.count(Fact_Order_Items.Order_Item_ID).label("order_count")
            ).join(
                Dim_Users, Fact_Order_Items.User_ID == Dim_Users.Users_ID
            ).group_by(
                Dim_Users.Users_ID, Dim_Users.Username, Dim_Users.City, Dim_Users.Country
            ).order_by(
                func.sum(Fact_Order_Items.Total_Revenue).desc()
            ).limit(50).all()
            
            return {
                "group_by": "user",
                "data": [
                    {
                        "username": r.Username,
                        "city": r.City,
                        "country": r.Country,
                        "total_revenue": float(r.total_revenue) if r.total_revenue else 0,
                        "order_count": r.order_count
                    }
                    for r in results
                ]
            }
        
        elif group_by == "rider":
            results = db.query(
                Dim_Rider.First_Name,
                Dim_Rider.Last_Name,
                Dim_Rider.Courier_Name,
                func.count(Fact_Order_Items.Order_Item_ID).label("delivery_count"),
                func.sum(Fact_Order_Items.Total_Revenue).label("total_revenue")
            ).join(
                Dim_Rider, Fact_Order_Items.Delivery_Rider_ID == Dim_Rider.Rider_ID
            ).group_by(
                Dim_Rider.Rider_ID, Dim_Rider.First_Name, Dim_Rider.Last_Name, Dim_Rider.Courier_Name
            ).order_by(
                func.count(Fact_Order_Items.Order_Item_ID).desc()
            ).limit(50).all()
            
            return {
                "group_by": "rider",
                "data": [
                    {
                        "rider_name": f"{r.First_Name} {r.Last_Name}",
                        "courier_name": r.Courier_Name,
                        "delivery_count": r.delivery_count,
                        "total_revenue": float(r.total_revenue) if r.total_revenue else 0
                    }
                    for r in results
                ]
            }
        
    except Exception as e:
        logger.error(f"Error fetching sales summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def health_check():
    """Health check endpoint"""
    try:
        db = next(get_db())
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")


if __name__ == "__main__":
    logger.info("Starting FastAPI server...")
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
