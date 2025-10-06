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
                    "full_name": f"{u.First_Name} {u.Last_Name}",
                    "address": {
                        "address_1": u.Address_1,
                        "address_2": u.Address_2,
                        "city": u.City,
                        "country": u.Country,
                        "zipcode": u.Zipcode
                    },
                    "phone_number": u.Phone_Number,
                    "birth_date": u.Birth_Date.isoformat() if u.Birth_Date else None,
                    "gender": u.Gender
                }
                for u in users
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users/{user_id}")
def get_user_by_id(user_id: int):
    """Get a specific user by ID"""
    try:
        db = next(get_db())
        user = db.query(Dim_Users).filter(Dim_Users.Users_ID == user_id).first()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "user_id": user.Users_ID,
            "username": user.Username,
            "first_name": user.First_Name,
            "last_name": user.Last_Name,
            "full_name": f"{user.First_Name} {user.Last_Name}",
            "address": {
                "address_1": user.Address_1,
                "address_2": user.Address_2,
                "city": user.City,
                "country": user.Country,
                "zipcode": user.Zipcode
            },
            "phone_number": user.Phone_Number,
            "birth_date": user.Birth_Date.isoformat() if user.Birth_Date else None,
            "gender": user.Gender
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {e}")
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
        port=4000,
        reload=True,
        log_level="info"
    )
