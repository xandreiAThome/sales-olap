# Sales OLAP FastAPI Backend

A FastAPI backend for querying the sales data warehouse using the existing SQLAlchemy models.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Make sure your `.env` file has the `DATABASE_WAREHOUSE_URL` configured:

```
DATABASE_WAREHOUSE_URL=mysql+pymysql://username:password@host:port/database_name
```

### 3. Run the API Server

```bash
# From the ETL directory
python api.py
```

Or using uvicorn directly:

```bash
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

## API Documentation

Once the server is running, you can access:
- **Interactive API docs (Swagger UI)**: http://localhost:8000/docs
- **Alternative API docs (ReDoc)**: http://localhost:8000/redoc

## Available Endpoints

### Root
- **GET /** - API information and available endpoints

### Products
- **GET /api/products** - Get all products
  - Query params: `skip`, `limit`, `category`
  - Example: `/api/products?category=Electronics&limit=50`

### Users
- **GET /api/users** - Get all users
  - Query params: `skip`, `limit`, `city`, `country`
  - Example: `/api/users?city=New York&limit=100`

### Riders
- **GET /api/riders** - Get all delivery riders
  - Query params: `skip`, `limit`, `courier_name`
  - Example: `/api/riders?courier_name=FastDelivery`

### Orders
- **GET /api/orders** - Get order items with details
  - Query params: `skip`, `limit`, `order_num`
  - Example: `/api/orders?order_num=ORD123456`

### Sales Summary
- **GET /api/sales-summary** - Get sales analytics
  - Query params: `group_by` (category | product | user | rider)
  - Example: `/api/sales-summary?group_by=category`

### Health Check
- **GET /api/health** - Check API and database connection status

## Example Usage

### Using cURL

```bash
# Get all products
curl http://localhost:8000/api/products

# Get products by category
curl "http://localhost:8000/api/products?category=Electronics"

# Get sales summary by category
curl "http://localhost:8000/api/sales-summary?group_by=category"

# Health check
curl http://localhost:8000/api/health
```

### Using Python requests

```python
import requests

# Get products
response = requests.get("http://localhost:8000/api/products")
products = response.json()
print(products)

# Get sales summary
response = requests.get("http://localhost:8000/api/sales-summary?group_by=product")
summary = response.json()
print(summary)
```

### Using JavaScript fetch

```javascript
// Get orders
fetch('http://localhost:8000/api/orders?limit=10')
  .then(response => response.json())
  .then(data => console.log(data));

// Get sales summary
fetch('http://localhost:8000/api/sales-summary?group_by=rider')
  .then(response => response.json())
  .then(data => console.log(data));
```

## Response Format

All endpoints return JSON with a consistent structure:

```json
{
  "count": 10,
  "data": [
    {
      "product_id": 1,
      "name": "Product Name",
      ...
    }
  ]
}
```

Sales summary endpoint returns:

```json
{
  "group_by": "category",
  "data": [
    {
      "category": "Electronics",
      "total_revenue": 15000.50,
      "total_quantity": 150,
      "order_count": 45
    }
  ]
}
```

## CORS Configuration

The API is configured with CORS enabled for all origins (`*`). For production, update the CORS settings in `api.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Specify your domain
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)
```

## Development

The API uses:
- **FastAPI** - Modern, fast web framework
- **SQLAlchemy** - Database ORM
- **Pydantic** - Data validation and schemas
- **Uvicorn** - ASGI server

## Notes

- All endpoints support pagination with `skip` and `limit` parameters
- The default limit is 100 items, max is 1000
- The API automatically logs all requests and errors
- Database sessions are properly managed and closed after each request
