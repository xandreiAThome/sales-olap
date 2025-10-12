# Use Python 3.13 slim image
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install system dependencies for MySQL client
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire ETL application
COPY . .

# Make entrypoint script executable
RUN chmod +x entrypoint.sh

# Set Python to run in unbuffered mode for better logging
ENV PYTHONUNBUFFERED=1

# Use entrypoint script to run migrations first, then ETL
ENTRYPOINT ["./entrypoint.sh"]
