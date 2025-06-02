# Use regular Python image (not Lambda)
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app folder (which contains main.py)
COPY app/ ./app/

# App Runner uses port 8000
EXPOSE 8000

# Run FastAPI with correct module path
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]