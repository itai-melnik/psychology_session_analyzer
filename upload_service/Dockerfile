# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# NEW: Force Python to print logs immediately
ENV PYTHONUNBUFFERED=1

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Run the application
# host 0.0.0.0 is crucial for Docker containers to be accessible
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]