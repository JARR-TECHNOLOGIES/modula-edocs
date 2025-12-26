FROM python:3.14-slim

# Prevent Python from buffering logs
ENV PYTHONUNBUFFERED=1

# System deps
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application into a package directory
COPY . /app/job

# Cloud Run Job entrypoint (package-safe)
ENTRYPOINT ["python", "-m", "job.main"]
