FROM python:3.11-slim

# prevents python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONBUFFERED=1

WORKDIR /app

# System deps

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app

COPY . .
#Make entrypoint executable
RUN chmod +x docker/entrypoint.sh
#EXPOSE PORT 

EXPOSE 8000

ENTRYPOINT [ "docker/entrypoint" ]
