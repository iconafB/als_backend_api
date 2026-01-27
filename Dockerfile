FROM python:3.13

# prevents python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt requirements.txt

RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt && \
    # Remove the WRONG jwt package if it exists (it breaks PyJWT exceptions)
    python -m pip uninstall -y jwt || true && \
    # Ensure PyJWT is installed (and correct version)
    python -m pip install --no-cache-dir "PyJWT==2.10.1" && \
    # Build-time sanity check: fail the build if 'jwt' dist is present
    python -c "import importlib.metadata as m; d={dist.metadata['Name']:dist.version for dist in m.distributions()}; assert 'jwt' not in d, f\"BAD: jwt dist installed: {d.get('jwt')}\"; assert d.get('PyJWT')=='2.10.1', f\"BAD: PyJWT version: {d.get('PyJWT')}\"; print('OK: PyJWT', d.get('PyJWT'))"

# Copy app
COPY . .

EXPOSE 8005

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8005"]
