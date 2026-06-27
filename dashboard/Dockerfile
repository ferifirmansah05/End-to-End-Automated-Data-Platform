FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

EXPOSE 8050

CMD ["gunicorn", "app:server", "-b", "0.0.0.0:8050", "--workers=2", "--timeout=120"]
