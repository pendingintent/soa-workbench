#!/bin/bash
# Ensure database directory exists
mkdir -p /mnt/data

# Run database migrations (handled by app lifespan)
# Start gunicorn with uvicorn worker
gunicorn soa_builder.web.app:app \
    --bind 0.0.0.0:8000 \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -