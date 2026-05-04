#!/bin/bash
# Fail fast if the persistent database mount is missing or unusable.
if [ ! -d /mnt/data ]; then
    echo "ERROR: /mnt/data does not exist; expected persistent Azure Files mount is missing." >&2
    exit 1
fi

if ! mountpoint -q /mnt/data; then
    echo "ERROR: /mnt/data is not a mounted filesystem; refusing to start on ephemeral storage." >&2
    exit 1
fi

if [ ! -w /mnt/data ]; then
    echo "ERROR: /mnt/data is not writable; refusing to start." >&2
    exit 1
fi

# Run database migrations (handled by app lifespan)
# Start gunicorn with uvicorn worker
gunicorn soa_builder.web.app:app \
    --bind 0.0.0.0:8000 \
    --workers 1 \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -