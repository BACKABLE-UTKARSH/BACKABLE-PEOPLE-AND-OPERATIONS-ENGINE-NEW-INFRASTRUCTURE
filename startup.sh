#!/bin/bash
gunicorn -w 1 -k uvicorn.workers.UvicornWorker BACKABLE_NEW_INFRASTRUCTURE_PEOPLE_AND_OPERATION_ENGINE:app --bind 0.0.0.0:8000 --timeout 600 --access-logfile '-' --error-logfile '-' --log-level info
