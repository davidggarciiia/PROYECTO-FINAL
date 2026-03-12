"""workers/celery_app.py — Configuración de Celery."""
from celery import Celery
from config import get_settings

s = get_settings()

celery_app = Celery(
    "georetail",
    broker=s.CELERY_BROKER_URL,
    backend=s.CELERY_RESULT_BACKEND,
    include=["workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Madrid",
    enable_utc=True,
    task_track_started=True,
    result_expires=3600,
)
