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
    # Fiabilidad de tareas: confirmar SOLO tras completar (no al recibir)
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    # Timeouts: aviso suave a 1h, kill hard a 1h5m
    task_soft_time_limit=3600,
    task_time_limit=3900,
)
