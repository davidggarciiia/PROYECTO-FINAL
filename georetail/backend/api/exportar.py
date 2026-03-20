"""
api/exportar.py — POST /api/exportar  y  GET /api/exportar/download/{pdf_id}

Generación y descarga de informes en PDF.

La generación es una operación lenta (~3-8s) que se delega a Celery:
  - POST /api/exportar → encola la tarea y devuelve la URL de descarga
  - GET  /api/exportar/download/{pdf_id} → descarga el fichero cuando está listo

Almacenamiento:
  - Los PDFs generados se guardan en disco: `/data/exports/{pdf_id}.pdf`
  - Se registran en la tabla `exportaciones` (PostgreSQL) con `expires_at` (+1h)
  - El worker Celery (`workers/celery_app.py`) es el que ejecuta `generar_pdf`

Contenido del PDF (según los parámetros del request):
  - Portada con nombre de empresa y fecha
  - Mapa estático con las zonas seleccionadas (Mapbox Static Images API)
  - Ficha de cada zona: score, dimensiones, SHAP, competidores, alertas
  - Análisis financiero (si se solicita)
  - Información legal resumida
  - Disclaimer

Fuentes de datos del PDF:
  - `zonas` + `locales` + `scores_zona` → datos de zonas
  - `analisis_financieros` → proyección financiera (si ya se calculó antes)
  - Mapbox Static Images API → imagen del mapa
  - `sesiones` → perfil del negocio y nombre de empresa
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from db.sesiones import get_sesion
from db.exportaciones import registrar_exportacion, get_exportacion
from exportar.generador import encolar_generacion_pdf

logger = logging.getLogger(__name__)
router = APIRouter(tags=["exportar"])

# El PDF expira 1 hora después de generarse — suficiente para que el usuario descargue
_TTL_PDF_HORAS = 1


# ─── Request ──────────────────────────────────────────────────────────────────

class ExportarRequest(BaseModel):
    session_id: str
    zona_ids: list[str] = Field(
        ...,
        min_length=1,
        max_length=5,
        description="IDs de las zonas a incluir en el informe (máximo 5)",
    )
    incluir_financiero: bool = Field(
        default=True,
        description="Incluir análisis financiero en el PDF (requiere haber usado /api/financiero)",
    )
    incluir_mapa: bool = Field(
        default=True,
        description="Incluir imagen estática del mapa (Mapbox Static Images API)",
    )
    incluir_competencia: bool = Field(
        default=True,
        description="Incluir tabla de competidores cercanos por zona",
    )
    nombre_empresa: Optional[str] = Field(
        default=None,
        max_length=100,
        description="Nombre que aparecerá en la portada del PDF",
    )


# ─── Responses ────────────────────────────────────────────────────────────────

class ExportarResponse(BaseModel):
    url_descarga: str = Field(
        ...,
        description="Ruta relativa para descargar el PDF (/api/exportar/download/{pdf_id})",
    )
    expires_at: str = Field(
        ...,
        description="Timestamp ISO 8601 de expiración del enlace (+1h desde la generación)",
    )


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post(
    "/exportar",
    response_model=ExportarResponse,
    summary="Generar PDF del informe de zonas (~3-8s)",
)
async def exportar(body: ExportarRequest) -> ExportarResponse:
    """
    Encola la generación del PDF en Celery y devuelve la URL de descarga.

    El PDF se genera de forma asíncrona en el worker Celery.
    El endpoint espera a que termine antes de responder (espera bloqueante del lado del cliente).

    Si el usuario ya tiene un análisis financiero guardado para las zonas solicitadas
    (en `analisis_financieros`), se incluye automáticamente en el PDF sin necesidad
    de recalcular.

    Si `incluir_mapa=True`, el worker llama a:
      GET https://api.mapbox.com/styles/v1/mapbox/light-v11/static/...
      con los pines de las zonas seleccionadas.
    """
    # ── Validar sesión ────────────────────────────────────────────────────────
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(
            status_code=404,
            detail="Sesión no encontrada o expirada.",
        )

    # ── Validar que las zonas existen en la sesión ────────────────────────────
    zonas_sesion = {z["zona_id"] for z in sesion.get("zonas_actuales", [])}
    zonas_invalidas = [zid for zid in body.zona_ids if zid not in zonas_sesion]
    if zonas_invalidas:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Las zonas {zonas_invalidas} no pertenecen a la búsqueda actual. "
                "Solo se pueden exportar zonas de la sesión activa."
            ),
        )

    # ── Registrar la exportación en BD ────────────────────────────────────────
    # Guardamos en `exportaciones` (PostgreSQL) antes de encolar la tarea Celery.
    # Si el worker falla, el registro permite relanzar la tarea sin duplicar.
    expires_at = datetime.now(timezone.utc) + timedelta(hours=_TTL_PDF_HORAS)
    pdf_id = await registrar_exportacion(
        session_id=body.session_id,
        zona_ids=body.zona_ids,
        horas_expiracion=_TTL_PDF_HORAS,
    )

    # ── Encolar generación en Celery ──────────────────────────────────────────
    # `encolar_generacion_pdf` envía la tarea a Celery y espera el resultado.
    # El worker ejecuta `exportar/generador.py`:
    #   1. Recupera datos de zonas de PostgreSQL
    #   2. Llama a Mapbox Static Images API para el mapa
    #   3. Genera el PDF con WeasyPrint
    #   4. Guarda en `/data/exports/{pdf_id}.pdf`
    #   5. Actualiza `exportaciones.estado = 'ok'` en PostgreSQL
    try:
        await encolar_generacion_pdf(
            pdf_id=pdf_id,
            session_id=body.session_id,
            zona_ids=body.zona_ids,
            opciones={
                "incluir_financiero": body.incluir_financiero,
                "incluir_mapa": body.incluir_mapa,
                "incluir_competencia": body.incluir_competencia,
                "nombre_empresa": body.nombre_empresa,
            },
        )
    except Exception as exc:
        logger.error(
            "Error encolando generacion PDF pdf_id=%s: %s", pdf_id, exc, exc_info=True
        )
        raise HTTPException(
            status_code=503,
            detail="Error generando el PDF. Inténtalo de nuevo en unos segundos.",
        )

    return ExportarResponse(
        url_descarga=f"/api/exportar/download/{pdf_id}",
        expires_at=expires_at.isoformat().replace("+00:00", "Z"),
    )


@router.get(
    "/exportar/download/{pdf_id}",
    summary="Descargar el PDF generado (expira 1h después de la generación)",
)
async def exportar_download(pdf_id: str) -> FileResponse:
    """
    Descarga el fichero PDF directamente.
    El Content-Type es `application/pdf`.

    Usar como:
      <a href="/api/exportar/download/{pdf_id}" download>Descargar informe</a>
    O:
      window.open(`/api/exportar/download/${pdfId}`)

    El enlace expira 1 hora después de la generación (campo `expires_at` de
    POST /api/exportar). Pasado ese tiempo, devuelve 404.
    """
    # ── Verificar que el PDF existe y no ha expirado ──────────────────────────
    # Tabla `exportaciones` (PostgreSQL): estado, expires_at, ruta en disco
    exportacion = await get_exportacion(pdf_id)

    if exportacion is None:
        raise HTTPException(
            status_code=404,
            detail="PDF no encontrado. Puede que el enlace haya expirado (TTL: 1h).",
        )

    if exportacion["estado"] != "ok":
        raise HTTPException(
            status_code=404,
            detail=(
                "El PDF aún está generándose. "
                "Inténtalo en unos segundos."
                if exportacion["estado"] == "generando"
                else "El PDF no se pudo generar. Inténtalo de nuevo."
            ),
        )

    now = datetime.now(timezone.utc)
    if exportacion["expires_at"] < now:
        raise HTTPException(
            status_code=404,
            detail="El enlace de descarga ha expirado (TTL: 1h). Regenera el informe.",
        )

    # ── Servir el fichero ─────────────────────────────────────────────────────
    ruta_pdf = exportacion["ruta_disco"]  # /data/exports/{pdf_id}.pdf

    return FileResponse(
        path=ruta_pdf,
        media_type="application/pdf",
        filename=f"georetail-informe-{pdf_id[:8]}.pdf",
        headers={
            # Forzar descarga (no preview en el navegador)
            "Content-Disposition": f'attachment; filename="georetail-informe-{pdf_id[:8]}.pdf"',
        },
    )
