"""
pipelines/scraping/models.py — Modelo Pydantic unificado para anuncios inmobiliarios.

InmueblePortal representa un anuncio de CUALQUIER portal (Idealista, Fotocasa,
Habitaclia, Milanuncios, Pisos.com) normalizado al mismo esquema.

Todos los scrapers devuelven list[dict] con estos campos mínimos garantizados:
    id, fuente, precio, m2, precio_m2, lat, lng, direccion, distrito, barrio, url

Los campos adicionales (escaparate, esquina, dias_publicado...) solo los
extrae el scraper enriquecido de Idealista — para el resto son None.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class InmueblePortal(BaseModel):
    """
    Anuncio de local/oficina/vivienda normalizado desde cualquier portal.
    Persistido en la tabla inmuebles_portales de PostgreSQL.
    """

    # ── Identificación ────────────────────────────────────────────────────────
    portal_id:      str                          # ID único dentro del portal (ej: "idealista_12345678")
    fuente:         str                          # "idealista" | "fotocasa" | "habitaclia" | "milanuncios" | "pisos"
    url:            str
    tipo_operacion: str  = "alquiler-locales"    # "alquiler-locales" | "venta-locales" | "alquiler-viviendas"
    tipo_inmueble:  str  = "local"               # "local" | "oficina" | "nave" | "vivienda" | "desconocido"
    fecha_scraping: datetime = Field(default_factory=datetime.utcnow)

    # ── Precio ────────────────────────────────────────────────────────────────
    precio:          Optional[float] = None
    precio_m2:       Optional[float] = None
    precio_anterior: Optional[float] = None

    # ── Superficie ────────────────────────────────────────────────────────────
    superficie_util:       Optional[float] = None
    superficie_construida: Optional[float] = None

    # ── Localización ──────────────────────────────────────────────────────────
    titulo:        Optional[str]   = None
    direccion:     Optional[str]   = None
    barrio:        Optional[str]   = None
    distrito:      Optional[str]   = None
    codigo_postal: Optional[str]   = None
    lat:           Optional[float] = None
    lon:           Optional[float] = None

    # ── Campos enriquecidos (solo Idealista por ahora) ────────────────────────
    anyo_construccion:   Optional[int]  = None
    estado_conservacion: Optional[str]  = None   # "reformado" | "buen_estado" | "a_reformar" | "obra_nueva"
    certificado_energia: Optional[str]  = None   # "A".."G"
    escaparate:          Optional[bool] = None
    esquina:             Optional[bool] = None
    sotano:              Optional[bool] = None
    altura_libre_m:      Optional[float] = None
    potencia_kw:         Optional[float] = None
    dias_publicado:      Optional[int]  = None
    es_particular:       bool           = False
    agente_nombre:       Optional[str]  = None
    num_fotos:           int            = 0
    etiquetas:           list[str]      = Field(default_factory=list)

    @field_validator("precio", "precio_m2", "superficie_util", "superficie_construida", mode="before")
    @classmethod
    def limpiar_numero(cls, v: Any) -> Optional[float]:
        if isinstance(v, str):
            limpio = re.sub(r"[€$\s]", "", v.replace(".", "").replace(",", "."))
            m = re.search(r"-?[\d.]+", limpio)
            try:
                return float(m.group()) if m else None
            except ValueError:
                return None
        return v

    @model_validator(mode="after")
    def calcular_precio_m2(self) -> InmueblePortal:
        if self.precio_m2 is None and self.precio and self.superficie_util:
            if self.superficie_util > 0:
                self.precio_m2 = round(self.precio / self.superficie_util, 2)
        return self


def desde_dict_scraper(raw: dict) -> Optional[InmueblePortal]:
    """
    Convierte el dict genérico que devuelven TODOS los scrapers en un InmueblePortal.

    Campos garantizados en el dict:
        id, fuente, precio, m2, precio_m2, lat, lng, direccion, distrito, barrio, url

    Campos opcionales (solo Idealista enriquecido):
        titulo, codigo_postal, anyo_construccion, estado_conservacion,
        certificado_energia, escaparate, esquina, sotano, altura_libre_m,
        potencia_kw, dias_publicado, es_particular, agente_nombre,
        num_fotos, etiquetas, precio_anterior, superficie_construida,
        tipo_operacion, tipo_inmueble
    """
    if not raw:
        return None

    portal_id = raw.get("id")
    if not portal_id:
        return None

    try:
        return InmueblePortal(
            portal_id=str(portal_id),
            fuente=raw.get("fuente", "desconocido"),
            url=raw.get("url", ""),
            tipo_operacion=raw.get("tipo_operacion", "alquiler-locales"),
            tipo_inmueble=raw.get("tipo_inmueble", "local"),

            precio=raw.get("precio"),
            precio_m2=raw.get("precio_m2"),
            precio_anterior=raw.get("precio_anterior"),

            superficie_util=raw.get("m2") or raw.get("superficie_util"),
            superficie_construida=raw.get("superficie_construida"),

            titulo=raw.get("titulo"),
            direccion=raw.get("direccion") or raw.get("address"),
            barrio=raw.get("barrio") or raw.get("neighborhood"),
            distrito=raw.get("distrito") or raw.get("district"),
            codigo_postal=raw.get("codigo_postal"),
            lat=raw.get("lat") or raw.get("latitude"),
            lon=raw.get("lng") or raw.get("longitude"),

            # Enriquecidos
            anyo_construccion=raw.get("anyo_construccion"),
            estado_conservacion=raw.get("estado_conservacion"),
            certificado_energia=raw.get("certificado_energia"),
            escaparate=raw.get("escaparate"),
            esquina=raw.get("esquina"),
            sotano=raw.get("sotano"),
            altura_libre_m=raw.get("altura_libre_m"),
            potencia_kw=raw.get("potencia_kw"),
            dias_publicado=raw.get("dias_publicado"),
            es_particular=raw.get("es_particular", False),
            agente_nombre=raw.get("agente_nombre"),
            num_fotos=raw.get("num_fotos", 0),
            etiquetas=raw.get("etiquetas") or [],
        )
    except Exception:
        return None
