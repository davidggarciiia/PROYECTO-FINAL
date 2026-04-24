"""
exportar/generador.py — Generación de PDF con WeasyPrint + Jinja2.

El PDF incluye para cada zona:
  1. Ficha con score global y scores por dimensión
  2. Top 5 factores SHAP (si XGBoost activo)
  3. Análisis IA (texto del LLM)
  4. Análisis financiero (si incluir_financiero=True)
  5. Mapa estático de Mapbox (imagen PNG)
"""
from __future__ import annotations
import base64, logging, os
from datetime import datetime

import httpx
from jinja2 import Environment, BaseLoader, select_autoescape

from config import get_settings
from db.conexion import get_db
from db.financiero import get_analisis_financiero
from db.sesiones import get_sesion
from db.zonas import get_zona_completa

logger = logging.getLogger(__name__)


async def encolar_generacion_pdf(
    pdf_id: str,
    session_id: str,
    zona_ids: list[str],
    opciones: dict,
) -> None:
    """Encola la generación del PDF en Celery."""
    from workers.tasks import generar_pdf_task
    generar_pdf_task.delay(pdf_id, session_id, zona_ids, opciones)


async def generar(pdf_id: str, session_id: str, zona_ids: list[str], opciones: dict) -> str:
    """
    Genera el PDF completo y lo guarda en disco.
    Returns: ruta al fichero PDF.
    """
    settings = get_settings()
    sesion = await get_sesion(session_id)
    sector = (sesion or {}).get("perfil",{}).get("sector","")
    empresa = opciones.get("nombre_empresa","")

    # Recopilar datos de cada zona
    zonas_data = []
    for zona_id in zona_ids[:5]:  # máximo 5 zonas en el PDF
        zona = await get_zona_completa(zona_id, sector)
        if not zona:
            continue
        fin = await get_analisis_financiero(session_id, zona_id)
        zonas_data.append({"zona": zona, "financiero": fin})

    # Mapa estático con todas las zonas marcadas
    mapa_b64 = await _mapa_estatico(zonas_data, settings.MAPBOX_PUBLIC_TOKEN)

    # Renderizar HTML con Jinja2
    html = _renderizar_html(
        empresa=empresa,
        sector=sector,
        zonas_data=zonas_data,
        mapa_b64=mapa_b64,
        incluir_financiero=opciones.get("incluir_financiero", True),
        incluir_competencia=opciones.get("incluir_competencia", True),
        fecha=datetime.now().strftime("%d/%m/%Y"),
    )

    # Convertir HTML → PDF
    import weasyprint
    ruta = os.path.join(settings.EXPORTS_DIR, f"{pdf_id}.pdf")
    os.makedirs(settings.EXPORTS_DIR, exist_ok=True)
    weasyprint.HTML(string=html).write_pdf(ruta)
    logger.info("PDF generado: %s", ruta)
    return ruta


async def _mapa_estatico(zonas_data: list[dict], token: str) -> str:
    """Genera imagen PNG del mapa con todas las zonas marcadas. Devuelve base64."""
    if not token or not zonas_data:
        return ""

    # Construir overlays de pins para Mapbox Static API
    pins = []
    colores = {"verde": "00d4a1", "amarillo": "f5a623", "rojo": "ff5c5c"}
    for z in zonas_data:
        score = z["zona"].get("score_global") or 50
        color = colores["verde"] if score >= 75 else colores["amarillo"] if score >= 50 else colores["rojo"]
        lat = z["zona"].get("lat")
        lng = z["zona"].get("lng")
        if lat and lng:
            pins.append(f"pin-s+{color}({lng},{lat})")

    if not pins:
        return ""

    overlay = ",".join(pins)
    url = (f"https://api.mapbox.com/styles/v1/mapbox/light-v11/static/"
           f"{overlay}/auto/800x400@2x?access_token={token}&padding=50")

    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(url)
            r.raise_for_status()
            return base64.b64encode(r.content).decode()
    except Exception as e:
        logger.warning("Mapa estático error: %s", e)
        return ""


def _renderizar_html(empresa, sector, zonas_data, mapa_b64,
                      incluir_financiero, incluir_competencia, fecha) -> str:
    template_str = r"""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:'Inter',sans-serif; color:#1a1a2e; font-size:11pt; }
  .portada { page-break-after:always; padding:60px 50px; }
  .portada h1 { font-size:32pt; color:#0a0a23; margin-bottom:8px; }
  .portada .sub { font-size:14pt; color:#666; margin-bottom:40px; }
  .portada .meta { font-size:10pt; color:#999; }
  .mapa { width:100%; border-radius:8px; margin:30px 0; }
  .zona-seccion { page-break-before:always; padding:40px 50px; }
  .zona-header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:24px; }
  .zona-nombre { font-size:20pt; font-weight:700; }
  .zona-barrio { font-size:11pt; color:#666; margin-top:4px; }
  .badge-score { font-size:28pt; font-weight:700; padding:12px 20px; border-radius:12px; text-align:center; }
  .badge-verde { background:#e6faf5; color:#00a07a; }
  .badge-amarillo { background:#fff8e6; color:#c07a00; }
  .badge-rojo { background:#ffecec; color:#cc0000; }
  .kpis { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:24px; }
  .kpi { background:#f8f8fc; border-radius:8px; padding:12px; text-align:center; }
  .kpi-valor { font-size:16pt; font-weight:700; color:#1a1a2e; }
  .kpi-label { font-size:8pt; color:#888; margin-top:2px; }
  .analisis-box { background:#f0f4ff; border-radius:8px; padding:16px; margin-bottom:16px; }
  .analisis-box h3 { font-size:11pt; font-weight:600; margin-bottom:8px; color:#2d3a8c; }
  .pros-cons { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:16px; }
  .pros { background:#e6faf5; border-radius:8px; padding:12px; }
  .cons { background:#fff0f0; border-radius:8px; padding:12px; }
  .pros h4 { color:#00a07a; font-size:9pt; margin-bottom:6px; }
  .cons h4 { color:#cc3333; font-size:9pt; margin-bottom:6px; }
  ul { padding-left:16px; }
  li { font-size:9.5pt; line-height:1.5; }
  .fin-tabla { width:100%; border-collapse:collapse; margin-top:12px; font-size:9pt; }
  .fin-tabla th { background:#1a1a2e; color:white; padding:6px 10px; text-align:left; }
  .fin-tabla td { padding:5px 10px; border-bottom:1px solid #eee; }
  .fin-tabla tr:nth-child(even) td { background:#f8f8fc; }
  .recomendacion { display:inline-block; padding:6px 14px; border-radius:20px; font-size:9pt; font-weight:600; margin-top:8px; }
  .rec-recomendado { background:#e6faf5; color:#00a07a; }
  .rec-reservas { background:#fff8e6; color:#c07a00; }
  .rec-no { background:#ffecec; color:#cc0000; }
  .notas { page-break-before:always; padding:40px 50px; }
  .notas h2 { font-size:14pt; margin-bottom:12px; }
  .notas p { font-size:9pt; color:#666; line-height:1.6; margin-bottom:8px; }
</style>
</head>
<body>

<div class="portada">
  <h1>Análisis de Ubicación</h1>
  {% if empresa %}<div class="sub">{{ empresa }}</div>{% endif %}
  <p class="meta">Sector: {{ sector or "Retail" }} &nbsp;·&nbsp; Fecha: {{ fecha }}</p>
  {% if mapa_b64 %}
  <img class="mapa" src="data:image/png;base64,{{ mapa_b64 }}" alt="Mapa de zonas analizadas"/>
  {% endif %}
  <p class="meta" style="margin-top:20px;">{{ zonas_data|length }} zona(s) analizada(s)</p>
</div>

{% for item in zonas_data %}
{% set z = item.zona %}
{% set f = item.financiero %}
{% set score = z.score_global or 0 %}
{% set badge_cls = "badge-verde" if score >= 75 else ("badge-amarillo" if score >= 50 else "badge-rojo") %}
{% set rec = z.analisis_ia.recomendacion_final if z.analisis_ia else "" %}

<div class="zona-seccion">
  <div class="zona-header">
    <div>
      <div class="zona-nombre">{{ z.nombre }}</div>
      <div class="zona-barrio">{{ z.barrio }} · {{ z.distrito }}</div>
      {% if z.direccion %}<div class="zona-barrio" style="margin-top:4px;">{{ z.direccion }}</div>{% endif %}
    </div>
    <div class="badge-score {{ badge_cls }}">{{ score|round(0)|int }}<div style="font-size:9pt;font-weight:400;">/ 100</div></div>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="kpi-valor">{% if z.probabilidad_supervivencia %}{{ (z.probabilidad_supervivencia*100)|round(0)|int }}%{% else %}—{% endif %}</div>
      <div class="kpi-label">Superv. 3 años</div>
    </div>
    <div class="kpi">
      <div class="kpi-valor">{{ z.alquiler_mensual|round(0)|int if z.alquiler_mensual else "—" }}€</div>
      <div class="kpi-label">Alquiler/mes</div>
    </div>
    <div class="kpi">
      <div class="kpi-valor">{{ z.m2|round(0)|int if z.m2 else "—" }}m²</div>
      <div class="kpi-label">Superficie</div>
    </div>
    <div class="kpi">
      <div class="kpi-valor">{{ z.num_lineas_transporte or "—" }}</div>
      <div class="kpi-label">Líneas transporte</div>
    </div>
  </div>

  {% if z.analisis_ia %}
  <div class="analisis-box">
    <h3>Análisis IA</h3>
    <p style="font-size:9.5pt;line-height:1.6;">{{ z.analisis_ia.resumen }}</p>
  </div>

  <div class="pros-cons">
    <div class="pros">
      <h4>✓ PUNTOS FUERTES</h4>
      <ul>{% for p in z.analisis_ia.puntos_fuertes[:3] %}<li>{{ p }}</li>{% endfor %}</ul>
    </div>
    <div class="cons">
      <h4>✗ PUNTOS DÉBILES</h4>
      <ul>{% for p in z.analisis_ia.puntos_debiles[:2] %}<li>{{ p }}</li>{% endfor %}</ul>
    </div>
  </div>

  <div>
    <span class="recomendacion {% if 'Recomendado' == rec %}rec-recomendado{% elif 'reservas' in rec %}rec-reservas{% else %}rec-no{% endif %}">
      {{ rec }}
    </span>
    {% if z.analisis_ia.razon_recomendacion %}
    <p style="font-size:9pt;color:#666;margin-top:6px;">{{ z.analisis_ia.razon_recomendacion }}</p>
    {% endif %}
  </div>
  {% endif %}

  {% if incluir_financiero and f %}
  <div style="margin-top:24px;">
    <h3 style="font-size:12pt;margin-bottom:10px;">Análisis Financiero</h3>
    <table class="fin-tabla">
      <thead><tr><th>Concepto</th><th>Conservador</th><th>Optimista</th></tr></thead>
      <tbody>
        <tr><td>Inversión inicial</td><td colspan="2" style="text-align:center;">{{ f.inversion_total|round(0)|int }}€</td></tr>
        <tr><td>Ingresos año 1</td><td>{{ f.ingresos_anuales_conservador|round(0)|int }}€</td><td>{{ f.ingresos_anuales_optimista|round(0)|int }}€</td></tr>
        <tr><td>EBITDA año 1</td><td>{{ f.ebitda_conservador|round(0)|int }}€</td><td>{{ f.ebitda_optimista|round(0)|int }}€</td></tr>
        <tr><td>ROI 3 años</td><td>{{ (f.roi_3a_conservador*100)|round(1) }}%</td><td>{{ (f.roi_3a_optimista*100)|round(1) }}%</td></tr>
        <tr><td>Payback</td><td>{{ f.payback_meses_conservador }} meses</td><td>{{ f.payback_meses_optimista }} meses</td></tr>
      </tbody>
    </table>
  </div>
  {% endif %}

  {% if incluir_competencia and z.competidores_cercanos %}
  <div style="margin-top:24px;">
    <h3 style="font-size:12pt;margin-bottom:10px;">Competencia directa (300m)</h3>
    <table class="fin-tabla">
      <thead><tr><th>Nombre</th><th>Distancia</th><th>Rating</th></tr></thead>
      <tbody>
        {% for c in z.competidores_cercanos[:8] %}
        {% if c.es_competencia_directa %}
        <tr><td>{{ c.nombre }}</td><td>{{ c.distancia_m|round(0)|int }}m</td><td>{{ c.rating or "—" }}</td></tr>
        {% endif %}
        {% endfor %}
      </tbody>
    </table>
  </div>
  {% endif %}
</div>
{% endfor %}

<div class="notas">
  <h2>Notas metodológicas</h2>
  <p><strong>Fuentes de datos:</strong> Open Data Ajuntament de Barcelona (aforadors, padró, cens locals), 
  Idealista API (precios alquiler), Google Places API (negocios activos, ratings), 
  INE Encuesta Anual de Comercio (benchmarks sectoriales), 
  Convenios Colectivos Cataluña 2024 (costes laborales).</p>
  <p><strong>Scoring:</strong> Los scores de 0-100 se calculan combinando 8 dimensiones ponderadas 
  según el sector comercial. La probabilidad de supervivencia a 3 años se calcula con un modelo 
  XGBoost entrenado sobre datos del Registre Mercantil de Barcelona.</p>
  <p><strong>Análisis financiero:</strong> Las estimaciones se derivan automáticamente de datos de 
  mercado reales. Los escenarios conservador y optimista representan el 60% y 115% de la 
  capacidad estimada respectivamente. La proyección incluye una curva de arranque de 12 meses.</p>
  <p><strong>Importante:</strong> Este análisis es orientativo. Los datos pueden variar. 
  Consulte con un experto local antes de tomar decisiones de inversión.</p>
</div>

</body>
</html>
""".strip()

    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    tmpl = env.from_string(template_str)
    return tmpl.render(
        empresa=empresa, sector=sector, fecha=fecha,
        mapa_b64=mapa_b64, zonas_data=zonas_data,
        incluir_financiero=incluir_financiero,
        incluir_competencia=incluir_competencia,
    )
