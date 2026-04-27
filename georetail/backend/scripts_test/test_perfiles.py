"""
Smoke test: 5 perfiles de negocio sobre 4 zonas representativas.
Imprime score global + dimensiones + bullets de turismo + dinamismo.
"""
import asyncio
from db.conexion import init_db_pool, close_db_pool
from scoring.motor import get_scores_zona
from scoring.explainability import build_llm_grounding_payload, build_fallback_analysis
from db.zonas import get_zona_completa

ZONAS = [
    ("bcn_barceloneta_02", "Platja Barceloneta"),
    ("bcn_gotic_03",       "Pl. Reial (Gòtic)"),
    ("bcn_esqeix_01",      "C/ Muntaner (Eixample)"),
    ("bcn_standreu_01",    "Gran Sant Andreu"),
]

PERFILES = [
    {
        "nombre": "Restaurante de tapas para turistas",
        "sector": "restauracion",
        "descripcion": "Restaurante de tapas tradicionales para turistas internacionales con menú en inglés y sangría",
        "tags": ["tapas", "turistas", "menu_turistico"],
    },
    {
        "nombre": "Panadería de barrio",
        "sector": "alimentacion",
        "descripcion": "Panadería artesanal de toda la vida para vecinos, pan recién hecho cada mañana, clientela fija",
        "tags": ["panaderia", "barrio", "vecinos"],
    },
    {
        "nombre": "Boutique de moda premium",
        "sector": "moda",
        "descripcion": "Tienda de moda de autor de diseñadores españoles, target medio-alto",
        "tags": ["moda", "premium", "diseñadores"],
    },
    {
        "nombre": "Box CrossFit boutique",
        "sector": "deporte",
        "descripcion": "Box de CrossFit pequeño con clases de máximo 15 personas, perfil joven-adulto profesional",
        "tags": ["crossfit", "gimnasio", "boutique"],
    },
    {
        "nombre": "Clínica fisioterapia residentes",
        "sector": "salud",
        "descripcion": "Clínica de fisioterapia para residentes, sesiones individualizadas, sin público turístico",
        "tags": ["fisioterapia", "salud", "residentes"],
    },
]


def fmt(v):
    return f"{float(v):>5.1f}" if v is not None else "  -- "


async def evaluar_zona(zona_id, sector, descripcion, tags):
    detalle = await get_zona_completa(zona_id, sector)
    if not detalle:
        return None
    scores_data = await get_scores_zona(
        zona_id=zona_id, sector_codigo=sector,
        descripcion_negocio=descripcion, idea_tags=tags,
    )
    perfil = {"sector_codigo": sector, "descripcion": descripcion, "idea_tags": tags}
    grounding = build_llm_grounding_payload(detalle, scores_data, perfil)
    fb = build_fallback_analysis(grounding)
    return {
        "scores": scores_data,
        "explicaciones": fb.get("explicaciones_dimensiones", {}),
        "puntos_fuertes": fb.get("puntos_fuertes", []),
        "puntos_debiles": fb.get("puntos_debiles", []),
    }


async def main():
    await init_db_pool()
    try:
        for p in PERFILES:
            print(f"\n{'═'*100}")
            print(f"  ➤ {p['nombre']}  [sector={p['sector']}]")
            print(f"  «{p['descripcion']}»")
            print(f"{'═'*100}")
            print(f"  {'ZONA':<25} {'GLOB':>5} {'FLUJ':>5} {'DEMO':>5} {'COMP':>5} {'TURI':>5} {'DINA':>5} {'SEGU':>5} {'TRAN':>5} {'PREC':>5} {'AFIN':>5}")
            print(f"  {'-'*25} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*5}")
            resultados = {}
            for zona_id, label in ZONAS:
                r = await evaluar_zona(zona_id, p["sector"], p["descripcion"], p["tags"])
                if not r:
                    print(f"  {label:<25} (sin datos)")
                    continue
                resultados[zona_id] = r
                sd = r["scores"].get("scores_dimension", {})
                print(
                    f"  {label:<25} "
                    f"{fmt(r['scores'].get('score_global'))} "
                    f"{fmt(sd.get('flujo_peatonal'))} "
                    f"{fmt(sd.get('demografia'))} "
                    f"{fmt(sd.get('competencia'))} "
                    f"{fmt(sd.get('turismo'))} "
                    f"{fmt(sd.get('dinamismo'))} "
                    f"{fmt(sd.get('seguridad'))} "
                    f"{fmt(sd.get('transporte'))} "
                    f"{fmt(sd.get('precio_alquiler'))} "
                    f"{fmt(sd.get('afinidad_concepto'))}"
                )

            # Bullets para 2 zonas extremas (1ª y última)
            for zona_id, label in [ZONAS[0], ZONAS[-1]]:
                r = resultados.get(zona_id)
                if not r:
                    continue
                print(f"\n  ▸ {label}")
                expl = r["explicaciones"]
                for dim in ("turismo", "dinamismo"):
                    e = expl.get(dim) or {}
                    hechos = e.get("hechos_clave") or []
                    drivers = e.get("drivers_negativos") or []
                    if hechos:
                        print(f"      [{dim:<10}] {' · '.join(hechos[:3])}")
                    if drivers:
                        print(f"      [{dim:<10} ⚠] {' · '.join(drivers[:2])}")
                if r["puntos_fuertes"]:
                    print(f"      ✓ Fuertes: {' / '.join(r['puntos_fuertes'][:3])}")
                if r["puntos_debiles"]:
                    print(f"      ✗ Débiles: {' / '.join(r['puntos_debiles'][:2])}")
    finally:
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
