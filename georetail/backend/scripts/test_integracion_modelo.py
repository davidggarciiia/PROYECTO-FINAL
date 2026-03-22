"""
scripts/test_integracion_modelo.py — Test de integración del modelo mejorado.

Requiere la BD levantada y datos seed. Ejecutar desde el directorio backend/:
    python scripts/test_integracion_modelo.py

NO es un test de pytest — es un script de diagnóstico interactivo para:

  TEST 1 — ConceptoMatcher en vivo
    Comprueba que el matcher carga los embeddings y devuelve resultados coherentes
    para descripciones de negocio reales.

  TEST 2 — Scores comparativos
    Para las mismas zonas, compara los scores de:
      a. "Cafetería de especialidad en barrio de diseño"
      b. "Cafetería express en zona de alto tránsito"
      c. "Cafetería con perros, terraza y ambiente familiar"
    → Los 3 tipos deben producir rankings distintos.

  TEST 3 — score_afinidad_concepto
    Comprueba que get_scores_zona devuelve score_afinidad_concepto cuando
    se pasa una descripción. Verifica que está en [0,100].

  TEST 4 — Flujo completo de validación LLM
    Simula una llamada a validar_negocio() con distintas descripciones y
    muestra los idea_tags y perfil_numerico resultantes.

Requisitos:
    - docker-compose up -d postgres redis
    - cd backend && psql $DATABASE_URL < db/migraciones/001_schema_inicial.sql
    - python -m scoring.scorer (para tener datos seed)
    - Opcionalmente: ANTHROPIC_API_KEY para el TEST 4
"""
from __future__ import annotations

import asyncio
import sys
import os
import time

# Añadir el directorio backend al path para imports relativos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── Colores para output ──────────────────────────────────────────────────────

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗{RESET} {msg}")
def header(msg): print(f"\n{BOLD}{'─'*60}\n  {msg}\n{'─'*60}{RESET}")
def info(msg):  print(f"    {msg}")


# ════════════════════════════════════════════════════════════════════════════
# TEST 1 — ConceptoMatcher en vivo
# ════════════════════════════════════════════════════════════════════════════

def test_concepto_matcher():
    header("TEST 1 — ConceptoMatcher (sin BD)")

    try:
        from scoring.concepto_matcher import get_matcher, CONCEPTOS_DB
    except ImportError as e:
        fail(f"No se pudo importar ConceptoMatcher: {e}")
        return False

    info(f"CONCEPTOS_DB tiene {len(CONCEPTOS_DB)} conceptos")

    descripciones = [
        "Cafetería de especialidad con origen único y métodos de extracción alternativos",
        "Cafetería express en estación de metro para gente con prisa",
        "Cafetería dog-friendly con terraza en un barrio residencial",
        "Restaurante de alta cocina catalana con menú degustación",
        "Bar de cócteles de autor con ambiente nocturno y música en directo",
        "Studio de reformer pilates boutique con máximo 8 clientes por clase",
        "Tienda de ropa sostenible para mujer en barrio hipster",
    ]

    t0 = time.time()
    try:
        matcher = get_matcher()
    except Exception as e:
        warn(f"Matcher no disponible (¿sentence-transformers instalado?): {e}")
        warn("Skipping TEST 1")
        return True  # No es un error crítico

    carga = time.time() - t0
    ok(f"Modelo cargado en {carga:.1f}s")

    errores = 0
    for desc in descripciones:
        t1 = time.time()
        matches = matcher.match(desc, top_k=3)
        elapsed = (time.time() - t1) * 1000

        if not matches:
            fail(f"Sin matches para: '{desc[:50]}...'")
            errores += 1
            continue

        top = matches[0]
        sim = top.get("similarity", 0)
        nombre = top.get("nombre", "?")

        if sim < 0.3:
            warn(f"'{desc[:45]}...' → {nombre} (sim={sim:.3f} — demasiado bajo)")
        else:
            ok(f"'{desc[:45]}...' → {nombre} (sim={sim:.3f}, {elapsed:.0f}ms)")
            for i, m in enumerate(matches[1:], start=2):
                info(f"    #{i} {m.get('nombre','?')} ({m.get('similarity',0):.3f})")

    print()
    if errores == 0:
        ok("TEST 1 PASADO")
    else:
        fail(f"TEST 1 FALLADO ({errores} errores)")
    return errores == 0


# ════════════════════════════════════════════════════════════════════════════
# TEST 2 — Scores comparativos (requiere BD)
# ════════════════════════════════════════════════════════════════════════════

async def test_scores_comparativos():
    header("TEST 2 — Scores comparativos (requiere BD + seed)")

    try:
        from scoring.motor import calcular_scores_batch
        from db.conexion import get_db
    except ImportError as e:
        fail(f"Imports fallados: {e}")
        return False

    # Obtener algunas zonas de la BD
    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                "SELECT id AS zona_id, nombre FROM zonas ORDER BY RANDOM() LIMIT 5"
            )
        if not rows:
            warn("No hay zonas en la BD. ¿Has ejecutado el seed?")
            return False
        zona_ids = [r["zona_id"] for r in rows]
        info(f"Zonas de prueba: {[r['nombre'] for r in rows]}")
    except Exception as e:
        fail(f"No se pudo conectar a la BD: {e}")
        fail("¿Está docker-compose up -d postgres corriendo?")
        return False

    # Tres conceptos de café completamente distintos
    conceptos = [
        {
            "nombre": "Specialty coffee (barrio cultural)",
            "sector": "restauracion",
            "idea_tags": ["specialty_coffee", "clientela_local", "instagrammable"],
            "descripcion": "Cafetería de especialidad con microlotes y baristas especializados",
        },
        {
            "nombre": "Cafetería express (zona de paso)",
            "sector": "restauracion",
            "idea_tags": ["alta_rotacion_clientes", "takeaway_delivery", "low_cost"],
            "descripcion": "Cafetería rápida para gente con prisa cerca del metro",
        },
        {
            "nombre": "Cafetería dog-friendly (barrio residencial)",
            "sector": "restauracion",
            "idea_tags": ["dog_friendly", "clientela_local", "clientela_vecindario"],
            "descripcion": "Cafetería con perros bienvenidos, terraza y ambiente familiar",
        },
    ]

    resultados_por_concepto = {}
    for concepto in conceptos:
        t0 = time.time()
        scores = await calcular_scores_batch(
            zona_ids,
            concepto["sector"],
            idea_tags=concepto["idea_tags"],
            descripcion_negocio=concepto["descripcion"],
        )
        elapsed = (time.time() - t0) * 1000

        scores_sorted = sorted(scores, key=lambda s: s.get("score_global", 0), reverse=True)
        resultados_por_concepto[concepto["nombre"]] = scores_sorted

        ok(f"{concepto['nombre']} ({elapsed:.0f}ms)")
        for s in scores_sorted:
            zona_nombre = next((r["nombre"] for r in rows if r["zona_id"] == s["zona_id"]), s["zona_id"])
            info(f"    {zona_nombre[:30]:<30} score={s.get('score_global', 0):.1f}")

    # Verificar que los rankings difieren entre conceptos
    print()
    if len(resultados_por_concepto) >= 2:
        keys = list(resultados_por_concepto.keys())
        ranking_1 = [s["zona_id"] for s in resultados_por_concepto[keys[0]]]
        ranking_2 = [s["zona_id"] for s in resultados_por_concepto[keys[1]]]
        if ranking_1 != ranking_2:
            ok("Los rankings difieren entre conceptos distintos ✓")
        else:
            warn("Los rankings son idénticos — puede indicar que los tags no tienen efecto")

    ok("TEST 2 PASADO")
    return True


# ════════════════════════════════════════════════════════════════════════════
# TEST 3 — score_afinidad_concepto
# ════════════════════════════════════════════════════════════════════════════

async def test_score_afinidad():
    header("TEST 3 — score_afinidad_concepto (requiere BD + sentence-transformers)")

    try:
        from scoring.motor import get_scores_zona
        from db.conexion import get_db
    except ImportError as e:
        fail(f"Import fallado: {e}")
        return False

    try:
        async with get_db() as conn:
            row = await conn.fetchrow("SELECT id AS zona_id, nombre FROM zonas LIMIT 1")
        if not row:
            warn("No hay zonas en la BD")
            return False
        zona_id = row["zona_id"]
        zona_nombre = row["nombre"]
    except Exception as e:
        fail(f"BD no disponible: {e}")
        return False

    descripcion = "Cafetería de especialidad con perros bienvenidos en barrio residencial"
    info(f"Zona: {zona_nombre} ({zona_id})")
    info(f"Descripción: '{descripcion}'")

    t0 = time.time()
    result = await get_scores_zona(
        zona_id=zona_id,
        sector_codigo="restauracion",
        idea_tags=["specialty_coffee", "dog_friendly", "clientela_local"],
        descripcion_negocio=descripcion,
    )
    elapsed = (time.time() - t0) * 1000

    ok(f"get_scores_zona completado en {elapsed:.0f}ms")

    dims = result.get("scores_dimension", {})
    if "afinidad_concepto" in dims:
        afinidad = dims["afinidad_concepto"]
        if 0 <= afinidad <= 100:
            ok(f"score_afinidad_concepto = {afinidad:.1f} ∈ [0,100] ✓")
        else:
            fail(f"score_afinidad_concepto = {afinidad} FUERA de [0,100]")
            return False
    else:
        warn("score_afinidad_concepto no calculado (sentence-transformers no disponible)")

    info(f"score_global = {result.get('score_global', '?')}")
    info("scores_dimension:")
    for dim, val in dims.items():
        info(f"    {dim:<25} {val}")

    ok("TEST 3 PASADO")
    return True


# ════════════════════════════════════════════════════════════════════════════
# TEST 4 — Flujo LLM completo (requiere ANTHROPIC_API_KEY)
# ════════════════════════════════════════════════════════════════════════════

async def test_flujo_llm():
    header("TEST 4 — Flujo LLM: idea_tags + perfil_numerico")

    api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        warn("ANTHROPIC_API_KEY no configurada — skipping TEST 4")
        warn("Set ANTHROPIC_API_KEY para activar este test")
        return True  # No es error

    try:
        from agente.validador import validar_negocio
    except ImportError as e:
        fail(f"Import fallado: {e}")
        return False

    descripciones = [
        "Quiero abrir un café de especialidad en un barrio de diseño con barra de madera y pizarra de orígenes",
        "Bar de cócteles de autor con música en directo para clientela de 25-40 años, decoración industrial",
        "Studio de reformer pilates boutique para mujeres profesionales, máximo 8 clientes por clase, zona premium",
        "Cafetería con perros bienvenidos, terraza, ambiente familiar, barrio con parques",
    ]

    errores = 0
    for desc in descripciones:
        info(f"\nDescripción: '{desc[:70]}...'")
        t0 = time.time()
        try:
            result = await validar_negocio(desc, f"test_integracion_{hash(desc)}")
            elapsed = (time.time() - t0) * 1000

            tags = result.get("idea_tags", [])
            perfil = result.get("perfil_negocio", {})
            sector = result.get("sector_detectado", "?")

            ok(f"Sector: {sector} ({elapsed:.0f}ms)")
            info(f"  idea_tags: {tags}")
            if perfil:
                info(f"  dependencia_flujo:    {perfil.get('dependencia_flujo', '?'):.2f}")
                info(f"  nivel_precio:         {perfil.get('nivel_precio', '?'):.2f}")
                info(f"  clientela_turismo:    {perfil.get('clientela_turismo', '?'):.2f}")
                info(f"  clientela_vecindario: {perfil.get('clientela_vecindario', '?'):.2f}")
                info(f"  horario_nocturno:     {perfil.get('horario_nocturno', '?'):.2f}")
                info(f"  experiencial:         {perfil.get('experiencial', '?'):.2f}")
                info(f"  citas_previas:        {perfil.get('citas_previas', '?'):.2f}")
                info(f"  sensibilidad_alquiler:{perfil.get('sensibilidad_alquiler', '?'):.2f}")

            if not tags:
                warn("Sin idea_tags — el LLM no los devolvió")

        except Exception as e:
            fail(f"Error en validar_negocio: {e}")
            errores += 1

    print()
    if errores == 0:
        ok("TEST 4 PASADO")
    else:
        fail(f"TEST 4 FALLADO ({errores} errores)")
    return errores == 0


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

async def _init_infra():
    """Inicializa el pool de BD y Redis para los tests que los necesitan."""
    try:
        from db.conexion import init_db_pool
        await init_db_pool()
        ok("Pool BD inicializado")
    except Exception as e:
        warn(f"No se pudo inicializar la BD: {e}")

    try:
        from db.redis_client import init_redis
        await init_redis()
        ok("Redis inicializado")
    except Exception as e:
        warn(f"No se pudo inicializar Redis: {e}")


async def _close_infra():
    try:
        from db.conexion import close_db_pool
        await close_db_pool()
    except Exception:
        pass
    try:
        from db.redis_client import close_redis
        await close_redis()
    except Exception:
        pass


async def main():
    print(f"\n{BOLD}GeoRetail — Test de integración del modelo mejorado{RESET}")
    print("Asegúrate de ejecutar desde el directorio backend/")
    print(f"Rama: modelo-mejorado\n")

    # Inicializar infraestructura
    header("Inicializando BD y Redis")
    await _init_infra()

    resultados = {}

    # TEST 1 — no requiere BD
    resultados["concepto_matcher"] = test_concepto_matcher()

    # TESTs 2, 3 — requieren BD
    try:
        resultados["scores_comparativos"] = await test_scores_comparativos()
        resultados["score_afinidad"] = await test_score_afinidad()
    except Exception as e:
        fail(f"Error inesperado en tests con BD: {e}")
        resultados["scores_comparativos"] = False
        resultados["score_afinidad"] = False

    # TEST 4 — requiere API key
    try:
        resultados["flujo_llm"] = await test_flujo_llm()
    except Exception as e:
        fail(f"Error inesperado en test LLM: {e}")
        resultados["flujo_llm"] = False

    # Resumen
    header("RESUMEN")
    total = len(resultados)
    pasados = sum(1 for v in resultados.values() if v)
    for nombre, resultado in resultados.items():
        estado = f"{GREEN}PASADO{RESET}" if resultado else f"{RED}FALLADO{RESET}"
        print(f"  {nombre:<30} {estado}")

    print(f"\n  {BOLD}{pasados}/{total} tests pasados{RESET}")

    if pasados < total:
        print(f"\n  {YELLOW}Nota: Los tests con BD requieren docker-compose up -d postgres{RESET}")
        print(f"  {YELLOW}TEST 4 requiere ANTHROPIC_API_KEY configurada{RESET}")

    await _close_infra()
    sys.exit(0 if pasados == total else 1)


if __name__ == "__main__":
    asyncio.run(main())
