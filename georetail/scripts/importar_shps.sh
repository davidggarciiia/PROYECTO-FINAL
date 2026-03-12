#!/bin/bash
# scripts/importar_shps.sh
#
# Carga los shapefiles (archivos con polígonos geográficos) de Barcelona
# en las tablas distritos, barrios y zonas de PostgreSQL.
#
# Fuente de los shapefiles:
#   https://opendata-ajuntament.barcelona.cat/data/ca/dataset/20170706-districtes-barris
#
# Uso:
#   bash scripts/importar_shps.sh
#
# Requisitos:
#   - PostGIS instalado en el servidor/contenedor
#   - ogr2ogr (viene con GDAL — apt install gdal-bin)
#   - Los shapefiles en la carpeta data/shapefiles/
#   - DATABASE_URL definida como variable de entorno o en .env

set -e  # parar si hay algún error

# Cargar variables de entorno desde .env si existe
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

DB_URL="${DATABASE_URL:-postgresql://postgres:password@localhost:5432/georetail}"
SHP_DIR="./data/shapefiles"

echo "=== Importando shapefiles de Barcelona ==="
echo "Base de datos: $DB_URL"
echo "Directorio de shapefiles: $SHP_DIR"
echo ""

# ─── 1. Crear directorio si no existe ─────────────────────────────────────────
mkdir -p "$SHP_DIR"

# ─── 2. Descargar shapefiles si no están ──────────────────────────────────────
if [ ! -f "$SHP_DIR/districtes.shp" ]; then
  echo "Descargando shapefile de distritos..."
  curl -L "https://opendata-ajuntament.barcelona.cat/data/dataset/20170706-districtes-barris/resource/b1de8e87-c3e1-4e10-b4d2-f6c0a0a0b8a0/download/BCN_Districtes_ED50_SHP.zip" \
    -o "$SHP_DIR/districtes.zip" 2>/dev/null || {
    echo "⚠️  No se pudo descargar automáticamente."
    echo "   Descarga manualmente desde:"
    echo "   https://opendata-ajuntament.barcelona.cat/data/ca/dataset/20170706-districtes-barris"
    echo "   y guarda los .shp en $SHP_DIR/"
  }
  if [ -f "$SHP_DIR/districtes.zip" ]; then
    unzip -o "$SHP_DIR/districtes.zip" -d "$SHP_DIR/" > /dev/null
  fi
fi

if [ ! -f "$SHP_DIR/barris.shp" ]; then
  echo "Descargando shapefile de barrios..."
  curl -L "https://opendata-ajuntament.barcelona.cat/data/dataset/20170706-districtes-barris/resource/db3b6a35-d5e5-4aa5-b7c8-94ecaf6d0636/download/BCN_Barris_ED50_SHP.zip" \
    -o "$SHP_DIR/barris.zip" 2>/dev/null || true
  if [ -f "$SHP_DIR/barris.zip" ]; then
    unzip -o "$SHP_DIR/barris.zip" -d "$SHP_DIR/" > /dev/null
  fi
fi

# ─── 3. Importar distritos ─────────────────────────────────────────────────────
echo "Importando distritos..."
SHP_DIST=$(find "$SHP_DIR" -name "*.shp" | grep -i "distri" | head -1)
if [ -n "$SHP_DIST" ]; then
  ogr2ogr \
    -f "PostgreSQL" \
    PG:"$DB_URL" \
    "$SHP_DIST" \
    -nln "distritos_tmp" \
    -overwrite \
    -t_srs EPSG:4326 \
    -lco GEOMETRY_NAME=geometria \
    -lco FID=ogc_fid
  
  # Mover datos a la tabla definitiva
  psql "$DB_URL" << 'SQL'
    INSERT INTO distritos (codigo, nombre, geometria)
    SELECT
      LPAD(CAST("DISTRICTE" AS TEXT), 2, '0') AS codigo,
      "NOM"                                    AS nombre,
      ST_Multi(geometria)::GEOMETRY(MULTIPOLYGON,4326)
    FROM distritos_tmp
    ON CONFLICT (codigo) DO UPDATE
    SET nombre    = EXCLUDED.nombre,
        geometria = EXCLUDED.geometria;
    DROP TABLE IF EXISTS distritos_tmp;
SQL
  echo "✅ Distritos importados"
else
  echo "⚠️  No se encontró shapefile de distritos en $SHP_DIR"
fi

# ─── 4. Importar barrios ───────────────────────────────────────────────────────
echo "Importando barrios..."
SHP_BARRIS=$(find "$SHP_DIR" -name "*.shp" | grep -i "barri" | head -1)
if [ -n "$SHP_BARRIS" ]; then
  ogr2ogr \
    -f "PostgreSQL" \
    PG:"$DB_URL" \
    "$SHP_BARRIS" \
    -nln "barris_tmp" \
    -overwrite \
    -t_srs EPSG:4326 \
    -lco GEOMETRY_NAME=geometria \
    -lco FID=ogc_fid

  psql "$DB_URL" << 'SQL'
    INSERT INTO barrios (codigo, nombre, distrito_id, geometria)
    SELECT
      LPAD(CAST("BARRI" AS TEXT), 6, '0') AS codigo,
      "NOM"                                AS nombre,
      d.id                                 AS distrito_id,
      ST_Multi(bt.geometria)::GEOMETRY(MULTIPOLYGON,4326)
    FROM barris_tmp bt
    JOIN distritos d ON d.codigo = LPAD(CAST("DISTRICTE" AS TEXT), 2, '0')
    ON CONFLICT (codigo) DO UPDATE
    SET nombre      = EXCLUDED.nombre,
        distrito_id = EXCLUDED.distrito_id,
        geometria   = EXCLUDED.geometria;
    DROP TABLE IF EXISTS barris_tmp;
SQL
  echo "✅ Barrios importados"
else
  echo "⚠️  No se encontró shapefile de barrios en $SHP_DIR"
fi

# ─── 5. Crear zonas dividiendo los barrios ─────────────────────────────────────
# Cada barrio se divide en subzonas de ~300x300m usando una rejilla (grid).
# Esto crea las zonas que GeoRetail analiza y muestra en el mapa.
echo "Creando zonas desde barrios..."
psql "$DB_URL" << 'SQL'
  -- Dividir cada barrio en celdas de aproximadamente 300m × 300m
  -- ST_SquareGrid genera una rejilla cuadrada sobre el polígono del barrio
  INSERT INTO zonas (id, nombre, barrio_id, geometria, area_m2)
  SELECT
    'bcn_' || LOWER(REPLACE(b.nombre, ' ', '_')) || '_' || LPAD(CAST(ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY ST_X(ST_Centroid(g.geom))) AS TEXT), 2, '0') AS id,
    b.nombre || ' — Zona ' || ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY ST_X(ST_Centroid(g.geom))) AS nombre,
    b.id     AS barrio_id,
    g.geom   AS geometria,
    ST_Area(g.geom::geography) AS area_m2
  FROM barrios b
  CROSS JOIN LATERAL ST_SquareGrid(0.003, b.geometria) AS g(geom)
  WHERE ST_Intersects(g.geom, b.geometria)
    AND ST_Area(ST_Intersection(g.geom, b.geometria)::geography) > 10000  -- mínimo 1 hectárea
  ON CONFLICT (id) DO NOTHING;

  -- Índice espacial (acelera las búsquedas por coordenadas)
  REINDEX INDEX idx_zonas_geometria;
SQL
echo "✅ Zonas creadas"

# ─── 6. Resumen ───────────────────────────────────────────────────────────────
echo ""
echo "=== Resumen ==="
psql "$DB_URL" -c "SELECT COUNT(*) AS distritos FROM distritos;"
psql "$DB_URL" -c "SELECT COUNT(*) AS barrios FROM barrios;"
psql "$DB_URL" -c "SELECT COUNT(*) AS zonas FROM zonas;"
echo ""
echo "✅ Importación completada"
echo ""
echo "Siguiente paso: ejecutar los pipelines de datos iniciales:"
echo "  cd backend && python -m pipelines.transporte"
echo "  cd backend && python -m pipelines.demografia"
echo "  cd backend && python -m pipelines.precios"
