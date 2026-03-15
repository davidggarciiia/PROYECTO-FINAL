# docker/postgres/Dockerfile
# PostgreSQL 15 con PostGIS 3.4 + pgvector
#
# postgis/postgis:15-3.4  → PostgreSQL 15 + PostGIS (ST_SquareGrid, geometrías)
# postgresql-15-pgvector  → extensión vectorial para embeddings de reseñas

FROM postgis/postgis:15-3.4

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-15-pgvector \
    && rm -rf /var/lib/apt/lists/*
