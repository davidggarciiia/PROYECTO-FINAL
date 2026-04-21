# BUG-P0-5: `httpx.AsyncClient(proxies=...)` roto en httpx ≥ 0.28

**Fecha:** 2026-04-22
**Severidad:** CRITICA
**Estado:** Resuelto
**Archivo:** `georetail/backend/pipelines/inmobiliario/scraping/base_scraper.py:177`

---

## Descripción

`requirements.txt` fija `httpx[http2]==0.28.1` pero `base_scraper.py` llama a
`httpx.AsyncClient(proxies=proxy, ...)`. Desde httpx 0.28.0 (Oct 2024) el
parámetro `proxies=` fue eliminado — ahora es `proxy=` (str, un solo proxy) o
`mounts=` (dict por scheme). Con esta versión, cualquier fallback a httpx (rama
3 del dispatcher) lanza `TypeError: unexpected keyword argument 'proxies'` y
corta el scrape.

## Impacto

- El método `_get_httpx` es el **fallback final**: si ScrapingBee/ZenRows no
  están configurados y curl_cffi devuelve <500 chars (Cloudflare 403, por
  ejemplo), el scraper intentaba httpx → crasheaba → el scheduler marcaba la
  ejecución como errored.
- Afecta a todo el pipeline inmobiliario cuando se usa con proxies rotatorios.

## Reproducción

```python
import httpx
async with httpx.AsyncClient(proxies="http://user:pass@host:port") as c:
    ...
# TypeError: AsyncClient.__init__() got an unexpected keyword argument 'proxies'
```

## Causa raíz

El código precede a httpx 0.28; el bump de versión en `requirements.txt`
dejó la llamada incompatible. No hay test que ejercite el path httpx
fallback → no saltó ningún error en CI.

## Fix aplicado

En `base_scraper.py:174-179`:

```diff
  async with httpx.AsyncClient(
      timeout=self.cfg.timeout,
      follow_redirects=True,
-     proxies=proxy,
+     proxy=proxy if proxy else None,
      http2=True,
  ) as c:
```

Comentario añadido:

> httpx 0.28 eliminó `proxies=`; ahora es `proxy=` (str) o `mounts=` (dict).
> random_proxy() devuelve str|None, directamente compatible.

## Verificación

Búsqueda global de más matches:

```bash
grep -rn "proxies=" georetail/backend/ --include="*.py"
```

Las 4 coincidencias restantes (`fotocasa_scraper.py:362`,
`habitaclia_scraper.py:254`, `milanuncios_scraper.py:198`, y
`base_scraper.py:149`) pertenecen a `curl_cffi.AsyncSession`, que **sí** acepta
`proxies={"https": proxy, "http": proxy}`. No requieren cambio.

## Notas

- A futuro: añadir un smoke test `tests/test_scraper_httpx_fallback.py` que
  monte un mock HTTP con `respx` y verifique que el fallback httpx arranca sin
  crash (incluso sin proxy).
- Revisar si conviene volver a httpx <0.28 o migrar definitivamente a la API
  nueva (hecho aquí).

---

**Tags:** #bug #critico #scraping #httpx #p0
