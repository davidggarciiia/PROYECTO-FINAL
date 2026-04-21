# BUG-COMP: Tab de competencia atascada — loadingComp no se resetea al cambiar zona

**Fecha:** 2026-04-21  
**Severidad:** MEDIA  
**Estado:** ✅ Resuelto (2026-04-21)  
**Archivo:** `frontend/src/components/map/Dossier.tsx`  

---

## Descripción

Tres problemas acumulados que causaban que la pestaña de competencia "no se viera":

### 1. `loadingComp` no se reseteaba al cambiar de zona

```tsx
// ANTES (incorrecto):
useEffect(() => {
    setCompetencia(null);
    setCompetenciaError(null);
    // ← faltaba setLoadingComp(false)
}, [zone.zona_id]);
```

Si el usuario cambiaba de zona mientras la petición de competencia estaba en vuelo, `loadingComp` quedaba a `true`. El guard `if (loadingComp) return` en `loadCompetencia` impedía cargar la nueva zona. El panel quedaba mostrando skeletons indefinidamente o datos de la zona anterior.

### 2. No había guard para `sessionId` vacío

`loadCompetencia` se podía llamar con `sessionId = ""` (estado inicial antes de hacer búsqueda), lo que causaba un 404 del backend que no era un error del usuario.

### 3. Race condition: datos de zona A sobreescriben zona B

Si la respuesta de zona A llegaba después de que el usuario hubiera cambiado a zona B, se mostraban datos incorrectos de zona A en el panel de zona B.

---

## Fix aplicado

```tsx
// 1. Reset completo al cambiar zona (incluye loadingComp)
const activeZoneRef = useRef(zone.zona_id);

useEffect(() => {
    activeZoneRef.current = zone.zona_id;
    setCompetencia(null);
    setCompetenciaError(null);
    setLoadingComp(false);  // ← añadido
}, [zone.zona_id]);

// 2. Guard de sessionId + snapshot de zona para cancelar respuestas stale
const loadCompetencia = useCallback(async () => {
    if (competencia || loadingComp || !sessionId) return;  // ← añadido !sessionId
    const zonaIdSnapshot = zone.zona_id;
    setLoadingComp(true);
    try {
        const data = await api.competencia(zonaIdSnapshot, sessionId);
        if (activeZoneRef.current !== zonaIdSnapshot) return;  // ← stale check
        setCompetencia(data);
    } catch (error) {
        if (activeZoneRef.current === zonaIdSnapshot) {
            setCompetenciaError("No se ha podido cargar el análisis de competencia.");
        }
    } finally {
        if (activeZoneRef.current === zonaIdSnapshot) {
            setLoadingComp(false);
        }
    }
}, [...]);
```

---

**Tags:** #bug #frontend #competencia #react #race-condition #dossier
