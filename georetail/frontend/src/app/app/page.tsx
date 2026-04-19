"use client";

import { useState, useCallback, useEffect, useMemo } from "react";
import Onboarding from "@/components/Onboarding";
import Statusbar from "@/components/map/Statusbar";
import MapCanvas, { type BasemapId } from "@/components/map/MapCanvas";
import HudCoord from "@/components/map/HudCoord";
import HudLegend from "@/components/map/HudLegend";
import BasemapSwitcher from "@/components/map/BasemapSwitcher";
import CommandBar from "@/components/map/CommandBar";
import ZoneIndex from "@/components/map/ZoneIndex";
import ActiveDock from "@/components/map/ActiveDock";
import Dossier from "@/components/map/Dossier";
import styles from "./page.module.css";
import type { ZonaPreview, LocalDetalleResponse } from "@/lib/types";
import { api } from "@/lib/api";

const BCN_CENTER = { lat: 41.3851, lng: 2.1734, zoom: 13 };

export default function AppPage() {
  const [started, setStarted]             = useState(false);
  const [searchQuery, setSearchQuery]     = useState("");
  const [sessionId, setSessionId]         = useState("");
  const [zonas, setZonas]                 = useState<ZonaPreview[]>([]);
  const [loading, setLoading]             = useState(false);
  const [errorMsg, setErrorMsg]           = useState<string | null>(null);

  const [activeId, setActiveId]           = useState<string | null>(null);
  const [detalle, setDetalle]             = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [dossierOpen, setDossierOpen]     = useState(false);

  const [basemap, setBasemap]             = useState<BasemapId>("dark");
  const [coords, setCoords]               = useState(BCN_CENTER);

  const activeZone = useMemo(
    () => zonas.find((z) => z.zona_id === activeId) ?? null,
    [zonas, activeId],
  );

  const fetchZonas = useCallback(
    async (query: string) => {
      if (!query.trim()) return;
      setLoading(true);
      setErrorMsg(null);
      try {
        const res = await api.buscar({ descripcion: query, session_id: sessionId || undefined });
        if (res.estado === "ok" && Array.isArray(res.zonas)) {
          // API returns ZonaResumen (alquiler_estimado/m2_disponibles); HUD expects ZonaPreview (alquiler_mensual/m2)
          const adapted: ZonaPreview[] = res.zonas.map((z) => ({
            zona_id: z.zona_id,
            nombre: z.nombre,
            barrio: z.barrio,
            distrito: z.distrito,
            lat: z.lat,
            lng: z.lng,
            score_global: z.score_global,
            m2: z.m2_disponibles,
            alquiler_mensual: z.alquiler_estimado,
            color: z.color,
          }));
          setZonas(adapted);
          if (res.session_id) setSessionId(res.session_id);
          if (adapted.length > 0) setActiveId(adapted[0].zona_id);
        } else if (res.estado === "cuestionario") {
          setErrorMsg("El motor necesita más contexto. Por ahora, añade detalles a tu prompt.");
        } else if (res.estado === "error_tipo_negocio") {
          setErrorMsg("No reconocemos el tipo de negocio. Intenta con una descripción distinta.");
        } else if (res.estado === "inviable_legal") {
          setErrorMsg("La combinación negocio + zona no es viable legalmente.");
        }
      } catch (e) {
        console.error("api.buscar error:", e);
        setErrorMsg("Error conectando con el motor. ¿Backend corriendo en :8000?");
      } finally {
        setLoading(false);
      }
    },
    [sessionId],
  );

  // Onboarding submit -> start + first search
  const handleOnboardingSubmit = useCallback(
    (q: string) => {
      setSearchQuery(q);
      setStarted(true);
      void fetchZonas(q);
    },
    [fetchZonas],
  );

  // Refine search: append text and re-query
  const handleRefine = useCallback(
    (text: string) => {
      const merged = [searchQuery, text].filter(Boolean).join(" · ").slice(0, 240);
      setSearchQuery(merged);
      void fetchZonas(merged);
    },
    [searchQuery, fetchZonas],
  );

  const handleRestart = useCallback(() => {
    setStarted(false);
    setSearchQuery("");
    setZonas([]);
    setActiveId(null);
    setDetalle(null);
    setDossierOpen(false);
    setErrorMsg(null);
  }, []);

  // Dock navigation (prev / next)
  const handleNav = useCallback(
    (dir: -1 | 1) => {
      if (zonas.length === 0) return;
      const idx = zonas.findIndex((z) => z.zona_id === activeId);
      const nextIdx = ((idx < 0 ? 0 : idx) + dir + zonas.length) % zonas.length;
      setActiveId(zonas[nextIdx].zona_id);
    },
    [zonas, activeId],
  );

  // Fetch detalle lazily when dossier opens
  useEffect(() => {
    if (!dossierOpen || !activeId) return;
    // Refetch if we don't have detalle for the current zone
    if (detalle?.zona.zona_id === activeId) return;
    let cancelled = false;
    setLoadingDetalle(true);
    setDetalle(null);
    api
      .localDetalle(activeId, sessionId)
      .then((d) => {
        if (!cancelled) setDetalle(d);
      })
      .catch((e) => {
        console.error("api.localDetalle error:", e);
      })
      .finally(() => {
        if (!cancelled) setLoadingDetalle(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dossierOpen, activeId, sessionId, detalle]);

  // Onboarding stage
  if (!started) {
    return <Onboarding onSubmit={handleOnboardingSubmit} />;
  }

  return (
    <div className={styles.app}>
      <Statusbar
        query={searchQuery}
        numZonas={zonas.length}
        onRestart={handleRestart}
      />

      <main className={styles.canvas}>
        <MapCanvas
          zonas={zonas}
          activeId={activeId}
          basemap={basemap}
          onPick={(id) => setActiveId(id)}
          onMove={(c) => setCoords(c)}
        />

        <div className={styles.scrim} aria-hidden="true" />

        <HudCoord lat={coords.lat} lng={coords.lng} zoom={coords.zoom} label="BARCELONA" />

        <CommandBar onRefine={handleRefine} loading={loading} />

        <ZoneIndex
          zonas={zonas}
          activeId={activeId}
          onPick={(id) => setActiveId(id)}
        />

        <BasemapSwitcher value={basemap} onChange={setBasemap} />

        <ActiveDock
          zone={activeZone}
          zones={zonas}
          dims={null}
          loading={loading}
          onExpand={() => setDossierOpen(true)}
          onNav={handleNav}
        />

        <HudLegend />

        {errorMsg && (
          <div className={styles.errorToast} role="alert">
            <span className={styles.errorTick}>●</span>
            {errorMsg}
          </div>
        )}
      </main>

      {dossierOpen && activeZone && (
        <Dossier
          zone={activeZone}
          detalle={detalle}
          loading={loadingDetalle}
          onClose={() => setDossierOpen(false)}
        />
      )}
    </div>
  );
}
