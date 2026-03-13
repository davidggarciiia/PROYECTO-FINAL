"use client";

import { useState, useCallback } from "react";
import dynamic from "next/dynamic";
import SearchBox from "@/components/SearchBox";
import ZoneList from "@/components/ZoneList";
import DetailPanel from "@/components/DetailPanel";
import styles from "./page.module.css";
import type { ZonaPreview, LocalDetalleResponse } from "@/lib/types";
import { api } from "@/lib/api";

// Importar Leaflet solo en el cliente (no tiene soporte SSR)
const MapView = dynamic(() => import("@/components/MapView"), { ssr: false, loading: () => <div className={styles.mapPlaceholder}><div className="spinner" /></div> });

export default function HomePage() {
  const [sessionId, setSessionId] = useState<string>("");
  const [zonas, setZonas] = useState<ZonaPreview[]>([]);
  const [selectedZona, setSelectedZona] = useState<ZonaPreview | null>(null);
  const [detalle, setDetalle] = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [view, setView] = useState<"map" | "list">("map");

  const handleResults = useCallback((newZonas: ZonaPreview[], sid: string) => {
    setZonas(newZonas);
    setSessionId(sid);
    setSelectedZona(null);
    setDetalle(null);
  }, []);

  const handleZonaClick = useCallback(async (zona: ZonaPreview) => {
    setSelectedZona(zona);
    setDetalle(null);
    setLoadingDetalle(true);
    try {
      const data = await api.localDetalle(zona.zona_id, sessionId);
      setDetalle(data);
    } catch (e) {
      console.error("Error cargando detalle:", e);
    } finally {
      setLoadingDetalle(false);
    }
  }, [sessionId]);

  const handleClosePanel = useCallback(() => {
    setSelectedZona(null);
    setDetalle(null);
  }, []);

  return (
    <div className={styles.app}>
      {/* ── Sidebar izquierdo ── */}
      <aside className={styles.sidebar}>
        <div className={styles.logo}>
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
            <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z" fill="#6c63ff"/>
            <circle cx="12" cy="9" r="2.5" fill="#fff"/>
          </svg>
          <span>GeoRetail</span>
        </div>

        <SearchBox onResults={handleResults} sessionId={sessionId} />

        {zonas.length > 0 && (
          <>
            <div className={styles.viewToggle}>
              <button className={`${styles.toggleBtn} ${view === "map" ? styles.active : ""}`} onClick={() => setView("map")}>Mapa</button>
              <button className={`${styles.toggleBtn} ${view === "list" ? styles.active : ""}`} onClick={() => setView("list")}>Lista</button>
            </div>
            <div className={styles.resultsCount}>{zonas.length} zonas encontradas</div>
            {view === "list" && (
              <ZoneList zonas={zonas} selectedId={selectedZona?.zona_id} onSelect={handleZonaClick} />
            )}
          </>
        )}

        {zonas.length === 0 && (
          <div className={styles.emptyState}>
            <p>Describe tu negocio para ver las mejores ubicaciones en Barcelona</p>
            <div className={styles.examples}>
              <span>Prueba:</span>
              <button className={styles.exampleChip} onClick={() => {}}>Restaurante mediterráneo</button>
              <button className={styles.exampleChip} onClick={() => {}}>Tienda de ropa vintage</button>
              <button className={styles.exampleChip} onClick={() => {}}>Centro de estética</button>
            </div>
          </div>
        )}
      </aside>

      {/* ── Mapa ── */}
      <main className={`${styles.mapContainer} ${view === "list" ? styles.hidden : ""}`}>
        <MapView
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onZonaClick={handleZonaClick}
        />
      </main>

      {/* ── Panel de detalle ── */}
      {selectedZona && (
        <DetailPanel
          zona={selectedZona}
          detalle={detalle}
          loading={loadingDetalle}
          sessionId={sessionId}
          onClose={handleClosePanel}
        />
      )}
    </div>
  );
}
