"use client";

import { useState, useCallback } from "react";
import dynamic from "next/dynamic";
import SearchBox from "@/components/SearchBox";
import ZoneList from "@/components/ZoneList";
import DetailPanel from "@/components/DetailPanel";
import styles from "./page.module.css";
import type { ZonaPreview, LocalDetalleResponse } from "@/lib/types";
import { api } from "@/lib/api";

const MapView = dynamic(() => import("@/components/MapView"), {
  ssr: false,
  loading: () => <div className={styles.mapPlaceholder}><div className="spinner" /></div>,
});

const EXAMPLES = [
  "Restaurante de tapas con terraza",
  "Estudio de tatuajes en zona alternativa",
  "Tienda de ropa vintage",
  "Centro de estética y bienestar",
];

export default function HomePage() {
  const [sessionId, setSessionId] = useState("");
  const [zonas, setZonas] = useState<ZonaPreview[]>([]);
  const [selectedZona, setSelectedZona] = useState<ZonaPreview | null>(null);
  const [detalle, setDetalle] = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [view, setView] = useState<"map" | "list">("map");
  const [searchQuery, setSearchQuery] = useState("");

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

  const handleExampleClick = (example: string) => {
    setSearchQuery(example);
  };

  return (
    <div className={styles.app}>
      {/* ── Sidebar ── */}
      <aside className={styles.sidebar}>
        {/* Logo */}
        <div className={styles.logoBar}>
          <div className={styles.logoMark}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M8 1C5.24 1 3 3.24 3 6c0 3.5 5 9 5 9s5-5.5 5-9c0-2.76-2.24-5-5-5z" fill="white" opacity="0.9"/>
              <circle cx="8" cy="6" r="2" fill="white" opacity="0.5"/>
            </svg>
          </div>
          <span className={styles.logoText}>GeoRetail</span>
          <span className={styles.logoBadge}>BCN</span>
        </div>

        {/* Search */}
        <SearchBox
          onResults={handleResults}
          sessionId={sessionId}
          externalQuery={searchQuery}
          onQueryUsed={() => setSearchQuery("")}
        />

        {/* Results */}
        {zonas.length > 0 && (
          <>
            <div className={styles.viewToggle}>
              <button
                className={`${styles.viewBtn} ${view === "map" ? styles.viewBtnActive : ""}`}
                onClick={() => setView("map")}
              >
                <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                  <path d="M1 2.5L4.5 1l3 1.5L11 1v9L7.5 11l-3-1.5L1 11V2.5z" stroke="currentColor" strokeWidth="1" fill="none"/>
                </svg>
                Mapa
              </button>
              <button
                className={`${styles.viewBtn} ${view === "list" ? styles.viewBtnActive : ""}`}
                onClick={() => setView("list")}
              >
                <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                  <rect x="1" y="2" width="10" height="1.5" rx="0.75"/>
                  <rect x="1" y="5.25" width="10" height="1.5" rx="0.75"/>
                  <rect x="1" y="8.5" width="10" height="1.5" rx="0.75"/>
                </svg>
                Lista
              </button>
            </div>
            <div className={styles.resultsHeader}>
              <span className={styles.resultsCount}>{zonas.length} ubicaciones</span>
            </div>
            {view === "list" && (
              <ZoneList
                zonas={zonas}
                selectedId={selectedZona?.zona_id}
                onSelect={handleZonaClick}
              />
            )}
          </>
        )}

        {zonas.length === 0 && (
          <div className={styles.emptyState}>
            <div className={styles.emptyIllustration}>
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z" stroke="currentColor" strokeWidth="1.5" fill="none"/>
                <circle cx="12" cy="9" r="2.5" stroke="currentColor" strokeWidth="1.5"/>
              </svg>
            </div>
            <p className={styles.emptyTitle}>Encuentra tu ubicación ideal</p>
            <p className={styles.emptyDesc}>
              Describe tu negocio y recibirás un análisis de viabilidad para cada zona de Barcelona
            </p>
            <div className={styles.exampleChips}>
              {EXAMPLES.map(ex => (
                <button key={ex} className={styles.chip} onClick={() => handleExampleClick(ex)}>
                  {ex}
                </button>
              ))}
            </div>
          </div>
        )}
      </aside>

      {/* ── Map ── */}
      <main className={`${styles.mapContainer} ${view === "list" ? styles.mapHidden : ""}`}>
        <MapView
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onZonaClick={handleZonaClick}
        />
      </main>

      {/* ── Detail panel ── */}
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
