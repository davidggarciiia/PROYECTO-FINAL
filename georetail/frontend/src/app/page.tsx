"use client";

import { useState, useCallback } from "react";
import dynamic from "next/dynamic";
import SearchBox from "@/components/SearchBox";
import ZoneList from "@/components/ZoneList";
import DetailPanel from "@/components/DetailPanel";
import styles from "./page.module.css";
import type { ZonaPreview, LocalDetalleResponse } from "@/lib/types";
import { api } from "@/lib/api";

const MapView = dynamic(
  () => import("@/components/MapView"),
  { ssr: false, loading: () => <div className={styles.mapPlaceholder}><div className="spinner spinner-lg" /></div> }
);

const EXAMPLES = [
  { icon: "🍕", text: "Restaurante de tapas con terraza" },
  { icon: "👗", text: "Tienda de ropa vintage" },
  { icon: "💆", text: "Centro de estética y bienestar" },
];

export default function HomePage() {
  const [sessionId, setSessionId] = useState<string>("");
  const [zonas, setZonas] = useState<ZonaPreview[]>([]);
  const [selectedZona, setSelectedZona] = useState<ZonaPreview | null>(null);
  const [detalle, setDetalle] = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [view, setView] = useState<"map" | "list">("map");
  const [exampleText, setExampleText] = useState<string>("");

  const handleResults = useCallback((newZonas: ZonaPreview[], sid: string) => {
    setZonas(newZonas);
    setSessionId(sid);
    setSelectedZona(null);
    setDetalle(null);
    setExampleText("");
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
      {/* ── Sidebar ── */}
      <aside className={styles.sidebar}>
        <div className={styles.logo}>
          <div className={styles.logoMark}>
            <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
              <path d="M9 1C5.69 1 3 3.69 3 7c0 4.37 6 10 6 10s6-5.63 6-10c0-3.31-2.69-6-6-6z" fill="white"/>
              <circle cx="9" cy="7" r="2" fill="#4F96F7"/>
            </svg>
          </div>
          <div>
            <div className={styles.logoText}>Knowpoint</div>
            <div className={styles.logoTag}>Barcelona · AI</div>
          </div>
        </div>

        <SearchBox
          onResults={handleResults}
          sessionId={sessionId}
          initialText={exampleText}
          onExampleClear={() => setExampleText("")}
        />

        {zonas.length > 0 && (
          <>
            <div className={styles.viewToggle}>
              <button
                className={`${styles.toggleBtn} ${view === "map" ? styles.active : ""}`}
                onClick={() => setView("map")}
              >
                Mapa
              </button>
              <button
                className={`${styles.toggleBtn} ${view === "list" ? styles.active : ""}`}
                onClick={() => setView("list")}
              >
                Lista
              </button>
            </div>
            <div className={styles.resultsCount}>
              <strong>{zonas.length}</strong> zonas encontradas
            </div>
            {view === "list" && (
              <ZoneList zonas={zonas} selectedId={selectedZona?.zona_id} onSelect={handleZonaClick} />
            )}
          </>
        )}

        {zonas.length === 0 && (
          <div className={styles.emptyState}>
            <div className={styles.emptyHero}>
              <div className={styles.emptyIcon}>
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                  <path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z" fill="var(--blue)" opacity="0.8"/>
                  <circle cx="12" cy="9" r="2.5" fill="white"/>
                </svg>
              </div>
              <div className={styles.emptyTitle}>Encuentra tu ubicación ideal</div>
              <div className={styles.emptyDesc}>
                Describe tu negocio y Knowpoint analiza miles de zonas en Barcelona con IA
              </div>
            </div>
            <div className={styles.examplesLabel}>Prueba con</div>
            <div className={styles.examples}>
              {EXAMPLES.map((ex) => (
                <button
                  key={ex.text}
                  className={styles.exampleChip}
                  onClick={() => setExampleText(ex.text)}
                >
                  <span className={styles.chipIcon}>{ex.icon}</span>
                  {ex.text}
                </button>
              ))}
            </div>
          </div>
        )}
      </aside>

      {/* ── Map ── */}
      <main className={`${styles.mapContainer} ${view === "list" ? styles.hidden : ""}`}>
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
