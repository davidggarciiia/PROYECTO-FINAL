"use client";

import { useState, useCallback, useEffect } from "react";
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

type MobileView = "search" | "map" | "detail";

function useIsMobile() {
  const [isMobile, setIsMobile] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(max-width: 1023px)");
    setIsMobile(mq.matches);
    const handler = (e: MediaQueryListEvent) => setIsMobile(e.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);
  return isMobile;
}

export default function HomePage() {
  const isMobile = useIsMobile();

  const [sessionId, setSessionId]           = useState("");
  const [zonas, setZonas]                   = useState<ZonaPreview[]>([]);
  const [selectedZona, setSelectedZona]     = useState<ZonaPreview | null>(null);
  const [detalle, setDetalle]               = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [searchQuery, setSearchQuery]       = useState("");
  const [mobileView, setMobileView]         = useState<MobileView>("search");

  const handleResults = useCallback((newZonas: ZonaPreview[], sid: string) => {
    setZonas(newZonas);
    setSessionId(sid);
    setSelectedZona(null);
    setDetalle(null);
    setMobileView("map");
  }, []);

  const handleZonaClick = useCallback(async (zona: ZonaPreview) => {
    setSelectedZona(zona);
    setDetalle(null);
    setLoadingDetalle(true);
    setMobileView("detail");
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
    setMobileView("map");
  }, []);

  const handleBackToSearch = useCallback(() => {
    setMobileView("search");
  }, []);

  const topBarTitle =
    mobileView === "detail"
      ? (selectedZona?.nombre ?? "Detalle")
      : zonas.length > 0
        ? `${zonas.length} ubicaciones`
        : "Barcelona";

  return (
    <div className={styles.app} data-view={mobileView}>

      {/* ── Mobile top bar (map + detail views only) ── */}
      {isMobile && (mobileView === "map" || mobileView === "detail") && (
        <div className={styles.mobileTopBar}>
          <button
            className={styles.mobileBackBtn}
            onClick={mobileView === "detail" ? handleClosePanel : handleBackToSearch}
            title="Volver"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M10 3L5 8l5 5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </button>
          <span className={styles.mobileTopBarTitle}>{topBarTitle}</span>
          <span className={styles.mobileTopBarBadge}>BCN</span>
        </div>
      )}

      {/* ── Sidebar (search + list) ── */}
      <aside className={styles.sidebar}>
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

        <SearchBox
          onResults={handleResults}
          sessionId={sessionId}
          externalQuery={searchQuery}
          onQueryUsed={() => setSearchQuery("")}
        />

        {/* Zone list — desktop only inside sidebar */}
        {zonas.length > 0 && !isMobile && (
          <>
            <div className={styles.resultsHeader}>
              <span className={styles.resultsCount}>{zonas.length} ubicaciones</span>
              <span className={styles.resultsHint}>Haz clic para ver detalle</span>
            </div>
            <ZoneList
              zonas={zonas}
              selectedId={selectedZona?.zona_id}
              onSelect={handleZonaClick}
            />
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
                <button key={ex} className={styles.chip} onClick={() => setSearchQuery(ex)}>
                  {ex}
                </button>
              ))}
            </div>
          </div>
        )}
      </aside>

      {/* ── Map ── */}
      <main className={styles.mapContainer}>
        <MapView
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onZonaClick={handleZonaClick}
        />

        {/* Desktop: detail panel overlays map from the right */}
        {!isMobile && selectedZona && (
          <DetailPanel
            zona={selectedZona}
            detalle={detalle}
            loading={loadingDetalle}
            sessionId={sessionId}
            onClose={handleClosePanel}
          />
        )}
      </main>

      {/* ── Mobile: bottom sheet zone list over map ── */}
      {isMobile && mobileView === "map" && zonas.length > 0 && (
        <ZoneList
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onSelect={handleZonaClick}
          asBottomSheet
        />
      )}

      {/* ── Mobile: fullscreen detail panel ── */}
      {isMobile && mobileView === "detail" && selectedZona && (
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