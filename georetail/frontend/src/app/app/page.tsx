"use client";

import { useState, useCallback, useEffect } from "react";
import dynamic from "next/dynamic";
import SearchBox from "@/components/SearchBox";
import ZoneList from "@/components/ZoneList";
import DetailPanel from "@/components/DetailPanel";
import Onboarding from "@/components/Onboarding";
import styles from "./page.module.css";
import type { ZonaPreview, LocalDetalleResponse, Theme } from "@/lib/types";
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

function useTheme(): [Theme, (t: Theme) => void] {
  const [theme, setThemeState] = useState<Theme>("dark");

  useEffect(() => {
    const stored = localStorage.getItem("georetail-theme") as Theme | null;
    if (stored === "light" || stored === "dark") {
      setThemeState(stored);
      document.documentElement.setAttribute("data-theme", stored);
    }
  }, []);

  const setTheme = useCallback((t: Theme) => {
    setThemeState(t);
    document.documentElement.setAttribute("data-theme", t);
    localStorage.setItem("georetail-theme", t);
  }, []);

  return [theme, setTheme];
}

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

export default function AppPage() {
  const isMobile = useIsMobile();
  const [theme, setTheme] = useTheme();

  const [started, setStarted]               = useState(false);
  const [sessionId, setSessionId]           = useState("");
  const [zonas, setZonas]                   = useState<ZonaPreview[]>([]);
  const [selectedZona, setSelectedZona]     = useState<ZonaPreview | null>(null);
  const [detalle, setDetalle]               = useState<LocalDetalleResponse | null>(null);
  const [loadingDetalle, setLoadingDetalle] = useState(false);
  const [searchQuery, setSearchQuery]       = useState("");
  const [showDetail, setShowDetail]         = useState(false);
  const [hasSearched, setHasSearched]       = useState(false);
  const [searchBoxOpen, setSearchBoxOpen]   = useState(false);

  const handleOnboardingSubmit = useCallback((query: string) => {
    setSearchQuery(query);
    setStarted(true);
  }, []);

  const handleResults = useCallback((newZonas: ZonaPreview[], sid: string) => {
    setZonas(newZonas);
    setSessionId(sid);
    setSelectedZona(null);
    setDetalle(null);
    setShowDetail(false);
    setHasSearched(true);
  }, []);

  const handleZonaClick = useCallback(async (zona: ZonaPreview) => {
    setSelectedZona(zona);
    setDetalle(null);
    setLoadingDetalle(true);
    setShowDetail(true);
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
    setShowDetail(false);
  }, []);

  // Pre-stage: Signal onboarding. Al enviar, pasa la query al SearchBox.
  if (!started) {
    return <Onboarding onSubmit={handleOnboardingSubmit} />;
  }

  return (
    <div className={styles.app}>

      {/* ── Full-screen map ── */}
      <main className={styles.mapContainer}>
        <MapView
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onZonaClick={handleZonaClick}
          theme={theme}
          onThemeChange={setTheme}
        />

        {/* ── Floating overlay: search + zone list ── */}
        <div className={styles.floatingOverlay}>

          {/* Search ball */}
          <SearchBox
            onResults={handleResults}
            sessionId={sessionId}
            externalQuery={searchQuery}
            onQueryUsed={() => setSearchQuery("")}
            examples={EXAMPLES}
            hasResults={zonas.length > 0}
            onOpenChange={setSearchBoxOpen}
          />

          {/* Search hint — shown when no search done yet and box is closed */}
          {!hasSearched && !searchBoxOpen && (
            <div className={styles.searchHint}>
              <div className={styles.searchHintBubble}>
                <svg width="13" height="13" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                  <circle cx="6" cy="6" r="4.5"/>
                  <path d="M9.5 9.5L12.5 12.5"/>
                </svg>
                Indica tu negocio en el buscador
              </div>
            </div>
          )}

          {/* Zone list — desktop floating panel */}
          {zonas.length > 0 && !isMobile && (
            <ZoneList
              zonas={zonas}
              selectedId={selectedZona?.zona_id}
              onSelect={handleZonaClick}
              asFloatingPanel
            />
          )}
        </div>

        {/* Desktop: detail panel overlays map from the right */}
        {!isMobile && showDetail && selectedZona && (
          <DetailPanel
            zona={selectedZona}
            detalle={detalle}
            loading={loadingDetalle}
            sessionId={sessionId}
            onClose={handleClosePanel}
          />
        )}
      </main>

      {/* ── Mobile: bottom sheet zone list ── */}
      {isMobile && zonas.length > 0 && !showDetail && (
        <ZoneList
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onSelect={handleZonaClick}
          asBottomSheet
        />
      )}

      {/* ── Mobile: fullscreen detail panel ── */}
      {isMobile && showDetail && selectedZona && (
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
