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
  const [showDetail, setShowDetail]         = useState(false);

  const handleResults = useCallback((newZonas: ZonaPreview[], sid: string) => {
    setZonas(newZonas);
    setSessionId(sid);
    setSelectedZona(null);
    setDetalle(null);
    setShowDetail(false);
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

  return (
    <div className={styles.app}>

      {/* ── Full-screen map ── */}
      <main className={styles.mapContainer}>
        <MapView
          zonas={zonas}
          selectedId={selectedZona?.zona_id}
          onZonaClick={handleZonaClick}
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
          />

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
