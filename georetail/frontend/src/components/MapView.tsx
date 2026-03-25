"use client";

import { useEffect, useRef, useMemo, useState, useCallback } from "react";
import { MapContainer, TileLayer, Marker, Tooltip, useMap, ZoomControl } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { ZonaPreview } from "@/lib/types";
import styles from "./MapView.module.css";

const BCN_CENTER: [number, number] = [41.3851, 2.1734];

// ── Map styles ────────────────────────────────────────────────────────────────
type MapStyleId = 
  | "light" 
  | "colorful" 
  | "vibrant" 
  | "pastel" 
  | "satellite" 
  | "dark"
  | "humanitarian";

interface MapStyle {
  id: MapStyleId;
  label: string;
  emoji: string;
  url: string;
  attribution: string;
  subdomains?: string;
  dark: boolean;
  maxZoom?: number;
}

const MAP_STYLES: MapStyle[] = [
  // Mapas claros y coloridos (principales)
  {
    id: "colorful",
    label: "Colorido",
    emoji: "🌈",
    url: "https://tile.openstreetmap.de/{z}/{x}/{y}.png",
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    dark: false,
    maxZoom: 19,
  },
  {
    id: "vibrant",
    label: "Vibrante",
    emoji: "🎨",
    url: "https://tiles.stadiamaps.com/tiles/osm_bright/{z}/{x}/{y}{r}.png",
    attribution: '© <a href="https://www.stadiamaps.com/">Stadia Maps</a>, © <a href="https://openmaptiles.org/">OpenMapTiles</a>, © <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    subdomains: "abc",
    dark: false,
    maxZoom: 20,
  },
  {
    id: "pastel",
    label: "Pastel",
    emoji: "🌸",
    url: "https://tiles.stadiamaps.com/tiles/stamen_toner_lite/{z}/{x}/{y}{r}.png",
    attribution: '© <a href="https://www.stadiamaps.com/">Stadia Maps</a>, © <a href="https://www.stamen.com/">Stamen Design</a>, © <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    subdomains: "abc",
    dark: false,
    maxZoom: 20,
  },
  {
    id: "light",
    label: "Claro",
    emoji: "☀️",
    url: "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OSM</a> © <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    dark: false,
  },
  
  // Mapas temáticos adicionales
  {
    id: "humanitarian",
    label: "Humanitario",
    emoji: "🌍",
    url: "https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors, Humanitarian OSM Team',
    subdomains: "abc",
    dark: false,
    maxZoom: 19,
  },
  {
    id: "satellite",
    label: "Satélite",
    emoji: "🛰️",
    url: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    attribution: "Tiles © Esri — Source: Esri, USGS, NOAA",
    dark: true,
    maxZoom: 18,
  },
  {
    id: "dark",
    label: "Oscuro",
    emoji: "🌑",
    url: "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OSM</a> © <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    dark: true,
  },
];
// ─────────────────────────────────────────────────────────────────────────────

function scoreColor(score?: number): string {
  if (!score) return "#F59E0B";
  if (score >= 75) return "#10B981";
  if (score >= 50) return "#F59E0B";
  return "#EF4444";
}

function scoreColorRgb(score?: number): string {
  if (!score) return "245,158,11";
  if (score >= 75) return "16,185,129";
  if (score >= 50) return "245,158,11";
  return "239,68,68";
}

function scoreLabel(score?: number): string {
  if (!score) return "—";
  if (score >= 75) return "Alta viabilidad";
  if (score >= 50) return "Viabilidad media";
  return "Baja viabilidad";
}

function colorClass(score?: number): string {
  if (!score) return "amarillo";
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

function createMarkerIcon(score: number, selected: boolean, isDark: boolean) {
  const L = require("leaflet");
  const color = scoreColor(score);
  const rgb = scoreColorRgb(score);
  const s = Math.round(score);
  const textColor = isDark ? color : "#1e293b";

  if (selected) {
    return L.divIcon({
      className: "",
      html: `
        <div style="position:relative;width:48px;height:48px;display:flex;align-items:center;justify-content:center;">
          <div style="position:absolute;width:48px;height:48px;border-radius:50%;border:2px solid ${color};opacity:0.5;animation:markerRing 1.6s ease-out infinite;"></div>
          <div style="width:34px;height:34px;border-radius:50%;background:rgba(${rgb},0.28);border:2.5px solid ${color};display:flex;align-items:center;justify-content:center;font-weight:800;font-size:11px;color:${color};font-family:Inter,-apple-system,sans-serif;box-shadow:0 0 18px rgba(${rgb},0.55);position:relative;z-index:1;">${s}</div>
        </div>`,
      iconSize: [48, 48],
      iconAnchor: [24, 24],
      tooltipAnchor: [0, -28],
    });
  }

  return L.divIcon({
    className: "",
    html: `
      <div style="width:30px;height:30px;border-radius:50%;background:rgba(${rgb},${isDark ? "0.15" : "0.22"});border:2px solid ${color};display:flex;align-items:center;justify-content:center;font-weight:800;font-size:10px;color:${textColor};font-family:Inter,-apple-system,sans-serif;box-shadow:0 2px 10px rgba(0,0,0,0.4),0 0 8px rgba(${rgb},0.3);cursor:pointer;transition:transform 0.15s;">${s}</div>`,
    iconSize: [30, 30],
    iconAnchor: [15, 15],
    tooltipAnchor: [0, -18],
  });
}

function AutoBounds({ zonas }: { zonas: ZonaPreview[] }) {
  const map = useMap();
  const prevCount = useRef(0);
  useEffect(() => {
    if (zonas.length === 0 || zonas.length === prevCount.current) return;
    prevCount.current = zonas.length;
    const L = require("leaflet");
    const bounds = L.latLngBounds(zonas.map(z => [z.lat, z.lng]));
    if (bounds.isValid()) map.fitBounds(bounds, { padding: [60, 60] });
  }, [zonas, map]);
  return null;
}

function PanToSelected({ zona }: { zona: ZonaPreview | null }) {
  const map = useMap();
  const prevId = useRef<string | null>(null);
  useEffect(() => {
    if (!zona || zona.zona_id === prevId.current) return;
    prevId.current = zona.zona_id;
    map.panTo([zona.lat, zona.lng], { animate: true, duration: 0.5 });
  }, [zona, map]);
  return null;
}

// ── Expanded card that appears above the marker ───────────────────────────────
interface ExpandedCardProps {
  zona: ZonaPreview;
  onOpen: (zona: ZonaPreview) => void;
  onClose: () => void;
}

function ExpandedCard({ zona, onOpen, onClose }: ExpandedCardProps) {
  const score = zona.score_global ?? 0;
  const color = scoreColor(score);
  const cls = colorClass(score);

  return (
    <div className={styles.expandedCard}>
      {/* ...existing close button... */}
      <button className={styles.expandedClose} onClick={e => { e.stopPropagation(); onClose(); }}>
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
          <path d="M2 2l6 6M8 2L2 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
        </svg>
      </button>

      {/* ...existing score + name... */}
      <div className={styles.expandedHeader}>
        <div className={styles.expandedScorePill} style={{ color, borderColor: `${color}44`, background: `${color}18` }}>
          {Math.round(score)}
        </div>
        <div className={styles.expandedNames}>
          <div className={styles.expandedName}>{zona.nombre}</div>
          <div className={styles.expandedSub}>{zona.barrio} · {zona.distrito}</div>
        </div>
      </div>

      {/* ...existing badge... */}
      <span className={`badge badge-${cls}`} style={{ alignSelf: "flex-start", marginTop: 2 }}>
        {scoreLabel(score)}
      </span>

      {/* ...existing KPIs... */}
      {(zona.alquiler_mensual || zona.m2) && (
        <div className={styles.expandedKpis}>
          {zona.alquiler_mensual && (
            <div className={styles.expandedKpi}>
              <span className={styles.expandedKpiVal}>{zona.alquiler_mensual.toLocaleString("es-ES")} €</span>
              <span className={styles.expandedKpiLabel}>/ mes</span>
            </div>
          )}
          {zona.m2 && (
            <div className={styles.expandedKpi}>
              <span className={styles.expandedKpiVal}>{zona.m2}</span>
              <span className={styles.expandedKpiLabel}>m²</span>
            </div>
          )}
        </div>
      )}

      {/* ...existing CTA... */}
      <button
        className={styles.expandedCta}
        onClick={e => { e.stopPropagation(); onOpen(zona); }}
      >
        Ver análisis completo
        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
          <path d="M2.5 6h7M6.5 3l3 3-3 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </button>
    </div>
  );
}
// ─────────────────────────────────────────────────────────────────────────────

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onZonaClick: (zona: ZonaPreview) => void;
}

export default function MapView({ zonas, selectedId, onZonaClick }: Props) {
  const [styleId, setStyleId] = useState<MapStyleId>("vibrant");
  const [pickerOpen, setPickerOpen] = useState(false);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const currentStyle = MAP_STYLES.find(s => s.id === styleId)!;
  const selectedZona = useMemo(() => zonas.find(z => z.zona_id === selectedId) ?? null, [zonas, selectedId]);

  const handleMarkerClick = useCallback((zona: ZonaPreview) => {
    if (expandedId === zona.zona_id) {
      onZonaClick(zona);
      setExpandedId(null);
    } else {
      setExpandedId(zona.zona_id);
    }
  }, [expandedId, onZonaClick]);

  const handleOpenDetail = useCallback((zona: ZonaPreview) => {
    onZonaClick(zona);
    setExpandedId(null);
  }, [onZonaClick]);

  return (
    <div className={styles.wrapper} onClick={() => { setPickerOpen(false); }}>
      <MapContainer
        center={BCN_CENTER}
        zoom={13}
        style={{ width: "100%", height: "100%" }}
        zoomControl={false}
      >
        <ZoomControl position="bottomright" />
        <TileLayer
          key={styleId}
          url={currentStyle.url}
          attribution={currentStyle.attribution}
          subdomains={currentStyle.subdomains ?? "abc"}
          maxZoom={20}
        />

        <AutoBounds zonas={zonas} />
        <PanToSelected zona={selectedZona} />

        {zonas.map(zona => {
          const score = zona.score_global ?? 0;
          const isSelected = zona.zona_id === selectedId;
          const isExpanded = zona.zona_id === expandedId;
          const color = scoreColor(zona.score_global);

          return (
            <Marker
              key={`${zona.zona_id}-${isSelected}-${styleId}`}
              position={[zona.lat, zona.lng]}
              icon={createMarkerIcon(score, isSelected || isExpanded, currentStyle.dark)}
              zIndexOffset={isSelected || isExpanded ? 1000 : 0}
              eventHandlers={{ click: () => handleMarkerClick(zona) }}
            >
              {/* ...existing hover tooltip... */}
              {!isExpanded && (
                <Tooltip direction="top" offset={[0, isSelected ? -30 : -18]} opacity={1} permanent={false}>
                  <div className={styles.tooltip}>
                    <strong className={styles.tooltipName}>{zona.nombre}</strong>
                    <span className={styles.tooltipBarrio}>{zona.barrio} · {zona.distrito}</span>
                    <div className={styles.tooltipBottom}>
                      <span className={styles.tooltipScore} style={{ color }}>
                        <span className={styles.tooltipDot} style={{ background: color }} />
                        {Math.round(score)} · {scoreLabel(zona.score_global)}
                      </span>
                      {zona.alquiler_mensual && (
                        <span className={styles.tooltipRent}>{zona.alquiler_mensual.toLocaleString("es-ES")} €/mes</span>
                      )}
                    </div>
                  </div>
                </Tooltip>
              )}

              {/* ...existing expanded card... */}
              {isExpanded && (
                <Tooltip direction="top" offset={[0, -28]} opacity={1} permanent={true} className="leaflet-tooltip-expanded">
                  <ExpandedCard
                    zona={zona}
                    onOpen={handleOpenDetail}
                    onClose={() => setExpandedId(null)}
                  />
                </Tooltip>
              )}
            </Marker>
          );
        })}
      </MapContainer>

      {/* ── Map style picker ── */}
      <div className={styles.stylePickerWrap} onClick={e => e.stopPropagation()}>
        <button
          className={`${styles.styleBtn} ${pickerOpen ? styles.styleBtnActive : ""}`}
          onClick={() => setPickerOpen(v => !v)}
        >
          <span className={styles.styleBtnEmoji}>{currentStyle.emoji}</span>
          <span className={styles.styleBtnLabel}>{currentStyle.label}</span>
          <svg className={`${styles.chevron} ${pickerOpen ? styles.chevronOpen : ""}`} width="10" height="10" viewBox="0 0 10 10" fill="none">
            <path d="M2 4l3 3 3-3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>

        {pickerOpen && (
          <div className={styles.stylePicker}>
            <div className={styles.stylePickerTitle}>Estilo de mapa</div>
            <div className={styles.styleGrid}>
              {MAP_STYLES.map(style => (
                <button
                  key={style.id}
                  className={`${styles.styleOption} ${styleId === style.id ? styles.styleOptionActive : ""}`}
                  onClick={() => { setStyleId(style.id); setPickerOpen(false); }}
                >
                  <span className={styles.styleOptionEmoji}>{style.emoji}</span>
                  <span className={styles.styleOptionLabel}>{style.label}</span>
                  {styleId === style.id && (
                    <span className={styles.styleCheck}>
                      <svg width="8" height="8" viewBox="0 0 8 8" fill="none">
                        <path d="M1.5 4l2 2L6.5 2" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                      </svg>
                    </span>
                  )}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* ...existing legend... */}
      {zonas.length > 0 && (
        <div className={styles.legend}>
          <div className={styles.legendTitle}>Score viabilidad</div>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#10B981", boxShadow: "0 0 6px rgba(16,185,129,0.5)" }} />
            <span>Alta ≥ 75</span>
          </div>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#F59E0B", boxShadow: "0 0 6px rgba(245,158,11,0.5)" }} />
            <span>Media 50–74</span>
          </div>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#EF4444", boxShadow: "0 0 6px rgba(239,68,68,0.5)" }} />
            <span>Baja &lt; 50</span>
          </div>
        </div>
      )}

    </div>
  );
}