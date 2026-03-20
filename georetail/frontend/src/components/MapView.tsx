"use client";

import { useEffect, useRef, useMemo } from "react";
import { MapContainer, TileLayer, Marker, Tooltip, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { ZonaPreview } from "@/lib/types";
import styles from "./MapView.module.css";

const BCN_CENTER: [number, number] = [41.3851, 2.1734];

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

function createMarkerIcon(score: number, selected: boolean) {
  // Imported dynamically to avoid SSR — component is loaded with ssr:false
  const L = require("leaflet");
  const color = scoreColor(score);
  const rgb = scoreColorRgb(score);
  const s = Math.round(score);

  if (selected) {
    // Pulsing ring + filled circle
    return L.divIcon({
      className: "",
      html: `
        <div style="position:relative;width:48px;height:48px;display:flex;align-items:center;justify-content:center;">
          <div style="
            position:absolute;width:48px;height:48px;border-radius:50%;
            border:2px solid ${color};opacity:0.5;
            animation:markerRing 1.6s ease-out infinite;
          "></div>
          <div style="
            width:34px;height:34px;border-radius:50%;
            background:rgba(${rgb},0.25);
            border:2.5px solid ${color};
            display:flex;align-items:center;justify-content:center;
            font-weight:800;font-size:11px;color:${color};
            font-family:Inter,-apple-system,sans-serif;
            box-shadow:0 0 18px rgba(${rgb},0.55);
            position:relative;z-index:1;
          ">${s}</div>
        </div>`,
      iconSize: [48, 48],
      iconAnchor: [24, 24],
      tooltipAnchor: [0, -28],
    });
  }

  return L.divIcon({
    className: "",
    html: `
      <div style="
        width:30px;height:30px;border-radius:50%;
        background:rgba(${rgb},0.15);
        border:2px solid ${color};
        display:flex;align-items:center;justify-content:center;
        font-weight:800;font-size:10px;color:${color};
        font-family:Inter,-apple-system,sans-serif;
        box-shadow:0 2px 10px rgba(0,0,0,0.45),0 0 8px rgba(${rgb},0.3);
        cursor:pointer;transition:transform 0.15s;
      ">${s}</div>`,
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

// Pan to selected zone
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

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onZonaClick: (zona: ZonaPreview) => void;
}

export default function MapView({ zonas, selectedId, onZonaClick }: Props) {
  const selectedZona = useMemo(
    () => zonas.find(z => z.zona_id === selectedId) ?? null,
    [zonas, selectedId]
  );

  return (
    <div className={styles.wrapper}>
      <MapContainer
        center={BCN_CENTER}
        zoom={13}
        style={{ width: "100%", height: "100%" }}
        zoomControl={true}
      >
        {/* CartoDB Dark Matter — professional dark map, no token needed */}
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='© <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors © <a href="https://carto.com/attributions">CARTO</a>'
          subdomains="abcd"
          maxZoom={20}
        />

        <AutoBounds zonas={zonas} />
        <PanToSelected zona={selectedZona} />

        {zonas.map(zona => {
          const score = zona.score_global ?? 0;
          const isSelected = zona.zona_id === selectedId;
          const color = scoreColor(zona.score_global);

          return (
            <Marker
              key={`${zona.zona_id}-${isSelected}`}
              position={[zona.lat, zona.lng]}
              icon={createMarkerIcon(score, isSelected)}
              zIndexOffset={isSelected ? 1000 : 0}
              eventHandlers={{ click: () => onZonaClick(zona) }}
            >
              <Tooltip
                direction="top"
                offset={[0, isSelected ? -30 : -18]}
                opacity={1}
                permanent={false}
              >
                <div className={styles.tooltip}>
                  <strong className={styles.tooltipName}>{zona.nombre}</strong>
                  <span className={styles.tooltipBarrio}>{zona.barrio} · {zona.distrito}</span>
                  <div className={styles.tooltipBottom}>
                    <span className={styles.tooltipScore} style={{ color }}>
                      <span className={styles.tooltipDot} style={{ background: color }} />
                      {Math.round(score)} · {scoreLabel(zona.score_global)}
                    </span>
                    {zona.alquiler_mensual && (
                      <span className={styles.tooltipRent}>
                        {zona.alquiler_mensual.toLocaleString("es-ES")} €/mes
                      </span>
                    )}
                  </div>
                </div>
              </Tooltip>
            </Marker>
          );
        })}
      </MapContainer>

      {/* Legend */}
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

      {/* Empty overlay */}
      {zonas.length === 0 && (
        <div className={styles.overlay}>
          <div className={styles.overlayCard}>
            <div className={styles.overlayIcon}>
              <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
                <path d="M16 3C10.48 3 6 7.48 6 13c0 8.4 10 17 10 17s10-8.6 10-17c0-5.52-4.48-10-10-10z" stroke="#6366F1" strokeWidth="1.5" fill="rgba(99,102,241,0.1)"/>
                <circle cx="16" cy="13" r="3.5" stroke="#6366F1" strokeWidth="1.5"/>
              </svg>
            </div>
            <p className={styles.overlayTitle}>Barcelona te espera</p>
            <p className={styles.overlayDesc}>
              Describe tu negocio para ver las mejores ubicaciones
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
