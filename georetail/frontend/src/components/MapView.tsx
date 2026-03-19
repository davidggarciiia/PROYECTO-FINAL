"use client";

import { useEffect, useRef } from "react";
import { MapContainer, TileLayer, CircleMarker, Tooltip, useMap } from "react-leaflet";
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

function scoreLabel(score?: number): string {
  if (!score) return "—";
  if (score >= 75) return "Alta";
  if (score >= 50) return "Media";
  return "Baja";
}

function AutoBounds({ zonas }: { zonas: ZonaPreview[] }) {
  const map = useMap();
  const prevCount = useRef(0);
  useEffect(() => {
    if (zonas.length === 0 || zonas.length === prevCount.current) return;
    prevCount.current = zonas.length;
    const L = require("leaflet");
    const bounds = L.latLngBounds(zonas.map(z => [z.lat, z.lng]));
    if (bounds.isValid()) map.fitBounds(bounds, { padding: [80, 80] });
  }, [zonas, map]);
  return null;
}

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onZonaClick: (zona: ZonaPreview) => void;
}

export default function MapView({ zonas, selectedId, onZonaClick }: Props) {
  return (
    <div className={styles.wrapper}>
      <MapContainer
        center={BCN_CENTER}
        zoom={13}
        style={{ width: "100%", height: "100%" }}
        zoomControl={false}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        />
        <AutoBounds zonas={zonas} />
        {zonas.map(zona => {
          const color = scoreColor(zona.score_global);
          const isSelected = zona.zona_id === selectedId;
          const score = zona.score_global ? Math.round(zona.score_global) : null;

          return (
            <CircleMarker
              key={zona.zona_id}
              center={[zona.lat, zona.lng]}
              radius={isSelected ? 18 : 13}
              pathOptions={{
                fillColor: color,
                fillOpacity: isSelected ? 1 : 0.85,
                color: isSelected ? "#fff" : "rgba(0,0,0,0.5)",
                weight: isSelected ? 3 : 1.5,
              }}
              eventHandlers={{ click: () => onZonaClick(zona) }}
            >
              <Tooltip direction="top" offset={[0, -10]} opacity={1} permanent={false}>
                <div className={styles.tooltip}>
                  <strong className={styles.tooltipName}>{zona.nombre}</strong>
                  <span className={styles.tooltipBarrio}>{zona.barrio}</span>
                  <div className={styles.tooltipBottom}>
                    {score !== null && (
                      <span className={styles.tooltipScore} style={{ color }}>
                        <span className={styles.tooltipScoreDot} style={{ background: color }} />
                        {score} · {scoreLabel(zona.score_global)}
                      </span>
                    )}
                    {zona.alquiler_mensual && (
                      <span className={styles.tooltipRent}>
                        {zona.alquiler_mensual.toLocaleString("es-ES")} €/mes
                      </span>
                    )}
                  </div>
                </div>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>

      {/* Legend */}
      {zonas.length > 0 && (
        <div className={styles.legend}>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#10B981" }} />
            <span>Score alto ≥75</span>
          </div>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#F59E0B" }} />
            <span>Score medio 50–74</span>
          </div>
          <div className={styles.legendItem}>
            <span className={styles.legendDot} style={{ background: "#EF4444" }} />
            <span>Score bajo &lt;50</span>
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
              Describe tu negocio en el panel izquierdo para ver las mejores ubicaciones
            </p>
          </div>
        </div>
      )}

      {/* Zoom controls (custom position) */}
      <div className={styles.zoomControls} id="custom-zoom" />
    </div>
  );
}
