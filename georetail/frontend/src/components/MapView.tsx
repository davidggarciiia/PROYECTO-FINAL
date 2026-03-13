"use client";

import { useEffect, useRef } from "react";
import { MapContainer, TileLayer, CircleMarker, Tooltip, useMap } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import type { ZonaPreview } from "@/lib/types";
import styles from "./MapView.module.css";

// Barcelona center
const BCN_CENTER: [number, number] = [41.3851, 2.1734];

function scoreColor(score?: number): string {
  if (!score) return "#eab308";
  if (score >= 75) return "#22c55e";
  if (score >= 50) return "#eab308";
  return "#ef4444";
}

// Componente para hacer zoom automático a las zonas
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
        zoomControl={true}
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
              radius={isSelected ? 16 : 12}
              pathOptions={{
                fillColor: color,
                fillOpacity: isSelected ? 1 : 0.8,
                color: isSelected ? "#fff" : "rgba(0,0,0,0.3)",
                weight: isSelected ? 2.5 : 1,
              }}
              eventHandlers={{ click: () => onZonaClick(zona) }}
            >
              <Tooltip direction="top" offset={[0, -8]} opacity={1}>
                <div className={styles.tooltip}>
                  <strong>{zona.nombre}</strong>
                  <span>{zona.barrio}</span>
                  {score !== null && <span className={styles.tooltipScore} style={{ color }}>Score {score}</span>}
                  {zona.alquiler_mensual && <span>{zona.alquiler_mensual.toLocaleString("es-ES")} €/mes</span>}
                </div>
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>

      {zonas.length === 0 && (
        <div className={styles.overlay}>
          <div className={styles.overlayText}>
            Busca tu negocio para ver las mejores ubicaciones
          </div>
        </div>
      )}
    </div>
  );
}
