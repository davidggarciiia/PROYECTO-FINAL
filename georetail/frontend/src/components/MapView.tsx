"use client";

import { useEffect, useRef } from "react";
import { MapContainer, TileLayer, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import type { ZonaPreview } from "@/lib/types";
import styles from "./MapView.module.css";

const BCN_CENTER: [number, number] = [41.3851, 2.1734];

function scoreClass(score?: number): string {
  if (!score) return "amarillo";
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

function makeMarkerIcon(score: number | undefined, isSelected: boolean, colorClass: string): L.DivIcon {
  const s = score !== undefined ? Math.round(score) : "–";
  const size = isSelected ? 48 : 38;
  return L.divIcon({
    className: "",
    html: `<div class="kp-marker kp-marker--${colorClass}${isSelected ? " kp-marker--selected" : ""}"
                style="width:${size}px;height:${size}px;font-size:${isSelected ? 14 : 12}px">
             ${s}
           </div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function AutoBounds({ zonas }: { zonas: ZonaPreview[] }) {
  const map = useMap();
  const prevCount = useRef(0);
  useEffect(() => {
    if (zonas.length === 0 || zonas.length === prevCount.current) return;
    prevCount.current = zonas.length;
    const bounds = L.latLngBounds(zonas.map(z => [z.lat, z.lng]));
    if (bounds.isValid()) map.fitBounds(bounds, { padding: [70, 70] });
  }, [zonas, map]);
  return null;
}

function Markers({
  zonas,
  selectedId,
  onZonaClick,
}: {
  zonas: ZonaPreview[];
  selectedId?: string;
  onZonaClick: (z: ZonaPreview) => void;
}) {
  const map = useMap();
  const markersRef = useRef<Record<string, L.Marker>>({});

  useEffect(() => {
    // Remove old markers
    Object.values(markersRef.current).forEach(m => m.remove());
    markersRef.current = {};

    zonas.forEach(zona => {
      const isSelected = zona.zona_id === selectedId;
      const colorClass = scoreClass(zona.score_global);
      const icon = makeMarkerIcon(zona.score_global, isSelected, colorClass);

      const marker = L.marker([zona.lat, zona.lng], { icon, zIndexOffset: isSelected ? 1000 : 0 });

      // Tooltip
      const alquiler = zona.alquiler_mensual
        ? `${zona.alquiler_mensual.toLocaleString("es-ES")} €/mes`
        : "";
      marker.bindTooltip(
        `<div class="${styles.tooltip}">
           <strong>${zona.nombre}</strong>
           <span>${zona.barrio}</span>
           ${alquiler ? `<span>${alquiler}</span>` : ""}
         </div>`,
        { direction: "top", offset: [0, -8], opacity: 1 }
      );

      marker.on("click", () => onZonaClick(zona));
      marker.addTo(map);
      markersRef.current[zona.zona_id] = marker;
    });

    return () => {
      Object.values(markersRef.current).forEach(m => m.remove());
      markersRef.current = {};
    };
  }, [zonas, selectedId, onZonaClick, map]);

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
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
          subdomains="abcd"
          maxZoom={20}
        />
        <AutoBounds zonas={zonas} />
        <Markers zonas={zonas} selectedId={selectedId} onZonaClick={onZonaClick} />
      </MapContainer>

      {zonas.length === 0 && (
        <div className={styles.overlay}>
          <div className={styles.overlayCard}>
            <svg width="32" height="32" viewBox="0 0 32 32" fill="none" className={styles.overlayIcon}>
              <path d="M16 2C10.48 2 6 6.48 6 12c0 7 10 18 10 18s10-11 10-18c0-5.52-4.48-10-10-10z" fill="var(--blue)" opacity="0.6"/>
              <circle cx="16" cy="12" r="3" fill="var(--blue-light)"/>
            </svg>
            <p>Busca tu negocio para ver<br/>las mejores ubicaciones</p>
          </div>
        </div>
      )}
    </div>
  );
}
