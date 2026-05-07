"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { ZonaDetalle, DimensionTurismoDetalle } from "@/lib/types";
import { api } from "@/lib/api";
import { formatDist } from "../helpers";
import styles from "./VizTurismo.module.css";

interface Props {
  zona: ZonaDetalle | null;
  sessionId: string;
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c] as string,
  );
}

function fallbackDetalle(zona: ZonaDetalle): DimensionTurismoDetalle {
  return {
    zona_id: zona.zona_id,
    zona_nombre: zona.nombre,
    zona_lat: zona.lat,
    zona_lng: zona.lng,
    radio_m: 500,
    score_turismo: zona.score_turismo ?? null,
    resumen: "Se muestra la cobertura turística disponible para esta zona.",
    hechos: {
      landmarks_count: 0,
      hoteles_count: zona.booking_hoteles_500m ?? 0,
      venues_count: 0,
      airbnb_density_500m: zona.airbnb_density_500m ?? null,
      booking_hoteles_500m: zona.booking_hoteles_500m ?? null,
      dist_playa_m: zona.dist_playa_m ?? null,
      dist_landmark_top3_m: zona.dist_landmark_top3_m ?? null,
      score_turismo_airbnb: (zona as ZonaDetalle & { score_turismo_airbnb?: number | null }).score_turismo_airbnb ?? null,
      score_turismo_hut: null,
    },
    landmarks: [],
    hoteles: [],
    venues: [],
  };
}

function fmtCount(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Sin dato";
  return Math.round(value).toLocaleString("es-ES");
}

export default function VizTurismo({ zona, sessionId }: Props) {
  const [detalle, setDetalle] = useState<DimensionTurismoDetalle | null>(null);
  const [loading, setLoading] = useState(false);
  const [mapFailed, setMapFailed] = useState(false);
  const [filter, setFilter] = useState<"landmark" | "hotel" | "venue" | null>(null);

  const mapRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapInstanceRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const layerGroups = useRef<{ landmark: any[]; hotel: any[]; venue: any[] }>({
    landmark: [],
    hotel: [],
    venue: [],
  });

  useEffect(() => {
    if (!zona?.zona_id) return;
    let cancelled = false;
    setLoading(true);
    setDetalle(null);
    setMapFailed(false);
    const request = api.dimensionTurismo?.(zona.zona_id, sessionId);
    if (!request) {
      setLoading(false);
      return () => { cancelled = true; };
    }
    request
      .then((d: DimensionTurismoDetalle) => {
        if (!cancelled) setDetalle(d);
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        console.error("Turismo fetch:", e);
        setDetalle(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [zona?.zona_id, sessionId]);

  const mapDetalle = useMemo(() => {
    if (!zona) return null;
    return detalle ?? fallbackDetalle(zona);
  }, [detalle, zona]);

  useEffect(() => {
    if (!mapDetalle || !mapRef.current || mapInstanceRef.current) return;
    const center: [number, number] = [
      mapDetalle.zona_lat ?? 41.3851,
      mapDetalle.zona_lng ?? 2.1734,
    ];

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let map: any = null;
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const L = require("leaflet");
      setMapFailed(false);

      map = L.map(mapRef.current, {
        center,
        zoom: 15,
        zoomControl: true,
        attributionControl: false,
      });
      mapInstanceRef.current = map;

      L.tileLayer(
        "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
        { maxZoom: 19 },
      ).addTo(map);

      L.circle(center, {
        radius: mapDetalle.radio_m ?? 500,
        fillColor: "rgba(245,158,11,0.05)",
        color: "rgba(245,158,11,0.55)",
        weight: 1,
        fillOpacity: 0.05,
        dashArray: "4,4",
      }).addTo(map);

      L.circleMarker(center, {
        radius: 9,
        fillColor: "#F59E0B",
        color: "#fff",
        weight: 2,
        fillOpacity: 0.95,
      })
        .addTo(map)
        .bindTooltip(mapDetalle.zona_nombre, { direction: "top", offset: [0, -6] });

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const groups: { landmark: any[]; hotel: any[]; venue: any[] } = {
        landmark: [],
        hotel: [],
        venue: [],
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const allBounds: any[] = [center];

      for (const l of mapDetalle.landmarks) {
        if (l.lat == null || l.lng == null) continue;
        const ring = L.circle([l.lat, l.lng], {
          radius: 64,
          fillColor: "#F59E0B",
          color: "transparent",
          fillOpacity: 0.16,
        }).addTo(map);
        const marker = L.circleMarker([l.lat, l.lng], {
          radius: 7.2,
          fillColor: "#F59E0B",
          color: "#0F0A1A",
          weight: 1.6,
          fillOpacity: 1,
        })
          .addTo(map)
          .bindTooltip(
            `<div style="font-family:inherit"><strong>${escapeHtml(l.nombre)}</strong><br><span style="color:#F59E0B;font-weight:700;font-size:11px">PUNTO TURÍSTICO</span> · ${l.distancia_m} m</div>`,
            { direction: "top", offset: [0, -8] },
          );
        groups.landmark.push(ring, marker);
        allBounds.push([l.lat, l.lng]);
      }

      for (const h of mapDetalle.hoteles) {
        if (h.lat == null || h.lng == null) continue;
        const isHotel = h.tipo === "hotel" || h.tipo === "hostel";
        const color = isHotel ? "#3B82F6" : "#EF4444";
        const marker = L.circleMarker([h.lat, h.lng], {
          radius: isHotel ? 5.4 : 3.8,
          fillColor: color,
          color: "#0F0A1A",
          weight: 1.4,
          fillOpacity: 0.92,
        })
          .addTo(map)
          .bindTooltip(
            `<div style="font-family:inherit"><strong>${escapeHtml(h.nombre)}</strong><br><span style="color:${color};font-weight:700;font-size:11px">${(h.tipo || "ALOJ").toUpperCase()}</span>${h.estrellas ? ` · ${h.estrellas}★` : ""} · ${h.distancia_m} m</div>`,
            { direction: "top", offset: [0, -6] },
          );
        groups.hotel.push(marker);
        allBounds.push([h.lat, h.lng]);
      }

      for (const v of mapDetalle.venues) {
        if (v.lat == null || v.lng == null) continue;
        const marker = L.circleMarker([v.lat, v.lng], {
          radius: 4.8,
          fillColor: "#10B981",
          color: "#0F0A1A",
          weight: 1.4,
          fillOpacity: 0.85,
        })
          .addTo(map)
          .bindTooltip(
            `<div style="font-family:inherit"><strong>${escapeHtml(v.nombre)}</strong><br><span style="color:#10B981;font-weight:700;font-size:11px">${(v.tipo || "VENUE").toUpperCase()}</span> · ${v.distancia_m} m</div>`,
            { direction: "top", offset: [0, -6] },
          );
        groups.venue.push(marker);
        allBounds.push([v.lat, v.lng]);
      }

      layerGroups.current = groups;

      if (allBounds.length > 1) {
        map.fitBounds(L.latLngBounds(allBounds), {
          padding: [24, 24],
          maxZoom: 16,
        });
      }
      window.setTimeout(() => map?.invalidateSize(), 80);
    } catch (e) {
      console.error("Error pintando mapa turismo:", e);
      setMapFailed(true);
      if (map) map.remove();
      mapInstanceRef.current = null;
    }

    return () => {
      try { mapInstanceRef.current?.remove(); } catch { /* noop */ }
      mapInstanceRef.current = null;
      layerGroups.current = { landmark: [], hotel: [], venue: [] };
    };
  }, [mapDetalle]);

  useEffect(() => {
    const groups = layerGroups.current;
    (["landmark", "hotel", "venue"] as const).forEach((kind) => {
      const dim = filter && filter !== kind;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      groups[kind].forEach((layer: any) => {
        if (typeof layer.setStyle === "function") {
          layer.setStyle({ opacity: dim ? 0.15 : 1, fillOpacity: dim ? 0.08 : (kind === "landmark" ? 0.16 : 0.92) });
        }
      });
    });
  }, [filter]);

  if (!zona || !mapDetalle) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. TURISMO EN EL ENTORNO</span>
        <p>Sin datos turísticos para esta zona.</p>
      </div>
    );
  }

  const cLand = mapDetalle.landmarks.length;
  const cHot = mapDetalle.hoteles.length;
  const cVen = mapDetalle.venues.length;
  const score = mapDetalle.score_turismo ?? zona.score_turismo;
  const airbnb = mapDetalle.hechos.airbnb_density_500m ?? zona.airbnb_density_500m;
  const airbnbScore = mapDetalle.hechos.score_turismo_airbnb ??
    (zona as ZonaDetalle & { score_turismo_airbnb?: number | null }).score_turismo_airbnb ??
    null;
  const hoteles = mapDetalle.hechos.booking_hoteles_500m ?? zona.booking_hoteles_500m;
  const occupancy = zona.airbnb_occupancy_est;
  const distLandmark = mapDetalle.hechos.dist_landmark_top3_m ?? zona.dist_landmark_top3_m;
  const distPlaya = mapDetalle.hechos.dist_playa_m ?? zona.dist_playa_m;

  return (
    <div className={styles.figWrap}>
      <div className={styles.mapBlock}>
        <div className={styles.figHead}>
          <div className={styles.figTitle}>
            <strong>Fig.</strong> Puntos turísticos en el entorno
          </div>
          <div className={styles.figSub}>
            {mapDetalle.radio_m} m · {cLand} puntos · {cHot} alojamientos · {cVen} ocio
          </div>
        </div>

        <div className={styles.mapWrap}>
          {loading && <div className={styles.mapNotice}>Actualizando puntos...</div>}
          {mapFailed && <div className={styles.mapFallback}>No se ha podido cargar el mapa.</div>}
          <div ref={mapRef} className={styles.leafletMap} />
        </div>

        <div className={styles.legend}>
          <button
            type="button"
            className={`${styles.chip} ${filter === "landmark" ? styles.chipOn : ""}`}
            onClick={() => setFilter(filter === "landmark" ? null : "landmark")}
            style={filter === "landmark" ? { background: "#F59E0B", color: "#0F0A1A", borderColor: "#F59E0B" } : undefined}
          >
            <i className={styles.chipDot} style={{ background: "#F59E0B" }} />
            Puntos turísticos · {cLand}
          </button>
          <button
            type="button"
            className={`${styles.chip} ${filter === "hotel" ? styles.chipOn : ""}`}
            onClick={() => setFilter(filter === "hotel" ? null : "hotel")}
            style={filter === "hotel" ? { background: "#3B82F6", color: "#fff", borderColor: "#3B82F6" } : undefined}
          >
            <i className={styles.chipDot} style={{ background: "#3B82F6" }} />
            Alojamientos · {cHot}
          </button>
          <button
            type="button"
            className={`${styles.chip} ${filter === "venue" ? styles.chipOn : ""}`}
            onClick={() => setFilter(filter === "venue" ? null : "venue")}
            style={filter === "venue" ? { background: "#10B981", color: "#fff", borderColor: "#10B981" } : undefined}
          >
            <i className={styles.chipDot} style={{ background: "#10B981" }} />
            Ocio cultural · {cVen}
          </button>
        </div>
      </div>

      <div className={styles.kpiPanel}>
        <div className={styles.summary}>
          <span>lectura turística</span>
          <p>{mapDetalle.resumen}</p>
        </div>

        <div className={styles.kpiRow}>
          {score != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Score turismo</span>
              <span className={styles.kpiValue}>{Math.round(score)}</span>
              <span className={styles.kpiHint}>/ 100</span>
            </div>
          )}
          {airbnb != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Airbnb 500 m</span>
              <span className={styles.kpiValue}>{fmtCount(airbnb)}</span>
              <span className={styles.kpiHint}>listados</span>
            </div>
          )}
          {airbnb == null && airbnbScore != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Airbnb</span>
              <span className={styles.kpiValue}>{Math.round(airbnbScore)}</span>
              <span className={styles.kpiHint}>score / 100</span>
            </div>
          )}
          {hoteles != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Hoteles 500 m</span>
              <span className={styles.kpiValue}>{fmtCount(hoteles)}</span>
              <span className={styles.kpiHint}>booking</span>
            </div>
          )}
          {occupancy != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Ocupación</span>
              <span className={styles.kpiValue}>{Math.round(occupancy * 100)}%</span>
              <span className={styles.kpiHint}>airbnb</span>
            </div>
          )}
          {distLandmark != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Landmark medio</span>
              <span className={styles.kpiValue}>{formatDist(distLandmark)}</span>
              <span className={styles.kpiHint}>top-3</span>
            </div>
          )}
          {distPlaya != null && (
            <div className={styles.kpi}>
              <span className={styles.kpiLabel}>Playa</span>
              <span className={styles.kpiValue}>{formatDist(distPlaya)}</span>
              <span className={styles.kpiHint}>distancia</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
