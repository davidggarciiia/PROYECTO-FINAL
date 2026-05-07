"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { LineaCercana, TransporteDetalleZona, TransporteTipo } from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./VizTransporte.module.css";

interface Props {
  zonaId: string;
}

const TIPO_ORDER: TransporteTipo[] = ["metro", "tram", "fgc", "rodalies", "bus"];

const TIPO_LABEL: Record<TransporteTipo, string> = {
  metro: "Metro",
  tram: "Tram",
  fgc: "FGC",
  rodalies: "Rodalies",
  bus: "Bus",
};

const TIPO_COLOR_FALLBACK: Record<TransporteTipo, string> = {
  metro: "#D03324",
  bus: "#E3000F",
  tram: "#007F3B",
  fgc: "#9B2743",
  rodalies: "#9B2743",
};

const MAX_STOPS_PER_LINE = 9;
const MAX_MARKERS = 90;

function colorForLinea(l: LineaCercana): string {
  if (l.color && l.color.trim()) return l.color;
  return TIPO_COLOR_FALLBACK[l.tipo as TransporteTipo] ?? "#888";
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c] as string,
  );
}

function sampledStops(stops: LineaCercana["paradas_cercanas"]): LineaCercana["paradas_cercanas"] {
  if (stops.length <= MAX_STOPS_PER_LINE) return stops;
  const last = stops.length - 1;
  return Array.from({ length: MAX_STOPS_PER_LINE }, (_, i) => {
    const idx = Math.round((i / (MAX_STOPS_PER_LINE - 1)) * last);
    return stops[idx];
  });
}

/**
 * VizTransporte — mapa Leaflet real con OSM (Voyager tiles), líneas trazadas
 * conectando paradas en orden, popups por línea/parada y filtros chip.
 *
 * Reemplaza la versión SVG anterior. Datos reales del endpoint extendido
 * /api/transporte/{zona_id} que incluye lat/lng/orden + centroide de zona.
 */
export default function VizTransporte({ zonaId }: Props) {
  const [data, setData] = useState<TransporteDetalleZona | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mapFailed, setMapFailed] = useState(false);
  const [activeLinea, setActiveLinea] = useState<string | null>(null);

  const mapRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapInstanceRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const layersByLinea = useRef<Map<string, any[]>>(new Map());

  // Fetch
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .transporte?.(zonaId)
      .then((d: TransporteDetalleZona) => {
        if (cancelled) return;
        setData(d);
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        console.error("Transporte fetch:", e);
        setError("No se ha podido cargar el detalle de transporte.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [zonaId]);

  // Mapa Leaflet
  useEffect(() => {
    if (!data || !mapRef.current || mapInstanceRef.current) return;
    const center: [number, number] = [
      data.zona_lat ?? 41.3851,
      data.zona_lng ?? 2.1734,
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

      // Anillo radio
      L.circle(center, {
        radius: data.radio_m ?? 500,
        fillColor: "rgba(124,58,237,0.04)",
        color: "rgba(168,85,247,0.5)",
        weight: 1,
        fillOpacity: 0.04,
        dashArray: "4,4",
      }).addTo(map);

      // Self marker
      L.circleMarker(center, {
        radius: 9,
        fillColor: "#A855F7",
        color: "#fff",
        weight: 2,
        fillOpacity: 0.95,
      })
        .addTo(map)
        .bindTooltip("Tu zona", { direction: "top", offset: [0, -6] });

      // Agrupar paradas por (lat,lng,nombre) para que paradas con varias
      // líneas se rendericen como un único marker con una "lista de líneas"
      // en el tooltip. Así NO inventamos polilíneas zigzag entre paradas
      // que no representan la geometría real de la línea — cada parada es
      // un punto real con sus líneas reales.
      type ParadaAgrupada = {
        lat: number;
        lng: number;
        nombre: string;
        distancia_m: number;
        lineas: { codigo: string; nombre: string; color: string; tipo: string }[];
      };
      const paradaKey = (lat: number, lng: number, nombre: string) =>
        `${lat.toFixed(5)},${lng.toFixed(5)}|${nombre}`;
      const paradasMap = new Map<string, ParadaAgrupada>();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const lineasLayers = new Map<string, any[]>();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const allBounds: any[] = [center];

      for (const linea of data.lineas) {
        const color = colorForLinea(linea);
        const rawStops = [...linea.paradas_cercanas]
          .filter((p) => p.lat != null && p.lng != null)
          .sort((a, b) => {
            const ordenA = a.orden ?? Number.MAX_SAFE_INTEGER;
            const ordenB = b.orden ?? Number.MAX_SAFE_INTEGER;
            if (ordenA !== ordenB) return ordenA - ordenB;
            return a.distancia_m - b.distancia_m;
          });
        const stops = sampledStops(rawStops);
        const coords = stops.map((p) => [p.lat as number, p.lng as number] as [number, number]);
        if (!lineasLayers.has(linea.codigo)) lineasLayers.set(linea.codigo, []);

        if (coords.length >= 2) {
          const halo = L.polyline(coords, {
            color: "#020617",
            weight: 8,
            opacity: 0.62,
            lineCap: "round",
            lineJoin: "round",
          }).addTo(map);
          const route = L.polyline(coords, {
            color,
            weight: linea.tipo === "bus" ? 3.2 : 4.6,
            opacity: 0.92,
            lineCap: "round",
            lineJoin: "round",
          })
            .addTo(map)
            .bindTooltip(
              `<strong>${escapeHtml(linea.codigo)}</strong> · ${escapeHtml(linea.nombre ?? linea.codigo)}<br>${rawStops.length} paradas cercanas`,
              { sticky: true },
            );
          lineasLayers.get(linea.codigo)!.push(halo, route);
          coords.forEach((point) => allBounds.push(point));
        } else if (coords.length === 1) {
          const connector = L.polyline([center, coords[0]], {
            color,
            weight: 2.2,
            opacity: 0.55,
            dashArray: "5,6",
            lineCap: "round",
          })
            .addTo(map)
            .bindTooltip(
              `<strong>${escapeHtml(linea.codigo)}</strong> · parada cercana`,
              { sticky: true },
            );
          lineasLayers.get(linea.codigo)!.push(connector);
          allBounds.push(coords[0]);
        }
      }

      for (const linea of data.lineas) {
        const color = colorForLinea(linea);
        for (const p of linea.paradas_cercanas) {
          if (p.lat == null || p.lng == null) continue;
          const k = paradaKey(p.lat, p.lng, p.nombre);
          const existing = paradasMap.get(k);
          if (existing) {
            existing.lineas.push({ codigo: linea.codigo, nombre: linea.nombre ?? linea.codigo, color, tipo: linea.tipo });
            existing.distancia_m = Math.min(existing.distancia_m, p.distancia_m);
          } else {
            paradasMap.set(k, {
              lat: p.lat,
              lng: p.lng,
              nombre: p.nombre,
              distancia_m: p.distancia_m,
              lineas: [{ codigo: linea.codigo, nombre: linea.nombre ?? linea.codigo, color, tipo: linea.tipo }],
            });
          }
        }
      }

      // Marker por parada agrupada
      const paradasArr: ParadaAgrupada[] = [];
      paradasMap.forEach((p) => paradasArr.push(p));
      const visibleParadas = paradasArr
        .sort((a, b) => a.distancia_m - b.distancia_m)
        .slice(0, MAX_MARKERS);
      for (const parada of visibleParadas) {
        // Color del marker = color de la línea principal (la primera)
        const primary = parada.lineas[0];
        const isMetroLike = parada.lineas.some((l: { tipo: string }) =>
          l.tipo === "metro" || l.tipo === "rodalies" || l.tipo === "fgc",
        );
        const radius = isMetroLike ? 6.5 : 4.5;

        // Halo de fondo según número de líneas
        if (parada.lineas.length > 1) {
          L.circleMarker([parada.lat, parada.lng], {
            radius: radius + 4,
            fillColor: primary.color,
            color: "transparent",
            fillOpacity: 0.12,
          }).addTo(map);
        }

        const lineasHtml = parada.lineas
          .map((l: { codigo: string; color: string }) => `<span style="display:inline-block;padding:2px 6px;margin:2px 3px 2px 0;background:${l.color};color:#fff;font-size:10px;font-weight:700;border-radius:3px;letter-spacing:0.04em">${escapeHtml(l.codigo)}</span>`)
          .join("");

        const marker = L.circleMarker([parada.lat, parada.lng], {
          radius,
          fillColor: "#0F0A1A",
          color: primary.color,
          weight: 2.2,
          fillOpacity: 1,
        })
          .addTo(map)
          .bindTooltip(
            `<div style="font-family:inherit;min-width:140px">
              <strong style="font-size:13px">${escapeHtml(parada.nombre)}</strong><br>
              <span style="color:#94a3b8;font-size:10.5px;font-variant-numeric:tabular-nums">${Math.round(parada.distancia_m)} m</span>
              <div style="margin-top:6px">${lineasHtml}</div>
            </div>`,
            { direction: "top", offset: [0, -6] },
          );

        // Asociar este marker a TODAS las líneas que pasan por la parada,
        // para que el filtro chip funcione (ocultar/mostrar).
        for (const l of parada.lineas as { codigo: string }[]) {
          if (!lineasLayers.has(l.codigo)) lineasLayers.set(l.codigo, []);
          lineasLayers.get(l.codigo)!.push(marker);
        }
        allBounds.push([parada.lat, parada.lng]);
      }
      layersByLinea.current = lineasLayers;

      // Fit bounds
      if (allBounds.length > 1) {
        map.fitBounds(L.latLngBounds(allBounds), {
          padding: [24, 24],
          maxZoom: 16,
        });
      }
      window.setTimeout(() => map?.invalidateSize(), 80);
    } catch (e) {
      console.error("Error pintando mapa transporte:", e);
      setMapFailed(true);
      if (map) map.remove();
      mapInstanceRef.current = null;
    }

    return () => {
      try {
        mapInstanceRef.current?.remove();
      } catch {
        /* noop */
      }
      mapInstanceRef.current = null;
      layersByLinea.current = new Map();
    };
  }, [data]);

  // Filtrado por línea (resaltar / ocultar resto)
  useEffect(() => {
    const layersMap = layersByLinea.current;
    if (!layersMap) return;
    layersMap.forEach((layers, codigo) => {
      const isActive = activeLinea === codigo;
      const isDim = activeLinea && !isActive;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      layers.forEach((layer: any) => {
        if (typeof layer.setStyle === "function") {
          layer.setStyle({ opacity: isDim ? 0.18 : 0.9 });
        }
      });
    });
  }, [activeLinea]);

  const modos = useMemo(() => {
    if (!data) return [] as { tipo: TransporteTipo; count: number }[];
    const counts: Partial<Record<TransporteTipo, number>> = {};
    for (const l of data.lineas) {
      const t = l.tipo as TransporteTipo;
      counts[t] = (counts[t] ?? 0) + 1;
    }
    return TIPO_ORDER.filter((t) => (counts[t] ?? 0) > 0).map((t) => ({
      tipo: t,
      count: counts[t] ?? 0,
    }));
  }, [data]);

  if (loading) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. LÍNEAS DE TRANSPORTE</span>
        <p>Cargando paradas y líneas…</p>
      </div>
    );
  }

  if (error || !data || data.lineas.length === 0) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. LÍNEAS DE TRANSPORTE</span>
        <p>{error || "Sin transporte público en el radio de captación."}</p>
      </div>
    );
  }

  return (
    <div className={styles.fig}>
      <div className={styles.figHead}>
        <div className={styles.figTitle}>
          <strong>Fig.</strong> Cobertura de líneas de transporte
        </div>
        <div className={styles.figSub}>
          {data.radio_m} m · {data.total_paradas} paradas · {data.total_lineas} líneas
        </div>
      </div>

      <div className={styles.mapWrap}>
        {mapFailed && (
          <div className={styles.mapFallback}>
            No se ha podido cargar el mapa.
          </div>
        )}
        <div className={styles.mapNotice}>
          Mapa simplificado: rutas y paradas más cercanas; los chips incluyen todas las líneas.
        </div>
        <div ref={mapRef} className={styles.leafletMap} />
      </div>

      {/* Filtros chip por línea */}
      <div className={styles.legend}>
        {data.lineas.map((l) => {
          const color = colorForLinea(l);
          const isOn = activeLinea === l.codigo;
          return (
            <button
              key={l.codigo}
              type="button"
              className={`${styles.chip} ${isOn ? styles.chipOn : ""}`}
              onClick={() => setActiveLinea(isOn ? null : l.codigo)}
              style={
                isOn
                  ? { background: color, color: "#fff", borderColor: color }
                  : undefined
              }
              title={`${l.nombre || l.codigo} · ${Math.round(l.dist_min_m)} m`}
            >
              <i className={styles.chipDot} style={{ background: color }} />
              {l.codigo}
            </button>
          );
        })}
      </div>

      <div className={styles.kpiRow}>
        <div className={styles.kpi}>
          <span className={styles.kpiLabel}>Líneas</span>
          <span className={styles.kpiValue}>{data.total_lineas}</span>
          <span className={styles.kpiHint}>en {data.radio_m} m</span>
        </div>
        <div className={styles.kpi}>
          <span className={styles.kpiLabel}>Paradas</span>
          <span className={styles.kpiValue}>{data.total_paradas}</span>
          <span className={styles.kpiHint}>únicas</span>
        </div>
        {modos.map((m) => (
          <div key={m.tipo} className={styles.kpi}>
            <span className={styles.kpiLabel}>{TIPO_LABEL[m.tipo]}</span>
            <span className={styles.kpiValue}>{m.count}</span>
            <span className={styles.kpiHint}>líneas</span>
          </div>
        ))}
      </div>
    </div>
  );
}
