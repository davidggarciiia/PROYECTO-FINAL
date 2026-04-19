"use client";

/**
 * MapCanvas — Leaflet map layer with 4 basemaps and KnowPoints "Signal" markers.
 * Port of prototype_v1/mapview.jsx. Pure map layer; HUD is composed by the page shell.
 */

import { useEffect, useRef } from "react";
import type { ZonaPreview } from "@/lib/types";
import styles from "./MapCanvas.module.css";

export type BasemapId = "dark" | "light" | "voya" | "sat";

interface BasemapConfig {
  base: string;
  labels: string;
  subdomains: string;
  attr: string;
  labelOpacity: number;
}

const BASEMAPS: Record<BasemapId, BasemapConfig> = {
  dark: {
    base: "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png",
    labels: "https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png",
    subdomains: "abcd",
    attr: "© OpenStreetMap · CARTO",
    labelOpacity: 0.55,
  },
  light: {
    base: "https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}.png",
    labels: "https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
    subdomains: "abcd",
    attr: "© OpenStreetMap · CARTO",
    labelOpacity: 0.9,
  },
  voya: {
    base: "https://{s}.basemaps.cartocdn.com/rastertiles/voyager_nolabels/{z}/{x}/{y}.png",
    labels: "https://{s}.basemaps.cartocdn.com/rastertiles/voyager_only_labels/{z}/{x}/{y}.png",
    subdomains: "abcd",
    attr: "© OpenStreetMap · CARTO",
    labelOpacity: 0.85,
  },
  sat: {
    base: "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    labels: "https://{s}.basemaps.cartocdn.com/rastertiles/voyager_only_labels/{z}/{x}/{y}.png",
    subdomains: "abcd",
    attr: "© Esri · OSM · CARTO",
    labelOpacity: 0.85,
  },
};

const BCN_CENTER: [number, number] = [41.397, 2.175];

interface Props {
  zonas: ZonaPreview[];
  activeId?: string | null;
  basemap: BasemapId;
  onPick: (id: string) => void;
  onMove?: (coords: { lat: number; lng: number; zoom: number }) => void;
}

function scoreBand(score?: number | null): "hi" | "mid" | "lo" {
  const s = score ?? 0;
  if (s >= 75) return "hi";
  if (s >= 55) return "mid";
  return "lo";
}

export default function MapCanvas({ zonas, activeId, basemap, onPick, onMove }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const mapRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const baseLayerRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const labelLayerRef = useRef<any>(null);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const markersRef = useRef<Record<string, any>>({});
  const onPickRef = useRef(onPick);
  const onMoveRef = useRef(onMove);

  useEffect(() => {
    onPickRef.current = onPick;
    onMoveRef.current = onMove;
  }, [onPick, onMove]);

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const L = require("leaflet");

    const map = L.map(containerRef.current, {
      center: BCN_CENTER,
      zoom: 13,
      zoomControl: false,
      attributionControl: true,
    });
    L.control.zoom({ position: "bottomright" }).addTo(map);
    mapRef.current = map;

    const emitMove = () => {
      if (!onMoveRef.current) return;
      const c = map.getCenter();
      onMoveRef.current({ lat: c.lat, lng: c.lng, zoom: map.getZoom() });
    };
    map.on("move", emitMove);
    map.on("zoom", emitMove);
    emitMove();

    return () => {
      map.off("move", emitMove);
      map.off("zoom", emitMove);
      map.remove();
      mapRef.current = null;
      baseLayerRef.current = null;
      labelLayerRef.current = null;
      markersRef.current = {};
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Swap basemap when prop changes
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const L = require("leaflet");
    const cfg = BASEMAPS[basemap] ?? BASEMAPS.dark;

    if (baseLayerRef.current) map.removeLayer(baseLayerRef.current);
    if (labelLayerRef.current) map.removeLayer(labelLayerRef.current);

    baseLayerRef.current = L.tileLayer(cfg.base, {
      attribution: cfg.attr + " · KnowPoints",
      subdomains: cfg.subdomains,
      maxZoom: 19,
    }).addTo(map);

    labelLayerRef.current = L.tileLayer(cfg.labels, {
      subdomains: cfg.subdomains,
      maxZoom: 19,
      opacity: cfg.labelOpacity,
    }).addTo(map);

    if (containerRef.current?.parentElement) {
      containerRef.current.parentElement.setAttribute("data-basemap", basemap);
    }
  }, [basemap]);

  // Render markers when zonas change
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const L = require("leaflet");

    // Clear previous markers
    Object.values(markersRef.current).forEach((m) => map.removeLayer(m));
    markersRef.current = {};

    zonas.forEach((z) => {
      if (typeof z.lat !== "number" || typeof z.lng !== "number") return;
      const band = scoreBand(z.score_global);
      const scoreStr = z.score_global != null ? String(Math.round(z.score_global)) : "—";
      const html = `
        <div class="kp-mk ${band}" data-id="${z.zona_id}">
          <div class="kp-mk-ring"></div>
          <div class="kp-mk-core">${scoreStr}</div>
          <div class="kp-mk-tail"></div>
          <div class="kp-mk-dot"></div>
        </div>`;
      const icon = L.divIcon({ html, className: "", iconSize: [0, 0], iconAnchor: [0, 0] });
      const m = L.marker([z.lat, z.lng], { icon }).addTo(map);
      m.on("click", () => onPickRef.current(z.zona_id));
      markersRef.current[z.zona_id] = m;
    });

    // Fit bounds on first batch
    if (zonas.length > 0) {
      const valid = zonas.filter((z) => typeof z.lat === "number" && typeof z.lng === "number");
      if (valid.length > 0) {
        const bounds = L.latLngBounds(valid.map((z) => [z.lat, z.lng]));
        if (bounds.isValid()) {
          map.fitBounds(bounds, { padding: [80, 80], maxZoom: 15 });
        }
      }
    }
  }, [zonas]);

  // Update active marker state + pan
  useEffect(() => {
    Object.entries(markersRef.current).forEach(([id, m]) => {
      const el = m.getElement?.();
      if (!el) return;
      const mk = el.querySelector(".kp-mk");
      if (!mk) return;
      mk.classList.toggle("is-active", id === activeId);
    });
    const map = mapRef.current;
    if (activeId && map) {
      const z = zonas.find((x) => x.zona_id === activeId);
      if (z && typeof z.lat === "number" && typeof z.lng === "number") {
        map.panTo([z.lat, z.lng], { animate: true, duration: 0.6 });
      }
    }
  }, [activeId, zonas]);

  return <div ref={containerRef} className={styles.map} id="kp-map" />;
}
