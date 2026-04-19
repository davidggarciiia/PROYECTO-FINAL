"use client";

import styles from "./HudCoord.module.css";

interface Props {
  lat: number;
  lng: number;
  zoom: number;
  label?: string;
}

function fmtDeg(v: number, pos: string, neg: string): string {
  const abs = Math.abs(v).toFixed(3);
  const dir = v >= 0 ? pos : neg;
  return `${abs} ${dir}`;
}

function scaleFromZoom(z: number): string {
  // Rough human-readable scale (Leaflet style)
  const zClamped = Math.max(1, Math.min(20, Math.round(z)));
  const scales: Record<number, string> = {
    10: "1:140.000",
    11: "1:70.000",
    12: "1:35.000",
    13: "1:20.000",
    14: "1:10.000",
    15: "1:5.000",
    16: "1:2.500",
    17: "1:1.200",
    18: "1:600",
    19: "1:300",
    20: "1:150",
  };
  return scales[zClamped] ?? `1:${Math.round(591657550 / 2 ** zClamped)}`;
}

export default function HudCoord({ lat, lng, zoom, label }: Props) {
  return (
    <div className={styles.coord}>
      <div>
        <strong>{fmtDeg(lat, "N", "S")}</strong> · <strong>{fmtDeg(lng, "E", "W")}</strong>
      </div>
      <div>{label ?? "BARCELONA · CIUTAT VELLA"}</div>
      <div>
        ZOOM {Math.round(zoom)} · ESC {scaleFromZoom(zoom)}
      </div>
    </div>
  );
}
