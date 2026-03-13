"use client";

import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneList.module.css";

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onSelect: (zona: ZonaPreview) => void;
}

function scoreColor(score?: number): string {
  if (!score) return "amarillo";
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

export default function ZoneList({ zonas, selectedId, onSelect }: Props) {
  return (
    <div className={styles.list}>
      {zonas.map(zona => {
        const color = scoreColor(zona.score_global);
        const score = zona.score_global ? Math.round(zona.score_global) : null;
        return (
          <button
            key={zona.zona_id}
            className={`${styles.item} ${selectedId === zona.zona_id ? styles.selected : ""}`}
            onClick={() => onSelect(zona)}
          >
            <div className={styles.header}>
              <div className={styles.name}>{zona.nombre}</div>
              {score !== null && (
                <span className={`badge badge-${color}`}>{score}</span>
              )}
            </div>
            <div className={styles.sub}>{zona.barrio} · {zona.distrito}</div>
            <div className={styles.meta}>
              {zona.alquiler_mensual && <span>{zona.alquiler_mensual.toLocaleString("es-ES")} €/mes</span>}
              {zona.m2 && <span>{zona.m2} m²</span>}
            </div>
          </button>
        );
      })}
    </div>
  );
}
