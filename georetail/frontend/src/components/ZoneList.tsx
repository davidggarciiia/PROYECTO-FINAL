"use client";

import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneList.module.css";

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onSelect: (zona: ZonaPreview) => void;
}

function scoreColor(score?: number): "verde" | "amarillo" | "rojo" {
  if (!score) return "amarillo";
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

export default function ZoneList({ zonas, selectedId, onSelect }: Props) {
  return (
    <div className={styles.list}>
      {zonas.map((zona, idx) => {
        const color = scoreColor(zona.score_global);
        const score = zona.score_global !== undefined ? Math.round(zona.score_global) : null;
        const isSelected = selectedId === zona.zona_id;

        return (
          <button
            key={zona.zona_id}
            className={`${styles.item} ${isSelected ? styles.selected : ""} animate-in`}
            style={{ animationDelay: `${idx * 30}ms` }}
            onClick={() => onSelect(zona)}
          >
            <div className={`score-ring score-ring-${color} ${styles.ring}`}>
              {score !== null ? score : "–"}
            </div>
            <div className={styles.info}>
              <div className={styles.name}>{zona.nombre}</div>
              <div className={styles.sub}>{zona.barrio} · {zona.distrito}</div>
              <div className={styles.meta}>
                {zona.alquiler_mensual && (
                  <span>{zona.alquiler_mensual.toLocaleString("es-ES")} €/mes</span>
                )}
                {zona.m2 && <span>{zona.m2} m²</span>}
              </div>
            </div>
          </button>
        );
      })}
    </div>
  );
}
