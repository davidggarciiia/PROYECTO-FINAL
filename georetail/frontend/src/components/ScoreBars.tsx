"use client";

import type { ScoresDimensiones } from "@/lib/types";
import styles from "./ScoreBars.module.css";

const LABELS: Record<keyof ScoresDimensiones, string> = {
  flujo_peatonal:    "Flujo peatonal",
  demografia:        "Demografía",
  competencia:       "Competencia",
  precio_alquiler:   "Precio alquiler",
  transporte:        "Transporte",
  seguridad:         "Seguridad",
  turismo:           "Turismo",
  entorno_comercial: "Entorno comercial",
};

interface Props {
  scores: ScoresDimensiones;
}

export default function ScoreBars({ scores }: Props) {
  const entries = Object.entries(LABELS) as Array<[keyof ScoresDimensiones, string]>;
  return (
    <div className={styles.bars}>
      {entries.map(([key, label]) => {
        const val = scores[key];
        if (val === undefined || val === null) return null;
        const pct = Math.min(100, Math.max(0, val));
        const color = pct >= 75 ? "var(--green)" : pct >= 50 ? "var(--yellow)" : "var(--red)";
        return (
          <div key={key} className={styles.bar}>
            <div className={styles.barHeader}>
              <span>{label}</span>
              <span className={styles.barVal} style={{ color }}>{Math.round(pct)}</span>
            </div>
            <div className={styles.barTrack}>
              <div className={styles.barFill} style={{ width: `${pct}%`, background: color }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}
