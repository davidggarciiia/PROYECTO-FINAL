"use client";

import type { ScoresDimensiones } from "@/lib/types";
import styles from "./ScoreBars.module.css";

const DIMS: Array<{ key: keyof ScoresDimensiones; label: string; icon: string }> = [
  { key: "flujo_peatonal",    label: "Flujo peatonal",    icon: "👥" },
  { key: "demografia",        label: "Demografía",         icon: "🏘" },
  { key: "competencia",       label: "Competencia",        icon: "⚡" },
  { key: "precio_alquiler",   label: "Precio alquiler",    icon: "💶" },
  { key: "transporte",        label: "Transporte",         icon: "🚇" },
  { key: "seguridad",         label: "Seguridad",          icon: "🛡" },
  { key: "turismo",           label: "Turismo",            icon: "🗺" },
  { key: "entorno_comercial", label: "Entorno comercial",  icon: "🏪" },
];

interface Props {
  scores: ScoresDimensiones;
}

export default function ScoreBars({ scores }: Props) {
  const dims = DIMS.filter(d => scores[d.key] !== undefined && scores[d.key] !== null);
  if (dims.length === 0) return null;

  return (
    <div className={styles.bars}>
      {dims.map(({ key, label, icon }) => {
        const val = scores[key] as number;
        const pct = Math.min(100, Math.max(0, val));
        const color = pct >= 75 ? "var(--green)" : pct >= 50 ? "var(--yellow)" : "var(--red)";
        const bgColor = pct >= 75 ? "var(--green-dim)" : pct >= 50 ? "var(--yellow-dim)" : "var(--red-dim)";
        return (
          <div key={key} className={styles.bar}>
            <div className={styles.barRow}>
              <span className={styles.barIcon}>{icon}</span>
              <span className={styles.barLabel}>{label}</span>
              <span className={styles.barVal} style={{ color }}>{Math.round(pct)}</span>
            </div>
            <div className={styles.barTrack}>
              <div
                className={styles.barFill}
                style={{
                  width: `${pct}%`,
                  background: `linear-gradient(90deg, ${color}aa, ${color})`,
                  boxShadow: `0 0 8px ${color}44`,
                }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
