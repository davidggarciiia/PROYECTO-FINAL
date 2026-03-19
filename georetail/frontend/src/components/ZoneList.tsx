"use client";

import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneList.module.css";

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onSelect: (zona: ZonaPreview) => void;
}

function ScoreRing({ score }: { score: number }) {
  const r = 14;
  const circ = 2 * Math.PI * r;
  const fill = circ * (score / 100);
  const color = score >= 75 ? "var(--green)" : score >= 50 ? "var(--yellow)" : "var(--red)";
  return (
    <svg width="36" height="36" viewBox="0 0 36 36" className={styles.scoreRing}>
      <circle cx="18" cy="18" r={r} fill="none" stroke="var(--surface-3)" strokeWidth="3" />
      <circle
        cx="18" cy="18" r={r} fill="none"
        stroke={color} strokeWidth="3"
        strokeDasharray={`${fill} ${circ - fill}`}
        strokeLinecap="round"
        transform="rotate(-90 18 18)"
        style={{ transition: "stroke-dasharray 0.5s ease" }}
      />
      <text x="18" y="22" textAnchor="middle" fontSize="9" fontWeight="700" fill={color}>
        {Math.round(score)}
      </text>
    </svg>
  );
}

export default function ZoneList({ zonas, selectedId, onSelect }: Props) {
  return (
    <div className={styles.list}>
      {zonas.map((zona, idx) => {
        const score = zona.score_global ?? 0;
        const isSelected = selectedId === zona.zona_id;
        return (
          <button
            key={zona.zona_id}
            className={`${styles.card} ${isSelected ? styles.cardSelected : ""}`}
            onClick={() => onSelect(zona)}
          >
            <div className={styles.rank}>#{idx + 1}</div>
            <div className={styles.info}>
              <div className={styles.name}>{zona.nombre}</div>
              <div className={styles.sub}>{zona.barrio} · {zona.distrito}</div>
              <div className={styles.meta}>
                {zona.alquiler_mensual && (
                  <span className={styles.metaItem}>
                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                      <circle cx="5" cy="5" r="4" stroke="currentColor" strokeWidth="1"/>
                      <path d="M5 2.5v5M3 4h3.5" stroke="currentColor" strokeWidth="1" strokeLinecap="round"/>
                    </svg>
                    {zona.alquiler_mensual.toLocaleString("es-ES")} €/mes
                  </span>
                )}
                {zona.m2 && (
                  <span className={styles.metaItem}>
                    <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                      <rect x="1" y="1" width="8" height="8" rx="1" stroke="currentColor" strokeWidth="1"/>
                    </svg>
                    {zona.m2} m²
                  </span>
                )}
              </div>
            </div>
            <ScoreRing score={score} />
          </button>
        );
      })}
    </div>
  );
}
