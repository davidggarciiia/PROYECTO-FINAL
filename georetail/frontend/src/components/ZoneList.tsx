"use client";

import { useState } from "react";
import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneList.module.css";

interface Props {
  zonas: ZonaPreview[];
  selectedId?: string;
  onSelect: (zona: ZonaPreview) => void;
  /** Renders as a floating collapsible panel (desktop) */
  asFloatingPanel?: boolean;
  /** Renders as a bottom sheet (mobile map view) */
  asBottomSheet?: boolean;
}

function ScoreRing({ score }: { score: number }) {
  const r = 18;
  const circ = 2 * Math.PI * r;
  const fill = circ * (score / 100);
  const color = score >= 75 ? "var(--green)" : score >= 50 ? "var(--yellow)" : "var(--red)";
  return (
    <svg width="46" height="46" viewBox="0 0 46 46" className={styles.scoreRing}>
      <circle cx="23" cy="23" r={r} fill="none" stroke="var(--surface-3)" strokeWidth="3.5" />
      <circle
        cx="23" cy="23" r={r} fill="none"
        stroke={color} strokeWidth="3.5"
        strokeDasharray={`${fill} ${circ - fill}`}
        strokeLinecap="round"
        transform="rotate(-90 23 23)"
        style={{ transition: "stroke-dasharray 0.5s ease" }}
      />
      <text x="23" y="27" textAnchor="middle" fontSize="12" fontWeight="800" fill={color}>
        {Math.round(score)}
      </text>
    </svg>
  );
}

function ZoneCards({ zonas, selectedId, onSelect }: Omit<Props, "asBottomSheet" | "asFloatingPanel">) {
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

export default function ZoneList({ zonas, selectedId, onSelect, asFloatingPanel, asBottomSheet }: Props) {
  const [expanded, setExpanded] = useState(true);

  // ── Desktop floating panel ──
  if (asFloatingPanel) {
    return (
      <div className={`${styles.floatingPanel} ${expanded ? styles.floatingExpanded : styles.floatingCollapsed}`}>
        <div className={styles.floatingHeader} onClick={() => setExpanded(e => !e)}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" className={styles.floatingHeaderIcon}>
            <circle cx="6" cy="3" r="1.5" fill="currentColor"/>
            <rect x="3" y="6" width="8" height="1.5" rx="0.75" fill="currentColor"/>
            <rect x="3" y="9.5" width="5" height="1.5" rx="0.75" fill="currentColor"/>
          </svg>
          <span className={styles.floatingTitle}>{zonas.length} ubicaciones</span>
          <svg
            className={`${styles.chevron} ${expanded ? styles.chevronUp : ""}`}
            width="14" height="14" viewBox="0 0 14 14" fill="none"
          >
            <path d="M3 5l4 4 4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>

        <div className={styles.floatingBody}>
          <ZoneCards zonas={zonas} selectedId={selectedId} onSelect={onSelect} />
        </div>
      </div>
    );
  }

  // ── Mobile bottom sheet ──
  if (asBottomSheet) {
    return (
      <div
        className={`${styles.bottomSheet} ${
          expanded ? styles.bottomSheetExpanded : styles.bottomSheetCollapsed
        }`}
      >
        <div className={styles.sheetHeader} onClick={() => setExpanded(e => !e)}>
          <div className={styles.sheetHandle} />
          <span className={styles.sheetTitle}>{zonas.length} ubicaciones</span>
          <svg
            className={`${styles.sheetToggleIcon} ${expanded ? styles.sheetToggleExpanded : ""}`}
            width="16" height="16" viewBox="0 0 16 16" fill="none"
          >
            <path d="M4 6l4 4 4-4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
        {expanded && (
          <ZoneCards zonas={zonas} selectedId={selectedId} onSelect={onSelect} />
        )}
      </div>
    );
  }

  // ── Default: plain list ──
  return <ZoneCards zonas={zonas} selectedId={selectedId} onSelect={onSelect} />;
}
