"use client";

import { useState, useMemo } from "react";
import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneList.module.css";

type SortBy  = "score" | "precio" | "m2";
type SortDir = "asc" | "desc";

const DEFAULT_DIR: Record<SortBy, SortDir> = {
  score:  "desc",
  precio: "asc",
  m2:     "desc",
};

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

const SORT_OPTIONS: { key: SortBy; label: string }[] = [
  { key: "score",  label: "Score" },
  { key: "precio", label: "Precio" },
  { key: "m2",     label: "Tamaño" },
];

function ZoneCards({ zonas, selectedId, onSelect }: Omit<Props, "asBottomSheet" | "asFloatingPanel">) {
  const [sortBy,  setSortBy]  = useState<SortBy>("score");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const handleSort = (key: SortBy) => {
    if (key === sortBy) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortBy(key);
      setSortDir(DEFAULT_DIR[key]);
    }
  };

  const sorted = useMemo(() => {
    return [...zonas].sort((a, b) => {
      let aVal: number, bVal: number;
      if (sortBy === "score")       { aVal = a.score_global ?? -1;        bVal = b.score_global ?? -1; }
      else if (sortBy === "precio") { aVal = a.alquiler_mensual ?? 1e9;   bVal = b.alquiler_mensual ?? 1e9; }
      else                          { aVal = a.m2 ?? -1;                  bVal = b.m2 ?? -1; }
      const diff = aVal - bVal;
      return sortDir === "asc" ? diff : -diff;
    });
  }, [zonas, sortBy, sortDir]);

  return (
    <>
      <div className={styles.sortRow}>
        {SORT_OPTIONS.map(opt => {
          const isActive = sortBy === opt.key;
          return (
            <button
              key={opt.key}
              className={`${styles.sortBtn} ${isActive ? styles.sortBtnActive : ""}`}
              onClick={() => handleSort(opt.key)}
            >
              {opt.label}
              {isActive && (
                <svg
                  className={`${styles.sortArrow} ${sortDir === "asc" ? styles.sortArrowUp : ""}`}
                  width="8" height="8" viewBox="0 0 8 8" fill="none"
                >
                  <path d="M1.5 3l2.5 2.5L6.5 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              )}
            </button>
          );
        })}
      </div>
      <div className={styles.list}>
        {sorted.map((zona) => {
          const score = zona.score_global ?? 0;
          const isSelected = selectedId === zona.zona_id;
          const tier = score >= 75 ? "high" : score >= 50 ? "mid" : "low";
          return (
            <button
              key={zona.zona_id}
              className={`${styles.card} ${styles[`cardTier_${tier}`]} ${isSelected ? styles.cardSelected : ""}`}
              onClick={() => onSelect(zona)}
            >
              <div className={styles.info}>
                <div className={styles.nameRow}>
                  <div className={styles.name}>{zona.nombre}</div>
                </div>
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
    </>
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
