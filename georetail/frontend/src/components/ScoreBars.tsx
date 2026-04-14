"use client";

import type { ScoresDimensiones } from "@/lib/types";
import styles from "./ScoreBars.module.css";

// ── SVG Icons ─────────────────────────────────────────────────────────────────
const IconFlujo = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="7" cy="2.5" r="1.4" fill="currentColor" stroke="none"/>
    <path d="M7 4.5v3.5"/>
    <path d="M5 6h4"/>
    <path d="M5.5 8.5L4 12.5M8.5 8.5L10 12.5"/>
  </svg>
);

const IconDemografia = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round">
    <circle cx="4.5" cy="2.8" r="1.4" fill="currentColor" stroke="none"/>
    <circle cx="9.5" cy="3.2" r="1.1" fill="currentColor" stroke="none" opacity="0.65"/>
    <path d="M1.5 12c0-1.7 1.3-3 3-3s3 1.3 3 12"/>
    <path d="M1.5 12c0-1.7 1.3-3 3-3s3 1.3 3 3"/>
    <path d="M7.5 12c0-1.5 1-2.5 2.5-2.5"/>
  </svg>
);

const IconCompetencia = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor">
    <path d="M9.5 1.5L6 7.5H8.5L6 12.5L12 6.5H9L11 1.5H9.5Z"/>
    <path d="M3 4L1.5 7H3L1.5 10L5 7H3.5L5 4H3Z" opacity="0.5"/>
  </svg>
);

const IconAlquiler = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <rect x="1.5" y="4" width="11" height="9" rx="1"/>
    <path d="M1.5 7h11"/>
    <path d="M5.5 4V2.5a1.5 1.5 0 0 1 3 0V4"/>
    <line x1="6" y1="10" x2="8" y2="10"/>
    <line x1="5" y1="8.5" x2="9" y2="8.5"/>
  </svg>
);

const IconTransporte = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <rect x="1.5" y="2.5" width="11" height="7.5" rx="2"/>
    <line x1="1.5" y1="6" x2="12.5" y2="6"/>
    <circle cx="4" cy="11.5" r="1.2" fill="currentColor" stroke="none"/>
    <circle cx="10" cy="11.5" r="1.2" fill="currentColor" stroke="none"/>
    <line x1="4" y1="10" x2="4" y2="10.3"/>
    <line x1="10" y1="10" x2="10" y2="10.3"/>
    <line x1="5.5" y1="4.2" x2="8.5" y2="4.2"/>
  </svg>
);

const IconSeguridad = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <path d="M7 1.5L2 4v4c0 2.8 2.2 5 5 5.5 2.8-.5 5-2.7 5-5.5V4L7 1.5Z"/>
    <path d="M5 7l1.5 1.5L9.5 6"/>
  </svg>
);

const IconTurismo = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <path d="M7 1.5C4.8 1.5 3 3.2 3 5.2c0 3 4 8.3 4 8.3s4-5.3 4-8.3C11 3.2 9.2 1.5 7 1.5z"/>
    <circle cx="7" cy="5.2" r="1.4" fill="currentColor" stroke="none"/>
  </svg>
);

const IconEntornoComercial = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <path d="M1.5 5.5L3 2.5h8L12.5 5.5"/>
    <rect x="1.5" y="5.5" width="11" height="7" rx="0.5"/>
    <path d="M1.5 5.5C1.5 7 2.9 8 4.5 8S7.5 7 7.5 5.5M7.5 5.5C7.5 7 8.8 8 10.5 8S12.5 7 12.5 5.5"/>
    <rect x="5" y="9" width="4" height="3.5" rx="0.5"/>
  </svg>
);

const IconDinamismo = () => (
  <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
    <path d="M2 10.5L5 7.5 7.3 9.8 12 4.5" />
    <path d="M9.5 4.5H12v2.5" />
  </svg>
);
// ─────────────────────────────────────────────────────────────────────────────

const DIMS: Array<{ key: keyof ScoresDimensiones; label: string; Icon: () => JSX.Element }> = [
  { key: "flujo_peatonal",    label: "Flujo peatonal",    Icon: IconFlujo },
  { key: "demografia",        label: "Demografía",         Icon: IconDemografia },
  { key: "competencia",       label: "Competencia",        Icon: IconCompetencia },
  { key: "precio_alquiler",   label: "Precio alquiler",    Icon: IconAlquiler },
  { key: "transporte",        label: "Transporte",         Icon: IconTransporte },
  { key: "seguridad",         label: "Seguridad",          Icon: IconSeguridad },
  { key: "turismo",           label: "Turismo",            Icon: IconTurismo },
  { key: "entorno_comercial", label: "Entorno comercial",  Icon: IconEntornoComercial },
  { key: "dinamismo",         label: "Dinamismo",          Icon: IconDinamismo },
];

interface Props {
  scores: ScoresDimensiones;
}

// Hex values for inline styles (CSS vars can't be used in template-string color mixing)
const SCORE_COLOR = (pct: number) =>
  pct >= 75 ? "#10B981" : pct >= 50 ? "#F59E0B" : "#EF4444";
const SCORE_COLOR_DIM = (pct: number) =>
  pct >= 75 ? "#10B98166" : pct >= 50 ? "#F59E0B66" : "#EF444466";

export default function ScoreBars({ scores }: Props) {
  const dims = DIMS.filter(d => scores[d.key] !== undefined && scores[d.key] !== null);
  if (dims.length === 0) return null;

  return (
    <div className={styles.bars}>
      {dims.map(({ key, label, Icon }, i) => {
        const val = scores[key] as number;
        const pct = Math.min(100, Math.max(0, val));
        const hex = SCORE_COLOR(pct);
        const dim = SCORE_COLOR_DIM(pct);
        return (
          <div key={key} className={styles.bar}>
            <div className={styles.barRow}>
              <span className={styles.barIcon}><Icon /></span>
              <span className={styles.barLabel}>{label}</span>
              <span className={styles.barVal} style={{ color: hex }}>{Math.round(pct)}</span>
            </div>
            <div className={styles.barTrack}>
              <div
                className={styles.barFill}
                style={{
                  width: `${pct}%`,
                  background: hex,
                  animationDelay: `${i * 55}ms`,
                }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
