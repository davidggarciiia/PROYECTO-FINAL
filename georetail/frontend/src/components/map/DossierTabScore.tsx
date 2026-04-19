"use client";

import { useState } from "react";
import type { ZonaPreview, LocalDetalleResponse, ScoresDimensiones } from "@/lib/types";
import { DIMENSIONS, type DimensionKey } from "./dimensions";
import styles from "./Dossier.module.css";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
}

function scoreBand(score?: number | null): "hi" | "mid" | "lo" | "na" {
  if (score == null) return "na";
  if (score >= 75) return "hi";
  if (score >= 55) return "mid";
  return "lo";
}

export default function DossierTabScore({ zone, detalle, loading }: Props) {
  const [openKey, setOpenKey] = useState<DimensionKey | null>(null);

  const dims: ScoresDimensiones = detalle?.zona.scores_dimensiones ?? {};
  const prob = detalle?.zona.probabilidad_supervivencia;

  if (loading && !detalle) {
    return (
      <div className={styles.loading}>
        <div className="spinner" />
        <span>Calculando análisis…</span>
      </div>
    );
  }

  return (
    <div className={styles.scoreTab}>
      {prob != null && (
        <div className={styles.probBanner}>
          <div className={styles.probLabel}>PROBABILIDAD SUPERVIVENCIA 3 AÑOS</div>
          <div className={styles.probValue}>
            {Math.round(prob * 100)}
            <span className={styles.probPct}>%</span>
          </div>
          <div className={styles.probBarTrack}>
            <div
              className={styles.probBarFill}
              style={{ width: `${Math.round(prob * 100)}%` }}
            />
          </div>
        </div>
      )}

      <div className={styles.dimGrid}>
        {DIMENSIONS.map((dim) => {
          const val = dims[dim.key];
          const band = scoreBand(val);
          const isOpen = openKey === dim.key;
          return (
            <div key={dim.key} className={styles.dimCellWrap}>
              <button
                type="button"
                className={`${styles.dimCell} ${styles[`dim_${band}`]} ${isOpen ? styles.dimCellOpen : ""}`}
                onClick={() => setOpenKey(isOpen ? null : dim.key)}
                aria-expanded={isOpen}
              >
                <div className={styles.dimCellHead}>
                  <span className={styles.dimCellName}>{dim.name}</span>
                  <span className={styles.dimCellVal}>
                    {val != null ? Math.round(val) : "—"}
                  </span>
                </div>
                <div className={styles.dimCellBarTrack}>
                  <div
                    className={styles.dimCellBarFill}
                    style={{ width: val != null ? `${Math.max(0, Math.min(100, val))}%` : "0%" }}
                  />
                </div>
                <div className={styles.dimCellFoot}>
                  <span>{dim.short}</span>
                  <span className={styles.dimCellHint}>{dim.hint}</span>
                </div>
              </button>

              {isOpen && (
                <div className={styles.dimDrawer}>
                  <div className={styles.drawerEyebrow}>
                    <span className={styles.tick}>●</span>
                    {dim.name.toUpperCase()} · DESGLOSE
                  </div>
                  <p className={styles.drawerSubtitle}>{dim.subtitle}</p>
                  <p className={styles.drawerWhat}>{dim.what}</p>
                  <div className={styles.drawerSources}>
                    {dim.sources.map((s) => (
                      <span key={s} className={styles.sourceChip}>
                        {s}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {detalle?.zona.analisis_ia?.resumen_global && (
        <div className={styles.summary}>
          <div className={styles.summaryEyebrow}>
            <span className={styles.tick}>●</span>RESUMEN IA
          </div>
          <p className={styles.summaryText}>{detalle.zona.analisis_ia.resumen_global}</p>
        </div>
      )}
    </div>
  );
}
