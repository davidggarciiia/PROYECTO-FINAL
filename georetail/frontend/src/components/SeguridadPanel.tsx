"use client";

import type { SeguridadDetalle } from "@/lib/types";
import styles from "./SeguridadPanel.module.css";

interface SeguridadPanelProps {
  detalle?: SeguridadDetalle | null;
  score?: number | null;
  explicacion_bullets?: string[];
}

const BCN_REF = {
  hurtos: 12,
  robos: 4,
  danys: 2,
};

function fmt1(n: number): string {
  return n.toLocaleString("es-ES", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

function fmtInt(n: number): string {
  return n.toLocaleString("es-ES", { maximumFractionDigits: 0 });
}

function hasAnyData(d: SeguridadDetalle): boolean {
  return (
    d.incidencias_por_1000hab != null ||
    d.hurtos_por_1000hab != null ||
    d.robatoris_por_1000hab != null ||
    d.danys_por_1000hab != null ||
    d.incidencias_noche_pct != null ||
    d.comisarias_1km != null ||
    d.dist_comisaria_m != null ||
    d.seguridad_barri_score != null
  );
}

function buildInterpretation(d: SeguridadDetalle): string[] {
  const frases: string[] = [];
  const hurtos = d.hurtos_por_1000hab;
  const robos = d.robatoris_por_1000hab;
  const nochePct = d.incidencias_noche_pct;
  const comisarias = d.comisarias_1km;

  if (hurtos != null && nochePct != null && hurtos > 15 && nochePct > 35) {
    frases.push(
      "Zona con alta actividad delictiva nocturna. Relevante para negocios con horario tarde-noche."
    );
  } else if (hurtos != null && robos != null && hurtos < 5 && robos < 2) {
    frases.push(
      "Zona tranquila. Riesgo de incidencias bajo respecto a la media de Barcelona."
    );
  }

  if (comisarias != null && comisarias >= 1) {
    frases.push("Presencia policial en el radio de 1 km es un factor disuasorio.");
  }

  return frases;
}

function scoreColor(s: number): string {
  if (s >= 70) return "var(--green)";
  if (s >= 50) return "var(--yellow)";
  return "var(--red)";
}

interface IncDef {
  label: string;
  value: number;
  ref: number;
}

export default function SeguridadPanel({
  detalle,
  score,
  explicacion_bullets,
}: SeguridadPanelProps) {
  if (!detalle || !hasAnyData(detalle)) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Sin datos de seguridad disponibles para esta zona.
        </div>
      </div>
    );
  }

  const displayScore = score ?? detalle.seguridad_barri_score ?? null;
  const ringColor = displayScore != null ? scoreColor(displayScore) : "var(--surface-3)";
  const ringPct = displayScore != null ? Math.min(Math.max(displayScore, 0), 100) : 0;

  const hasIncBars =
    detalle.hurtos_por_1000hab != null ||
    detalle.robatoris_por_1000hab != null ||
    detalle.danys_por_1000hab != null;

  const incDefs: IncDef[] = [
    { label: "Hurtos", value: detalle.hurtos_por_1000hab ?? 0, ref: BCN_REF.hurtos },
    { label: "Robatoris", value: detalle.robatoris_por_1000hab ?? 0, ref: BCN_REF.robos },
    { label: "Daños", value: detalle.danys_por_1000hab ?? 0, ref: BCN_REF.danys },
  ];

  const maxScale = Math.max(...incDefs.map((d) => Math.max(d.value, d.ref)), 0.01) * 1.15;

  const nochePct = detalle.incidencias_noche_pct;
  const diaPct = nochePct != null ? 100 - nochePct : null;

  const interpretacion = buildInterpretation(detalle);

  return (
    <div className={styles.panel}>

      {/* ── Score ring gauge ────────────────────────────────── */}
      {displayScore != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Índice de seguridad</div>
          <div className={styles.ringRow}>
            <div
              className={styles.ring}
              style={{
                background: `conic-gradient(${ringColor} calc(${ringPct} * 1%), var(--surface-3) 0)`,
              }}
              role="img"
              aria-label={`Score de seguridad: ${Math.round(ringPct)} sobre 100`}
            >
              <div className={styles.ringInner}>
                <span className={styles.ringScore}>{Math.round(ringPct)}</span>
              </div>
            </div>
            <div className={styles.ringMeta}>
              <span className={styles.ringLabel}>Seguridad</span>
              <span
                className={styles.ringChip}
                style={{
                  background:
                    ringPct >= 70
                      ? "rgba(16,185,129,0.12)"
                      : ringPct >= 50
                      ? "rgba(245,158,11,0.12)"
                      : "rgba(239,68,68,0.12)",
                  borderColor:
                    ringPct >= 70
                      ? "rgba(16,185,129,0.3)"
                      : ringPct >= 50
                      ? "rgba(245,158,11,0.3)"
                      : "rgba(239,68,68,0.3)",
                  color: ringColor,
                }}
              >
                {ringPct >= 70 ? "Zona segura" : ringPct >= 50 ? "Riesgo moderado" : "Alto riesgo"}
              </span>
            </div>
          </div>
        </section>
      )}

      {/* ── Incident bars with dual reference ───────────────── */}
      {hasIncBars && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Incidencias por tipo · ref. BCN</div>
          <div className={styles.incList}>
            {incDefs.map((bar) => {
              const fillPct = (bar.value / maxScale) * 100;
              const refPct = (bar.ref / maxScale) * 100;
              const aboveRef = bar.value > bar.ref;
              const wellAbove = bar.value > bar.ref * 1.5;
              const barColor = !aboveRef
                ? "var(--green)"
                : wellAbove
                ? "var(--red)"
                : "var(--yellow)";

              return (
                <div key={bar.label} className={styles.incRow}>
                  <div className={styles.incLabelGroup}>
                    <span className={styles.incLabel}>{bar.label}</span>
                    <span className={styles.incValue}>{fmt1(bar.value)} / 1.000 hab.</span>
                  </div>
                  <div className={styles.incTrack} role="presentation">
                    <div
                      className={styles.incFill}
                      style={{ width: `${fillPct}%`, background: barColor }}
                    />
                    <div
                      className={styles.incRefLine}
                      style={{ left: `${refPct}%` }}
                      title={`Ref. BCN: ${fmt1(bar.ref)}`}
                    />
                  </div>
                  <span className={styles.incRefLabel}>ref. {fmt1(bar.ref)}</span>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {/* ── Night / day split ───────────────────────────────── */}
      {nochePct != null && diaPct != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Distribución día / noche</div>
          <div className={styles.splitTrack} role="presentation">
            <div className={styles.splitDia} style={{ width: `${diaPct}%` }}>
              {diaPct >= 20 && (
                <span className={styles.splitLabel}>Día {Math.round(diaPct)}%</span>
              )}
            </div>
            <div className={styles.splitNoche} style={{ width: `${nochePct}%` }}>
              {nochePct >= 20 && (
                <span className={styles.splitLabel}>Noche {Math.round(nochePct)}%</span>
              )}
            </div>
          </div>
          {nochePct > 30 && (
            <span className={styles.splitWarnChip}>Alta actividad nocturna</span>
          )}
        </section>
      )}

      {/* ── Police presence ─────────────────────────────────── */}
      {(detalle.comisarias_1km != null || detalle.dist_comisaria_m != null) && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Presencia policial</div>
          <div className={styles.policeRow}>
            {detalle.comisarias_1km != null && (
              <span
                className={styles.policeChip}
                style={
                  detalle.comisarias_1km >= 1
                    ? {
                        background: "rgba(16,185,129,0.1)",
                        borderColor: "rgba(16,185,129,0.3)",
                        color: "var(--green)",
                      }
                    : {
                        background: "rgba(255,255,255,0.04)",
                        borderColor: "var(--border)",
                        color: "var(--text-subtle)",
                      }
                }
              >
                {detalle.comisarias_1km === 0
                  ? "Sin comisarías cercanas"
                  : detalle.comisarias_1km === 1
                  ? "1 comisaría"
                  : `${fmtInt(detalle.comisarias_1km)} comisarías`}
              </span>
            )}
            {detalle.dist_comisaria_m != null && (
              <span className={styles.policeDist}>
                a {fmtInt(detalle.dist_comisaria_m)} m
              </span>
            )}
          </div>
        </section>
      )}

      {/* ── Interpretation block ─────────────────────────────── */}
      {interpretacion.length > 0 && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Interpretación de tendencia</div>
          <div className={styles.interpretBlock}>
            {interpretacion.map((frase, i) => (
              <p key={i} className={styles.interpretFrase}>
                {frase}
              </p>
            ))}
          </div>
        </section>
      )}

      {/* ── Bullets ─────────────────────────────────────────── */}
      {explicacion_bullets && explicacion_bullets.length > 0 && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Hechos clave</div>
          <div className={styles.bulletRow}>
            {explicacion_bullets.map((b, i) => (
              <span key={i} className={styles.bulletPill}>
                {b}
              </span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
