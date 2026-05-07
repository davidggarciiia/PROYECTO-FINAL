"use client";

import type { CSSProperties } from "react";
import { probToPct, veredictoFromScore, formatPeatones, formatEurWithSymbol } from "./helpers";
import styles from "./Lede.module.css";

interface Props {
  /** Probabilidad de supervivencia 3a (0..1) — del modelo. */
  probabilidad?: number | null;
  /** Score global 0..100 — fallback si no hay probabilidad. */
  scoreGlobal?: number | null;
  /** Resumen IA o headline corto del análisis (1-2 frases). */
  headline?: string | null;
  /** Métricas ancla del lede — todas opcionales, se ocultan si null. */
  puntoMuerto?: number | null;        // €/mes desde /api/financiero
  flujoPotencial?: number | null;     // peat/día desde vcity_flujo_peatonal
  huecoDirecto?: number | null;       // n competidores directos < 200 m
  paybackMeses?: number | null;       // payback base
  /** True mientras /api/competencia (LLM relacional) no ha resuelto. */
  competenciaCalibrando?: boolean;
}

/**
 * Lede del informe — gauge cónico de probabilidad + 3 métricas headline.
 *
 * Sustituye al "ProbBanner" del dossier viejo. El gauge cónico es coherente
 * con el `dimensionGauge` que ya usa el repo (mismo conic-gradient pattern).
 *
 * Las métricas están elegidas para responder a las 3 preguntas que un
 * emprendedor se hace antes de firmar: ¿cuánto tengo que vender?, ¿cuánta
 * gente pasa?, ¿hay hueco?
 */
export default function Lede({
  probabilidad,
  scoreGlobal,
  headline,
  puntoMuerto,
  flujoPotencial,
  huecoDirecto,
  paybackMeses,
  competenciaCalibrando = false,
}: Props) {
  const pct = probToPct(probabilidad) ?? scoreGlobal ?? null;
  const ver = veredictoFromScore(pct);

  const colorVar =
    ver.band === "hi" ? "var(--green)" :
    ver.band === "mid" ? "var(--yellow)" :
    ver.band === "lo" ? "var(--red)" : "var(--text-subtle)";

  const dimVar =
    ver.band === "hi" ? "var(--green-dim)" :
    ver.band === "mid" ? "var(--yellow-dim)" :
    ver.band === "lo" ? "var(--red-dim)" : "var(--surface-3)";

  const borderVar =
    ver.band === "hi" ? "var(--green-border)" :
    ver.band === "mid" ? "var(--yellow-border)" :
    ver.band === "lo" ? "var(--red-border)" : "var(--border)";

  const gaugePct = pct ?? 0;

  return (
    <section className={styles.lede}>
      <div className={styles.gaugeCol}>
        <div
          className={styles.gauge}
          style={{
            "--score-pct": `${gaugePct}%`,
            "--gauge-color": colorVar,
          } as CSSProperties}
          role="img"
          aria-label={`Probabilidad de supervivencia: ${pct ?? "sin datos"} sobre 100`}
        >
          <div className={styles.gaugeInner}>
            <span className={styles.gaugeNum} style={{ color: colorVar }}>
              {pct != null ? pct : (competenciaCalibrando ? "···" : "—")}
            </span>
            <span className={styles.gaugeUnit}>
              {competenciaCalibrando && pct == null ? "calibrando" : "/ 100"}
            </span>
          </div>
        </div>
        <span
          className={styles.veredictoChip}
          style={{
            color: colorVar,
            background: dimVar,
            borderColor: borderVar,
          }}
        >
          {competenciaCalibrando && pct == null ? "Esperando competencia LLM…" : ver.label}
        </span>
      </div>

      <div className={styles.copyCol}>
        <span className={styles.kicker}>
          Probabilidad de supervivencia · 24 m
        </span>
        <h2 className={styles.headline}>
          {headline?.trim()
            ? headline
            : pct != null && pct >= 75
              ? "El cruce de datos sostiene una recomendación favorable para esta ubicación."
              : pct != null && pct >= 55
                ? "La ubicación es viable pero exige mitigar un par de palancas críticas antes de firmar."
                : pct != null
                  ? "El conjunto de indicadores recomienda replantear la ubicación o el modelo."
                  : competenciaCalibrando
                    ? "Esperando el análisis relacional de competencia para cerrar la recomendación. El resto de dimensiones ya está disponible."
                    : "Aún no hay datos suficientes para cerrar la recomendación."}
        </h2>

        {(puntoMuerto != null || flujoPotencial != null || huecoDirecto != null) && (
          <div className={styles.metrics}>
            {flujoPotencial != null && (
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Tráfico potencial</span>
                <span className={styles.metricValue}>
                  {formatPeatones(flujoPotencial)}
                  <em className={styles.metricUnit}>peat./día</em>
                </span>
              </div>
            )}
            {puntoMuerto != null && puntoMuerto > 0 && (
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Punto muerto</span>
                <span className={styles.metricValue}>
                  {formatEurWithSymbol(puntoMuerto)}
                  <em className={styles.metricUnit}>/ mes</em>
                </span>
              </div>
            )}
            {paybackMeses != null && paybackMeses > 0 && (
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Payback estimado</span>
                <span className={styles.metricValue}>
                  {Math.round(paybackMeses)}
                  <em className={styles.metricUnit}>meses</em>
                </span>
              </div>
            )}
            {huecoDirecto != null && (
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Competencia directa</span>
                <span className={styles.metricValue}>
                  {huecoDirecto}
                  <em className={styles.metricUnit}>en 200 m</em>
                </span>
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
