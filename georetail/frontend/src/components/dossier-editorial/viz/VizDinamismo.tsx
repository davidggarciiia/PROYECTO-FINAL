"use client";

import { useMemo } from "react";
import type { ZonaDetalle } from "@/lib/types";
import { mesLabel } from "../helpers";
import styles from "./VizDinamismo.module.css";

interface Props {
  zona: ZonaDetalle | null;
}

const W = 460;
const H = 200;
const PAD_L = 32;
const PAD_R = 14;
const PAD_T = 18;
const PAD_B = 30;
const INNER_W = W - PAD_L - PAD_R;
const INNER_H = H - PAD_T - PAD_B;

/**
 * VizDinamismo — viz exacta del Claude design (editorial.jsx VizDin).
 *
 * Línea de aperturas vs cierres últimos 24 meses, con datos REALES del
 * backend (campo `serie_aperturas_cierres_24m` calculado en api/local.py
 * desde la tabla `licencias_actividad`).
 *
 * Aperturas → línea verde + área. Cierres → línea roja punteada.
 *
 * Si no hay serie disponible en el response (zona sin licencias atorgadas
 * en el periodo), mostramos fallback informativo.
 */
export default function VizDinamismo({ zona }: Props) {
  const serie = zona?.serie_aperturas_cierres_24m ?? [];
  const tendencia = (zona as unknown as { tendencia?: string | null })?.tendencia ?? null;
  const ratio = zona?.ratio_apertura_cierre_1a;
  const supervivencia = zona?.tasa_supervivencia_3a;
  const negocios = zona?.negocios_historico_count;

  const computed = useMemo(() => {
    if (!serie || serie.length === 0) return null;
    const max = Math.max(
      2,
      ...serie.map((s) => Math.max(s.aperturas, s.cierres)),
    );
    const xAt = (i: number) =>
      PAD_L + (i / Math.max(1, serie.length - 1)) * INNER_W;
    const yAt = (v: number) => PAD_T + (1 - v / max) * INNER_H;

    const path = (key: "aperturas" | "cierres") =>
      serie
        .map((s, i) => `${i === 0 ? "M" : "L"}${xAt(i)},${yAt(s[key])}`)
        .join(" ");

    const pathA = path("aperturas");
    const pathC = path("cierres");
    const areaA = `${pathA} L${xAt(serie.length - 1)},${PAD_T + INNER_H} L${xAt(0)},${PAD_T + INNER_H} Z`;

    return { max, xAt, yAt, pathA, pathC, areaA };
  }, [serie]);

  const totalAper = serie.reduce((acc, s) => acc + s.aperturas, 0);
  const totalCier = serie.reduce((acc, s) => acc + s.cierres, 0);
  const ratio24m = totalCier > 0 ? totalAper / totalCier : null;

  const firstMes = serie[0]?.mes;
  const lastMes = serie[serie.length - 1]?.mes;

  return (
    <div className={styles.fig}>
      <div className={styles.figHead}>
        <div className={styles.figTitle}>
          <strong>Fig.</strong> Aperturas vs cierres
        </div>
        <div className={styles.figSub}>
          {computed
            ? `24 meses · ${firstMes ? mesLabel(firstMes) : "—"} → ${lastMes ? mesLabel(lastMes) : "—"}`
            : "sin licencias detalladas en el periodo · KPIs agregados disponibles"}
        </div>
      </div>

      {computed ? (
      <>
      <div className={styles.trend}>
        <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
          <defs>
            <linearGradient id="dinGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--green)" stopOpacity="0.30" />
              <stop offset="100%" stopColor="var(--green)" stopOpacity="0" />
            </linearGradient>
          </defs>

          {/* Gridlines horizontales */}
          {[0, 0.25, 0.5, 0.75, 1].map((p) => (
            <line
              key={p}
              x1={PAD_L}
              x2={W - PAD_R}
              y1={PAD_T + p * INNER_H}
              y2={PAD_T + p * INNER_H}
              stroke="rgba(255,255,255,0.05)"
              strokeDasharray="2,3"
            />
          ))}

          {/* Área aperturas */}
          <path d={computed.areaA} fill="url(#dinGrad)" />

          {/* Línea cierres (punteada roja) */}
          <path
            d={computed.pathC}
            fill="none"
            stroke="var(--red)"
            strokeWidth="1.5"
            strokeDasharray="3,3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Línea aperturas (sólida verde) */}
          <path
            d={computed.pathA}
            fill="none"
            stroke="var(--green)"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />

          {/* Puntos sobre aperturas */}
          {serie.map((s, i) => (
            <circle
              key={`a${i}`}
              cx={computed.xAt(i)}
              cy={computed.yAt(s.aperturas)}
              r="2.6"
              fill="var(--green)"
            />
          ))}

          {/* Eje X — etiquetas extremas */}
          <line
            x1={PAD_L}
            x2={W - PAD_R}
            y1={PAD_T + INNER_H}
            y2={PAD_T + INNER_H}
            stroke="rgba(255,255,255,0.18)"
            strokeWidth="1"
          />
          <text
            x={PAD_L}
            y={H - 8}
            fontSize="9.5"
            fontFamily="var(--font-mono)"
            fill="var(--text-subtle)"
          >
            M-23
          </text>
          <text
            x={W - PAD_R}
            y={H - 8}
            fontSize="9.5"
            fontFamily="var(--font-mono)"
            textAnchor="end"
            fill="var(--text-subtle)"
          >
            M-0
          </text>

          {/* Eje Y — escala */}
          <text
            x={PAD_L - 6}
            y={PAD_T + 4}
            fontSize="9.5"
            fontFamily="var(--font-mono)"
            textAnchor="end"
            fill="var(--text-subtle)"
          >
            {computed.max}
          </text>
          <text
            x={PAD_L - 6}
            y={PAD_T + INNER_H + 3}
            fontSize="9.5"
            fontFamily="var(--font-mono)"
            textAnchor="end"
            fill="var(--text-subtle)"
          >
            0
          </text>
        </svg>
      </div>

      <div className={styles.legend}>
        <span><i className={styles.dotAper} />Aperturas · {totalAper}</span>
        <span><i className={styles.dotCierre} />Cierres · {totalCier}</span>
      </div>
      </>
      ) : (
        <div className={styles.noSerieBlock}>
          <p className={styles.noSerieText}>
            La tabla <code>licencias_actividad</code> está vacía en esta base de datos.
            Mostramos los KPIs agregados que vienen de <code>dinamismo_zonal</code> (pipeline
            mensual): ratio de apertura/cierre, supervivencia a 3 años y tendencia.
          </p>
        </div>
      )}

      {/* KPIs adicionales que SÍ tenemos en el response actual */}
      <div className={styles.kpiRow}>
        {ratio24m != null && (
          <KPI
            label="Ratio 24 m"
            value={`${ratio24m.toFixed(1)}×`}
            tone={ratio24m >= 1.3 ? "green" : ratio24m >= 0.9 ? "yellow" : "red"}
            hint="aperturas / cierres"
          />
        )}
        {ratio != null && (
          <KPI
            label="Ratio 12 m"
            value={`${ratio.toFixed(1)}×`}
            tone={ratio >= 1.3 ? "green" : ratio >= 0.9 ? "yellow" : "red"}
            hint="dinamismo_zonal"
          />
        )}
        {supervivencia != null && (
          <KPI
            label="Supervivencia 3a"
            value={`${Math.round(supervivencia * 100)}%`}
            tone={supervivencia >= 0.65 ? "green" : supervivencia >= 0.5 ? "yellow" : "red"}
            hint="negocios que sobreviven"
          />
        )}
        {negocios != null && (
          <KPI
            label="Histórico"
            value={String(negocios)}
            tone="accent"
            hint="negocios analizados"
          />
        )}
      </div>

      {tendencia && (
        <div className={`${styles.trendChip} ${styles[`trend_${tendenciaTone(tendencia)}`]}`}>
          {tendenciaArrow(tendencia)} {tendenciaLabel(tendencia)}
        </div>
      )}
    </div>
  );
}

function KPI({
  label,
  value,
  tone,
  hint,
}: {
  label: string;
  value: string;
  tone: "red" | "yellow" | "green" | "accent";
  hint: string;
}) {
  return (
    <div className={`${styles.kpi} ${styles[`kpi_${tone}`]}`}>
      <span className={styles.kpiLabel}>{label}</span>
      <span className={styles.kpiValue}>{value}</span>
      <span className={styles.kpiHint}>{hint}</span>
    </div>
  );
}

function tendenciaTone(t: string): "green" | "yellow" | "red" {
  if (t === "emergente") return "green";
  if (t === "estable") return "yellow";
  return "red";
}

function tendenciaArrow(t: string): string {
  if (t === "emergente") return "↑";
  if (t === "estable") return "→";
  if (t === "saturado") return "⚠";
  return "↓";
}

function tendenciaLabel(t: string): string {
  if (t === "emergente") return "Zona emergente";
  if (t === "estable") return "Zona estable";
  if (t === "saturado") return "Mercado saturado";
  if (t === "declive") return "Tendencia bajista";
  return t;
}
