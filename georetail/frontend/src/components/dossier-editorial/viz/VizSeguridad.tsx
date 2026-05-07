"use client";

import { useMemo } from "react";
import type {
  ZonaDetalle,
  SeguridadDetalle,
  IncidentSerieMes,
  IncidentCategoriaTotal,
} from "@/lib/types";
import styles from "./VizSeguridad.module.css";

interface Props {
  zona: ZonaDetalle | null;
}

const DIAS = ["L", "M", "X", "J", "V", "S", "D"];

const BCN_REF = {
  hurtos: 12,
  robos: 4,
  danys: 2,
};

// ── Categorías que dibujamos en el trend chart ─────────────────────────────
// Total + las 3 más relevantes para retail. El resto se agrega visualmente
// como total. Colores neutros (sin emojis) ya en tokens.css.
const TREND_CATEGORIAS: ReadonlyArray<{
  key: keyof Omit<IncidentSerieMes, "mes" | "total">;
  label: string;
  color: string;
}> = [
  { key: "seguretat",   label: "Seguridad",   color: "rgba(239,68,68,1)" }, // var(--red)
  { key: "transit",     label: "Tráfico/movilidad", color: "rgba(245,158,11,1)" }, // var(--yellow)
  { key: "convivencia", label: "Convivencia vecinal", color: "rgba(59,130,246,1)" }, // azul
];

const CAT_COLORS: Record<IncidentCategoriaTotal["categoria"], string> = {
  incendis:    "rgba(239,68,68,1)",
  convivencia: "rgba(59,130,246,1)",
  transit:     "rgba(245,158,11,1)",
  seguretat:   "rgba(220,38,38,1)",
  serveis:     "rgba(99,102,241,1)",
  altres:      "rgba(156,163,175,1)",
};

const CAT_LABELS: Record<IncidentCategoriaTotal["categoria"], string> = {
  incendis: "Emergencias",
  convivencia: "Convivencia vecinal",
  transit: "Tráfico/movilidad",
  seguretat: "Seguridad",
  serveis: "Servicios urbanos",
  altres: "Otros avisos",
};

const CAT_HINTS: Record<IncidentCategoriaTotal["categoria"], string> = {
  incendis: "incendios y emergencias",
  convivencia: "ruido, molestias, locales y uso del espacio público",
  transit: "tráfico, circulación y movilidad",
  seguretat: "hurtos, robos y seguridad ciudadana",
  serveis: "avisos municipales y soporte urbano",
  altres: "resto de actuaciones",
};

// ── Heatmap derivado (FALLBACK si no hay serie_12m real) ───────────────────
// Mantiene el comportamiento legacy de la viz cuando el pipeline
// `incidents_gub` aún no se ha ejecutado.
function deriveHeatmap(
  detalle: SeguridadDetalle | null | undefined,
): number[][] {
  const incidencias = detalle?.incidencias_por_1000hab ?? 0;
  const nochePct = detalle?.incidencias_noche_pct ?? 30;

  const base = Math.min(1, incidencias / 60);
  const nocheRatio = Math.min(1, Math.max(0, nochePct / 100));

  function intensity(d: number, h: number): number {
    let v = base * 0.18;
    if (h >= 17 && h <= 21) v += base * 0.20;
    if (h >= 22 || h <= 2) v += base * 0.50 * nocheRatio;
    if (h === 0 || h === 1) v += base * 0.18 * nocheRatio;
    if (h >= 3 && h <= 6) v += base * 0.12 * nocheRatio;
    if (h >= 9 && h <= 15) v *= 0.7;
    if (d >= 5) v *= 1.4;
    return Math.min(1, v);
  }

  const matrix: number[][] = [];
  for (let d = 0; d < 7; d++) {
    const row: number[] = [];
    for (let h = 0; h < 24; h++) {
      row.push(intensity(d, h));
    }
    matrix.push(row);
  }
  return matrix;
}

function fmt1(n: number): string {
  return n.toLocaleString("es-ES", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
}

function fmtInt(n: number): string {
  return n.toLocaleString("es-ES", { maximumFractionDigits: 0 });
}

function shortMes(yyyymm: string): string {
  // 'YYYY-MM' → 'MMM' en castellano abreviado.
  const parts = yyyymm.split("-");
  const m = parseInt(parts[1] ?? "0", 10);
  const NOMS = [
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
  ];
  return NOMS[m - 1] ?? "";
}

interface IncDef {
  label: string;
  value: number;
  ref?: number;
}

// ── Trend chart 12 meses (datos reales) ─────────────────────────────────────
// SVG puro, sin librerías externas (alineado con el resto del editorial).

function TrendChart({ serie }: { serie: IncidentSerieMes[] }) {
  const W = 580;            // viewBox width — el SVG escala via width:100%
  const H = 160;
  const padL = 32, padR = 14, padT = 10, padB = 22;
  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  // Buscamos el máximo en cualquiera de las series + total para fijar el eje Y.
  const maxY = useMemo(() => {
    let m = 0;
    for (const entry of serie) {
      m = Math.max(m, entry.total);
      for (const cat of TREND_CATEGORIAS) {
        m = Math.max(m, entry[cat.key]);
      }
    }
    return Math.max(m, 1);
  }, [serie]);

  const yTicks = useMemo(() => {
    // 4 líneas de grid → max, 75%, 50%, 25%, 0
    return [0, 0.25, 0.5, 0.75, 1].map((p) => ({
      value: Math.round(maxY * p),
      y: padT + innerH - innerH * p,
    }));
  }, [maxY, innerH]);

  const xStep = serie.length > 1 ? innerW / (serie.length - 1) : innerW;

  function pointFor(value: number, idx: number): [number, number] {
    const x = padL + idx * xStep;
    const y = padT + innerH - (value / maxY) * innerH;
    return [x, y];
  }

  function pathFor(getValue: (e: IncidentSerieMes) => number): string {
    if (serie.length === 0) return "";
    const pts = serie.map((entry, i) => pointFor(getValue(entry), i));
    return pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  }

  function areaFor(getValue: (e: IncidentSerieMes) => number): string {
    if (serie.length === 0) return "";
    const pts = serie.map((entry, i) => pointFor(getValue(entry), i));
    const first = pts[0];
    const last = pts[pts.length - 1];
    const baseY = padT + innerH;
    const top = pts.map(([x, y]) => `L${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
    return `M${first[0].toFixed(1)},${baseY} ${top} L${last[0].toFixed(1)},${baseY} Z`;
  }

  return (
    <svg
      className={styles.trendSvg}
      viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      role="img"
      aria-label="Serie 12 meses incidencias por categoría"
    >
      {/* Grid + ticks */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line
            x1={padL}
            x2={W - padR}
            y1={t.y}
            y2={t.y}
            className={styles.trendGridLine}
          />
          <text
            x={padL - 4}
            y={t.y + 3}
            textAnchor="end"
            className={styles.trendAxisLabel}
          >
            {fmtInt(t.value)}
          </text>
        </g>
      ))}

      {/* Área del total (suave, atrás) */}
      <path
        d={areaFor((e) => e.total)}
        className={styles.trendArea}
        fill="rgba(148,163,184,0.55)"
      />

      {/* Línea total — gris/neutro */}
      <path
        d={pathFor((e) => e.total)}
        className={styles.trendLine}
        stroke="rgba(148,163,184,0.85)"
      />

      {/* Líneas categorías destacadas */}
      {TREND_CATEGORIAS.map((cat) => (
        <path
          key={cat.key}
          d={pathFor((e) => e[cat.key])}
          className={styles.trendLine}
          stroke={cat.color}
        />
      ))}

      {/* Etiquetas de mes en eje X (1 cada 2 para no saturar) */}
      {serie.map((entry, i) => {
        if (i % 2 !== 0 && i !== serie.length - 1) return null;
        const x = padL + i * xStep;
        return (
          <text
            key={entry.mes}
            x={x}
            y={H - 6}
            textAnchor="middle"
            className={styles.trendAxisLabel}
          >
            {shortMes(entry.mes)}
          </text>
        );
      })}
    </svg>
  );
}

export default function VizSeguridad({ zona }: Props) {
  const detalle =
    (zona?.seguridad_detalle as SeguridadDetalle | null | undefined) ?? null;
  const serie12m = detalle?.serie_12m ?? null;
  const topCats = detalle?.top_categorias ?? null;
  const tieneSerieReal = Array.isArray(serie12m) && serie12m.length > 0;
  const tieneCatsReales = Array.isArray(topCats) && topCats.length > 0;

  // Heatmap derivado solo si NO tenemos la serie real (graceful fallback).
  const heatmap = useMemo(
    () => (tieneSerieReal ? null : deriveHeatmap(detalle)),
    [detalle, tieneSerieReal],
  );

  if (!detalle) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. SEGURIDAD · 12M</span>
        <p>Sin datos de seguridad para esta zona.</p>
      </div>
    );
  }

  // ── Barras hurtos/robos/daños — siempre disponibles si hay desglose vz_entorno
  const breakdown: IncDef[] = [
    { label: "Hurtos", value: detalle.hurtos_por_1000hab ?? 0, ref: BCN_REF.hurtos },
    { label: "Robos",  value: detalle.robatoris_por_1000hab ?? 0, ref: BCN_REF.robos },
    { label: "Daños",  value: detalle.danys_por_1000hab ?? 0,    ref: BCN_REF.danys },
  ];
  const totalIncidencias = detalle.incidencias_por_1000hab ?? 0;
  const hasBreakdown = breakdown.some((d) => d.value > 0);
  const incDefs: IncDef[] = hasBreakdown
    ? breakdown
    : totalIncidencias > 0
      ? [{ label: "Incidencias totales", value: totalIncidencias }]
      : [];
  const maxScale = Math.max(
    ...incDefs.map((d) => Math.max(d.value, d.ref ?? 0)),
    0.01,
  ) * 1.15;

  const score = detalle.seguridad_barri_score ?? null;
  const nochePct = detalle.incidencias_noche_pct ?? null;
  const diaPct = nochePct != null ? 100 - nochePct : null;

  // Total acumulado del trend para el sub-título.
  const totalTrend = tieneSerieReal
    ? serie12m!.reduce((acc, e) => acc + e.total, 0)
    : 0;

  return (
    <div className={styles.fig}>
      <div className={styles.figHead}>
        <div className={styles.figTitle}>
          <strong>Fig.</strong>{" "}
          {tieneSerieReal ? "Serie 12 meses · Guardia Urbana" : "Incidencias semana × hora"}
        </div>
        <div className={styles.figSub}>
          {tieneSerieReal
            ? `${fmtInt(totalTrend)} actuaciones`
            : "último año"}
        </div>
      </div>

      {/* ── TOP: serie 12 meses real / heatmap derivado fallback ── */}
      {tieneSerieReal ? (
        <div className={styles.trendWrap}>
          <TrendChart serie={serie12m!} />
          <div className={styles.trendLegend}>
            <span className={styles.trendLegendItem}>
              <span
                className={styles.trendLegendDot}
                style={{ background: "rgba(148,163,184,0.85)" }}
              />
              Total
            </span>
            {TREND_CATEGORIAS.map((cat) => (
              <span key={cat.key} className={styles.trendLegendItem}>
                <span
                  className={styles.trendLegendDot}
                  style={{ background: cat.color }}
                />
                {cat.label}
              </span>
            ))}
          </div>
        </div>
      ) : (
        <div className={styles.heatWrap}>
          <div className={styles.heatHourLabels}>
            {[0, 6, 12, 18].map((h) => (
              <div key={h} className={styles.heatHourLabel}>
                {h.toString().padStart(2, "0")}h
              </div>
            ))}
          </div>
          <div className={styles.heat}>
            {(heatmap ?? []).map((row, di) => (
              <div key={di} className={styles.heatRow}>
                <span className={styles.heatDay}>{DIAS[di]}</span>
                {row.map((v, hi) => (
                  <div
                    key={hi}
                    className={styles.heatCell}
                    style={{ background: `rgba(239, 68, 68, ${0.06 + v * 0.85})` }}
                    title={`${DIAS[di]} ${hi.toString().padStart(2, "0")}:00 — ${Math.round(v * 100)}%`}
                  />
                ))}
              </div>
            ))}
          </div>
          <div className={styles.heatLegend}>
            <span>Bajo</span>
            <div
              className={styles.heatLegendBar}
              style={{
                background:
                  "linear-gradient(90deg, rgba(239,68,68,0.06), rgba(239,68,68,0.92))",
              }}
            />
            <span>Alto</span>
          </div>
        </div>
      )}

      {/* ── MEDIO: barras hurtos/robos/daños vs ref BCN (siempre si hay datos) */}
      {incDefs.length > 0 && (
      <>
      <div className={styles.incList}>
        {incDefs.map((bar) => {
          const fillPct = (bar.value / maxScale) * 100;
          const refPct = bar.ref != null ? (bar.ref / maxScale) * 100 : null;
          const above = bar.ref != null && bar.value > bar.ref;
          const wellAbove = bar.ref != null && bar.value > bar.ref * 1.5;
          const color = bar.ref == null
            ? "var(--accent-light)"
            : !above ? "var(--green)" : wellAbove ? "var(--red)" : "var(--yellow)";
          return (
            <div key={bar.label} className={styles.incRow}>
              <div className={styles.incLabel}>{bar.label}</div>
              <div className={styles.incTrack}>
                <div
                  className={styles.incFill}
                  style={{ width: `${fillPct}%`, background: color }}
                />
                {refPct != null && (
                  <div
                    className={styles.incRefLine}
                    style={{ left: `${refPct}%` }}
                    title={`Ref. BCN ${fmt1(bar.ref ?? 0)}`}
                  />
                )}
              </div>
              <div className={styles.incValue}>{fmt1(bar.value)}</div>
              <div className={styles.incRef}>
                {bar.ref != null ? `ref ${fmt1(bar.ref)}` : "total"}
              </div>
            </div>
          );
        })}
        {!hasBreakdown && totalIncidencias > 0 && (
          <div className={styles.incNote}>
            El desglose por tipo no está disponible para esta zona; se muestra el total registrado.
          </div>
        )}
      </div>

      {/* ── INFERIOR: distribución por categoría (real) ────────────────── */}
      </>
      )}

      {tieneCatsReales ? (
        <div className={styles.catList}>
          {topCats!.map((cat) => {
            const pct = Math.max(0, Math.min(100, cat.pct));
            return (
              <div key={cat.categoria} className={styles.catRow}>
                <div className={styles.catLabel}>
                  <span>{CAT_LABELS[cat.categoria] ?? cat.categoria}</span>
                  <small>{CAT_HINTS[cat.categoria]}</small>
                </div>
                <div className={styles.catTrack}>
                  <div
                    className={styles.catFill}
                    style={{
                      width: `${pct}%`,
                      background: CAT_COLORS[cat.categoria] ?? "var(--accent-light)",
                    }}
                  />
                </div>
                <div className={styles.catValue}>{fmtInt(cat.n_total)}</div>
                <div className={styles.catPct}>{cat.pct.toFixed(1)}%</div>
              </div>
            );
          })}
        </div>
      ) : (
        // Fallback: día/noche split del legacy
        nochePct != null && diaPct != null && (
          <div className={styles.splitBlock}>
            <div className={styles.splitLabel}>Distribución día / noche</div>
            <div className={styles.splitTrack}>
              <div
                className={styles.splitDia}
                style={{ width: `${diaPct}%` }}
                title={`Día ${Math.round(diaPct)}%`}
              >
                {diaPct >= 18 && <span>Día {Math.round(diaPct)}%</span>}
              </div>
              <div
                className={styles.splitNoche}
                style={{ width: `${nochePct}%` }}
                title={`Noche ${Math.round(nochePct)}%`}
              >
                {nochePct >= 18 && <span>Noche {Math.round(nochePct)}%</span>}
              </div>
            </div>
            {nochePct > 30 && (
              <span className={styles.splitWarn}>Alta actividad nocturna</span>
            )}
          </div>
        )
      )}

      {/* KPI row: score + comisarías */}
      <div className={styles.kpiRow}>
        {score != null && (
          <div className={styles.kpi}>
            <span className={styles.kpiLabel}>Score barrio</span>
            <span
              className={`${styles.kpiValue} ${score >= 70 ? styles.kpiGreen : score >= 50 ? styles.kpiYellow : styles.kpiRed}`}
            >
              {Math.round(score)}
            </span>
            <span className={styles.kpiHint}>/ 100</span>
          </div>
        )}
        {detalle.comisarias_1km != null && (
          <div className={styles.kpi}>
            <span className={styles.kpiLabel}>Comisarías 1 km</span>
            <span className={styles.kpiValue}>{detalle.comisarias_1km}</span>
            <span className={styles.kpiHint}>presencia</span>
          </div>
        )}
        {detalle.dist_comisaria_m != null && (
          <div className={styles.kpi}>
            <span className={styles.kpiLabel}>Distancia comisaría</span>
            <span className={styles.kpiValue}>
              {Math.round(detalle.dist_comisaria_m)} m
            </span>
            <span className={styles.kpiHint}>más cercana</span>
          </div>
        )}
        {detalle.incidencias_por_1000hab != null && detalle.incidencias_por_1000hab > 0 && (
          <div className={styles.kpi}>
            <span className={styles.kpiLabel}>Incidencias / 1000 hab</span>
            <span className={styles.kpiValue}>
              {fmt1(detalle.incidencias_por_1000hab)}
            </span>
            <span className={styles.kpiHint}>total año</span>
          </div>
        )}
      </div>

      <div className={styles.foot}>
        <span>{tieneSerieReal ? "fuente · GU Barcelona" : "lectura operativa"}</span>
        <span>{tieneSerieReal ? "incidents-gestionats-gub" : "radio 200 m"}</span>
      </div>
    </div>
  );
}
