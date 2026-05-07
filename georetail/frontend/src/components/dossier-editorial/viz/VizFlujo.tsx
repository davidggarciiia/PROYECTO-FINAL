"use client";

import { useMemo } from "react";
import type { ZonaDetalle } from "@/lib/types";
import { formatPeatones } from "../helpers";
import styles from "./VizFlujo.module.css";

interface Props {
  zona: ZonaDetalle | null;
}

const BCN_P50 = 31000;

/* ── Reloj geometría ────────────────────────────────────────────── */
const CX = 100;
const CY = 100;
const R_OUT = 88;
const R_IN = 48;

/**
 * Deriva una distribución de 24 valores horarios (0..1) a partir de los
 * campos agregados que ya están en vz_flujo:
 *   - flujo_peatonal_manana / tarde / noche  (8-14h, 14-21h, 21-24h+0-8h)
 *   - late_night_share                       (% 22-2h)
 *   - weekday_midday_share                   (% 12-14h L-V)
 *
 * No es un Popular Times real, pero sí un perfil coherente con la zona:
 * picos al inicio de cada franja (mañana 9h, tarde 18h, noche 22h) con
 * caídas suaves entre franjas. El propio nivel medio respeta los volúmenes
 * declarados. Cuando el backend persista `popular_times_dist_horaria` en
 * vz_flujo, se sustituye este derivado por el array crudo.
 */
function derive24h(zona: ZonaDetalle | null): number[] {
  if (!zona) return new Array(24).fill(0.2);

  const fd = zona.flujo_peatonal_dia ?? null;
  const manana = fd?.manana ?? 0;
  const tarde = fd?.tarde ?? 0;
  const noche = fd?.noche ?? 0;
  const total = manana + tarde + noche;

  if (total <= 0) {
    // Fallback simétrico
    return new Array(24).fill(0.4);
  }

  // Pesos relativos por franja (normalizados a 1)
  const wManana = manana / total;
  const wTarde = tarde / total;
  const wNoche = noche / total;

  // Picos típicos en cada franja
  const peakManana = 9; // 9:00
  const peakTarde = 18; // 18:30 → redondeo a 18
  const peakNoche = 22; // 22:00

  // Anchos (sigma) de la campana en cada franja
  const sigmaManana = 2.5;
  const sigmaTarde = 3.0;
  const sigmaNoche = 2.0;

  function gauss(x: number, mu: number, sigma: number): number {
    return Math.exp(-Math.pow(x - mu, 2) / (2 * sigma * sigma));
  }

  // Construimos la curva sumando 3 gaussianas escaladas por su peso de franja
  const raw: number[] = [];
  for (let h = 0; h < 24; h++) {
    // Para la noche: el pico cruza medianoche → consideramos también h+24
    const gN = Math.max(gauss(h, peakNoche, sigmaNoche), gauss(h + 24, peakNoche + 4, sigmaNoche));
    const gM = gauss(h, peakManana, sigmaManana);
    const gT = gauss(h, peakTarde, sigmaTarde);
    raw.push(gM * wManana + gT * wTarde + gN * wNoche * 0.7);
  }

  // Normalizar a 0..1
  const max = Math.max(...raw);
  if (max <= 0) return raw;
  return raw.map((v) => v / max);
}

export default function VizFlujo({ zona }: Props) {
  const horas = useMemo(() => derive24h(zona), [zona]);
  const peatonesDia = zona?.vcity_flujo_peatonal ?? null;
  const fd = zona?.flujo_peatonal_dia ?? null;
  const totalFranjas = (fd?.manana ?? 0) + (fd?.tarde ?? 0) + (fd?.noche ?? 0);

  const franjasMeta = useMemo(() => {
    if (!fd || totalFranjas <= 0) return [] as { key: string; icon: string; label: string; pct: number; isMax: boolean }[];
    const data = [
      { key: "manana", icon: "M", label: "Mañana", val: fd.manana },
      { key: "tarde", icon: "T", label: "Tarde", val: fd.tarde },
      { key: "noche", icon: "N", label: "Noche", val: fd.noche },
    ];
    const max = Math.max(...data.map((d) => d.val));
    return data.map((d) => ({
      ...d,
      pct: Math.round((d.val / totalFranjas) * 100),
      isMax: d.val === max,
    }));
  }, [fd, totalFranjas]);

  const peakHour = horas.indexOf(Math.max(...horas));

  // Segmentos del reloj — mismo método que editorial.jsx VizFlujo.
  const segments = horas.map((v, i) => {
    const a0 = (i / 24) * Math.PI * 2 - Math.PI / 2;
    const a1 = ((i + 1) / 24) * Math.PI * 2 - Math.PI / 2;
    const rOut = R_IN + (R_OUT - R_IN) * v;
    const x0 = CX + Math.cos(a0) * R_IN;
    const y0 = CY + Math.sin(a0) * R_IN;
    const x1 = CX + Math.cos(a1) * R_IN;
    const y1 = CY + Math.sin(a1) * R_IN;
    const x2 = CX + Math.cos(a1) * rOut;
    const y2 = CY + Math.sin(a1) * rOut;
    const x3 = CX + Math.cos(a0) * rOut;
    const y3 = CY + Math.sin(a0) * rOut;
    const opacity = 0.20 + v * 0.65;
    const isPeak = i === peakHour;
    const fill = isPeak ? "var(--accent-light)" : "var(--accent)";
    return (
      <path
        key={i}
        d={`M${x0},${y0} L${x3},${y3} A${rOut},${rOut} 0 0 1 ${x2},${y2} L${x1},${y1} A${R_IN},${R_IN} 0 0 0 ${x0},${y0} Z`}
        fill={fill}
        opacity={opacity}
      />
    );
  });

  // Etiquetas horarias (cada 6h)
  const hourLabels = [0, 6, 12, 18].map((h) => {
    const a = (h / 24) * Math.PI * 2 - Math.PI / 2;
    const x = CX + Math.cos(a) * (R_OUT + 12);
    const y = CY + Math.sin(a) * (R_OUT + 12);
    return (
      <text
        key={h}
        x={x}
        y={y + 3}
        fontSize="9"
        fontFamily="var(--font-mono)"
        textAnchor="middle"
        fill="var(--text-subtle)"
        letterSpacing="0.10em"
      >
        {h.toString().padStart(2, "0")}h
      </text>
    );
  });

  if (!zona || (peatonesDia == null && totalFranjas <= 0)) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. TRÁFICO 24h</span>
        <p>Sin datos de flujo peatonal para esta zona.</p>
      </div>
    );
  }

  const pctP50 = peatonesDia != null ? Math.min((peatonesDia / BCN_P50) * 100, 130) : null;
  const heroTone =
    peatonesDia == null
      ? "yellow"
      : peatonesDia >= BCN_P50 * 1.05
        ? "green"
        : peatonesDia >= BCN_P50 * 0.8
          ? "yellow"
          : "red";
  const dominant = franjasMeta.find((f) => f.isMax);
  const p50Text = peatonesDia == null
    ? "sin comparación de volumen"
    : peatonesDia >= BCN_P50 * 1.05
      ? "por encima de la mediana de Barcelona"
      : peatonesDia >= BCN_P50 * 0.8
        ? "cerca de la mediana de Barcelona"
        : "por debajo de la mediana de Barcelona";
  const quickRead = dominant
    ? `La mayor captación cae en ${dominant.label.toLowerCase()} (${dominant.pct}%). El pico estimado está alrededor de las ${peakHour.toString().padStart(2, "0")}:00.`
    : `El pico estimado está alrededor de las ${peakHour.toString().padStart(2, "0")}:00.`;

  return (
    <div className={styles.fig}>
      <div className={styles.figHead}>
        <div className={styles.figTitle}>
          <strong>Fig.</strong> Tráfico 24h
        </div>
        <div className={styles.figSub}>L–V · perfil diario</div>
      </div>

      <div className={styles.clock}>
        <svg viewBox="0 0 200 200">
          <circle cx={CX} cy={CY} r={R_IN} fill="none"
                  stroke="rgba(255,255,255,0.08)" strokeDasharray="2,3" />
          <circle cx={CX} cy={CY} r={R_OUT} fill="none"
                  stroke="rgba(255,255,255,0.06)" strokeDasharray="2,3" />
          {segments}
          {hourLabels}
        </svg>
        <div className={styles.clockCenter}>
          {peatonesDia != null && (
            <>
              <span className={styles.clockBig}>{formatPeatones(peatonesDia)}</span>
              <span className={styles.clockUnit}>peat / día</span>
            </>
          )}
          {peatonesDia != null && pctP50 != null && (
            <span className={`${styles.clockDelta} ${styles[`clockDelta_${heroTone}`]}`}>
              {peatonesDia >= BCN_P50 ? "+" : ""}
              {Math.round(((peatonesDia - BCN_P50) / BCN_P50) * 100)}% vs BCN p50
            </span>
          )}
        </div>
      </div>

      <div className={styles.sidePanel}>
        <div className={styles.reading}>
          <span>lectura rápida</span>
          <p>{quickRead}</p>
          <div>
            {peatonesDia != null && <strong>{formatPeatones(peatonesDia)} peat/día</strong>}
            <small>{p50Text}</small>
          </div>
        </div>

        {franjasMeta.length > 0 && (
          <div className={styles.franjas}>
            {franjasMeta.map((f) => (
              <div
                key={f.key}
                className={`${styles.franja} ${f.isMax ? styles.franjaMax : ""}`}
              >
                <span className={styles.franjaIcon}>{f.icon}</span>
                <span className={styles.franjaLabel}>{f.label}</span>
                <span className={styles.franjaPct}>{f.pct}%</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className={styles.foot}>
        <span>
          Pico {peakHour.toString().padStart(2, "0")}:00
        </span>
        <span>lectura por franjas</span>
      </div>
    </div>
  );
}
