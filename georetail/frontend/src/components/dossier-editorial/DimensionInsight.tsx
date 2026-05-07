"use client";

import type { CSSProperties } from "react";
import { type Band, type DimensionMeta, bandLabel } from "./dimensions";
import styles from "./DimSection.module.css";

type CSSVars = CSSProperties & Record<`--${string}`, string | number>;

interface DimensionInsightProps {
  dim: DimensionMeta;
  band: Band;
  score?: number | null;
  scoreClamped: number;
  pesoPct: number | null;
  hechos: string[];
  positivos: string[];
  negativos: string[];
  titular: string;
  explicacionCorta: string;
  implicaciones: string[];
  showActions?: boolean;
}

function vars(values: Record<`--${string}`, string | number>): CSSVars {
  return values as CSSVars;
}

function clamp(value: number, min = 0, max = 100): number {
  return Math.max(min, Math.min(max, value));
}

function displayScore(score?: number | null, scoreClamped = 0): string {
  return score != null ? `${scoreClamped}` : "Sin dato";
}

function normalizeText(text: string): string {
  return text
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isSameIdea(a: string, b: string): boolean {
  const left = normalizeText(a);
  const right = normalizeText(b);
  if (!left || !right) return false;
  if (left === right) return true;
  if (left.length > 36 && right.includes(left)) return true;
  if (right.length > 36 && left.includes(right)) return true;
  return false;
}

function cleanList(items: string[], exclude: string[] = [], limit = 4): string[] {
  const accepted: string[] = [];
  const blocked = exclude.filter(Boolean);

  for (const raw of items) {
    const item = raw.trim();
    if (!item) continue;
    const duplicated = [...blocked, ...accepted].some((prev) => isSameIdea(prev, item));
    if (duplicated) continue;
    accepted.push(item);
    if (accepted.length >= limit) break;
  }

  return accepted;
}

function firstLower(items: string[]): string {
  const text = items[0]?.replace(/\.$/, "").trim();
  if (!text) return "";
  return text.charAt(0).toLowerCase() + text.slice(1);
}

function buildInterpretacion({
  dim,
  band,
  score,
  datos,
  positivos,
  negativos,
}: {
  dim: DimensionMeta;
  band: Band;
  score?: number | null;
  datos: string[];
  positivos: string[];
  negativos: string[];
}): string {
  if (score == null) {
    return `Lectura: faltan datos suficientes para cerrar ${dim.name.toLowerCase()}; úsalo solo como contexto.`;
  }

  const apoyo = firstLower(positivos) || firstLower(datos);
  const freno = firstLower(negativos) || firstLower(datos);

  if (band === "hi") {
    return apoyo
      ? `Lectura: esta dimensión acompaña la apertura; apóyate en ${apoyo}.`
      : "Lectura: esta dimensión juega a favor de la zona.";
  }

  if (band === "lo") {
    return freno
      ? `Lectura: aquí está el punto a vigilar; revisa ${freno}.`
      : "Lectura: esta dimensión pide cautela antes de decidir.";
  }

  return freno
    ? `Lectura: la zona es viable, pero conviene comprobar ${freno}.`
    : "Lectura: la dimensión es aceptable, sin una señal extrema.";
}

function MetricDial({
  label,
  value,
  pct,
  tone,
}: {
  label: string;
  value: string;
  pct: number;
  tone?: Band;
}) {
  return (
    <div className={`${styles.metricDial} ${tone ? styles[`metric_${tone}`] : ""}`}>
      <span style={vars({ "--pct": `${clamp(pct)}%` })} />
      <strong>{value}</strong>
      <small>{label}</small>
    </div>
  );
}

function DatosImportantes({ items }: { items: string[] }) {
  if (items.length === 0) return null;

  return (
    <div className={styles.evidenceBoard}>
      <div className={styles.panelTitle}>
        <span>datos importantes</span>
        <strong>{items.length} pistas</strong>
      </div>
      <div className={styles.evidenceGrid}>
        {items.map((fact, i) => (
          <article key={`${fact}-${i}`} className={styles.evidenceItem}>
            <strong>{String(i + 1).padStart(2, "0")}</strong>
            <p>{fact}</p>
            <span aria-hidden="true" />
          </article>
        ))}
      </div>
    </div>
  );
}

function LecturaPractica({
  positivos,
  negativos,
}: {
  positivos: string[];
  negativos: string[];
}) {
  if (positivos.length === 0 && negativos.length === 0) return null;

  return (
    <div className={styles.forceMap}>
      <div className={styles.panelTitle}>
        <span>lectura práctica</span>
        <strong>qué ayuda y qué vigilar</strong>
      </div>
      <div className={`${styles.forceColumns} ${(positivos.length === 0 || negativos.length === 0) ? styles.forceColumns_single : ""}`}>
        {positivos.length > 0 && (
          <div className={`${styles.forceColumn} ${styles.forceUp}`}>
            <span className={styles.forceMark}>+</span>
            <strong className={styles.forceLabel}>A favor</strong>
            {positivos.map((item, i) => (
              <p key={`${item}-${i}`}>{item}</p>
            ))}
          </div>
        )}
        {negativos.length > 0 && (
          <div className={`${styles.forceColumn} ${styles.forceDown}`}>
            <span className={styles.forceMark}>-</span>
            <strong className={styles.forceLabel}>A revisar</strong>
            {negativos.map((item, i) => (
              <p key={`${item}-${i}`}>{item}</p>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function Decisiones({ implicaciones }: { implicaciones: string[] }) {
  if (implicaciones.length === 0) return null;

  return (
    <div className={styles.actionMap}>
      <div className={styles.panelTitle}>
        <span>siguiente lectura</span>
        <strong>decisiones prácticas</strong>
      </div>
      <ol>
        {implicaciones.slice(0, 3).map((item, i) => (
          <li key={`${item}-${i}`}>
            <span>{String(i + 1).padStart(2, "0")}</span>
            <p>{item}</p>
          </li>
        ))}
      </ol>
    </div>
  );
}

export default function DimensionInsight({
  dim,
  band,
  score,
  scoreClamped,
  pesoPct,
  hechos,
  positivos,
  negativos,
  titular,
  explicacionCorta,
  implicaciones,
  showActions = false,
}: DimensionInsightProps) {
  const baseText = [titular, explicacionCorta, dim.subtitle, dim.what].filter(Boolean);
  const datos = cleanList(hechos, baseText, 3);
  const aFavor = cleanList(positivos, [...baseText, ...datos], 3);
  const aRevisar = cleanList(negativos, [...baseText, ...datos, ...aFavor], 3);
  const interpretacion = buildInterpretacion({
    dim,
    band,
    score,
    datos,
    positivos: aFavor,
    negativos: aRevisar,
  });

  return (
    <section
      className={`${styles.insight} ${styles[`insight_${dim.key}`]} ${styles[`insight_${band}`]}`}
      aria-label={`Lectura de ${dim.name}`}
    >
      <div className={styles.decisionPanel}>
        <div className={styles.decisionCopy}>
          <span>qué significa</span>
          <h3>{titular}</h3>
          <p>{explicacionCorta}</p>
          <p className={styles.interpretation}>{interpretacion}</p>
        </div>
        <div className={styles.metricRow}>
          <MetricDial label={bandLabel(band)} value={displayScore(score, scoreClamped)} pct={score != null ? scoreClamped : 0} tone={band} />
          <MetricDial label="peso en el índice" value={pesoPct != null ? `${pesoPct}%` : "Sin dato"} pct={pesoPct ?? 0} />
        </div>
      </div>

      <DatosImportantes items={datos} />
      <LecturaPractica positivos={aFavor} negativos={aRevisar} />
      {showActions && implicaciones.length > 0 && <Decisiones implicaciones={implicaciones} />}
    </section>
  );
}
