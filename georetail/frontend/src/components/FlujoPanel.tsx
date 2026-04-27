"use client";

import styles from "./FlujoPanel.module.css";

interface FlujoPanelProps {
  flujo_dia?: { manana: number; tarde: number; noche: number } | null;
  vcity_peatones_dia?: number | null;
  coefs_estacionales?: Record<string, number> | null;
  sector_codigo?: string;
  explicacion_bullets?: string[];
}

const MESES_ES = [
  "enero", "febrero", "marzo", "abril", "mayo", "junio",
  "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
];

const MESES_LABEL = [
  "Ene", "Feb", "Mar", "Abr", "May", "Jun",
  "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
];

const BCN_P50 = 31000;

function formatPeatones(n: number): string {
  if (n >= 1000) return `${Math.round((n / 1000) * 10) / 10}k`;
  return String(Math.round(n));
}

function capitalizeFirst(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

const FRANJA_META: Array<{ key: "manana" | "tarde" | "noche"; icon: string; label: string }> = [
  { key: "manana", icon: "☀", label: "Mañana" },
  { key: "tarde",  icon: "🌤", label: "Tarde"  },
  { key: "noche",  icon: "🌙", label: "Noche"  },
];

export default function FlujoPanel({
  flujo_dia,
  vcity_peatones_dia,
  coefs_estacionales,
  explicacion_bullets,
}: FlujoPanelProps) {
  const hasFlujoDia =
    flujo_dia != null &&
    (flujo_dia.manana > 0 || flujo_dia.tarde > 0 || flujo_dia.noche > 0);
  const hasVCity = vcity_peatones_dia != null && vcity_peatones_dia > 0;
  const hasEstacional =
    coefs_estacionales != null && Object.keys(coefs_estacionales).length > 0;
  const hasBullets =
    Array.isArray(explicacion_bullets) && explicacion_bullets.length > 0;

  const hasAnyData = hasFlujoDia || hasVCity || hasEstacional || hasBullets;

  if (!hasAnyData) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Sin datos de flujo disponibles para esta zona.
        </div>
      </div>
    );
  }

  /* ── Sección 1: Hero peatones ── */
  let pctOfP50 = 0;
  let heroColor: "green" | "yellow" | "red" = "yellow";
  if (hasVCity && vcity_peatones_dia != null) {
    pctOfP50 = Math.min((vcity_peatones_dia / BCN_P50) * 100, 130);
    if (vcity_peatones_dia >= BCN_P50 * 1.05) heroColor = "green";
    else if (vcity_peatones_dia >= BCN_P50 * 0.8) heroColor = "yellow";
    else heroColor = "red";
  }

  /* ── Sección 2: Perfil horario vertical ── */
  let franjas: Array<{
    key: "manana" | "tarde" | "noche";
    icon: string;
    label: string;
    value: number;
    pct: number;
    isMax: boolean;
  }> = [];
  if (hasFlujoDia && flujo_dia) {
    const total = flujo_dia.manana + flujo_dia.tarde + flujo_dia.noche || 1;
    const maxVal = Math.max(flujo_dia.manana, flujo_dia.tarde, flujo_dia.noche);
    franjas = FRANJA_META.map(({ key, icon, label }) => ({
      key,
      icon,
      label,
      value: flujo_dia[key],
      pct: Math.round((flujo_dia[key] / total) * 100),
      isMax: flujo_dia[key] === maxVal,
    }));
  }

  /* ── Sección 3: Estacionalidad ── */
  let estacionalData: Array<{
    mes: string; label: string; coef: number; pct: number; isCurrent: boolean;
  }> = [];
  let mesPico = "";
  let mesBajo = "";
  if (hasEstacional && coefs_estacionales) {
    const currentMonth = new Date().getMonth();
    let maxCoef = -Infinity;
    let minCoef = Infinity;
    let maxMes = "";
    let minMes = "";
    const coefValues: number[] = [];
    MESES_ES.forEach((mes) => {
      const coef = coefs_estacionales[mes] ?? 1;
      coefValues.push(coef);
      if (coef > maxCoef) { maxCoef = coef; maxMes = mes; }
      if (coef < minCoef) { minCoef = coef; minMes = mes; }
    });
    const maxCoefNorm = Math.max(...coefValues);
    const minCoefNorm = Math.min(...coefValues);
    const range = maxCoefNorm - minCoefNorm || 1;
    estacionalData = MESES_ES.map((mes, idx) => {
      const coef = coefs_estacionales[mes] ?? 1;
      return {
        mes,
        label: MESES_LABEL[idx],
        coef,
        pct: Math.round(((coef - minCoefNorm) / range) * 75 + 25),
        isCurrent: idx === currentMonth,
      };
    });
    mesPico = maxMes;
    mesBajo = minMes;
  }

  const p50BarPct = Math.min(100, Math.round((BCN_P50 / (BCN_P50 * 1.3)) * 100));

  return (
    <div className={styles.panel}>

      {/* ── Sección 1: Hero peatones/día ── */}
      {hasVCity && vcity_peatones_dia != null && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Peatones estimados / día</p>
          <div className={styles.heroNum} data-color={heroColor}>
            {formatPeatones(vcity_peatones_dia)}
            <span className={styles.heroUnit}>peat/día</span>
          </div>

          {/* Reference bar */}
          <div className={styles.refBarWrap}>
            <div className={styles.refBarTrack}>
              <div
                className={styles.refBarFill}
                data-color={heroColor}
                style={{ width: `${Math.min(pctOfP50, 100)}%` }}
              />
              {/* p50 marker */}
              <div
                className={styles.refBarMarker}
                style={{ left: `${p50BarPct}%` }}
              />
            </div>
            <div className={styles.refBarLabels}>
              <span className={styles.refBarLabelLeft}>0</span>
              <span className={styles.refBarLabelCenter} style={{ left: `${p50BarPct}%` }}>
                BCN p50: {formatPeatones(BCN_P50)}
              </span>
            </div>
          </div>

          <div className={`${styles.chip} ${heroColor === "green" ? styles.chipGreen : heroColor === "yellow" ? styles.chipYellow : styles.chipRed}`}>
            {heroColor === "green"
              ? "Por encima de la mediana BCN"
              : heroColor === "yellow"
              ? "Cerca de la mediana BCN"
              : "Por debajo de la mediana BCN"}
          </div>
        </section>
      )}

      {/* ── Sección 2: Perfil horario vertical ── */}
      {hasFlujoDia && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Perfil horario</p>
          <div className={styles.vertBars}>
            {franjas.map((f) => (
              <div key={f.key} className={styles.vertBarCol}>
                <span className={styles.vertBarPct}>{f.pct}%</span>
                <div className={styles.vertBarTrack}>
                  <div
                    className={`${styles.vertBarFill} ${f.isMax ? styles.vertBarFillAccent : styles.vertBarFillMuted}`}
                    style={{ height: `${f.pct}%` }}
                  />
                </div>
                <span className={styles.vertBarIcon}>{f.icon}</span>
                <span className={styles.vertBarLabel}>{f.label}</span>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* ── Sección 3: Estacionalidad anual ── */}
      {hasEstacional && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Estacionalidad anual</p>
          <div className={styles.estacionalGrid}>
            {estacionalData.map((item) => (
              <div key={item.mes} className={styles.estacionalCol}>
                <div className={styles.estacionalBarWrap}>
                  <div
                    className={`${styles.estacionalBar} ${item.isCurrent ? styles.estacionalBarCurrent : ""}`}
                    style={{ height: `${item.pct}%` }}
                    title={`${capitalizeFirst(item.mes)}: \xd7${item.coef.toFixed(2)}`}
                  />
                </div>
                <span className={`${styles.estacionalMesLabel} ${item.isCurrent ? styles.estacionalMesCurrent : ""}`}>
                  {item.label}
                </span>
              </div>
            ))}
          </div>
          {(mesPico || mesBajo) && (
            <div className={styles.estacionalChips}>
              {mesPico && (
                <span className={styles.chipPico}>
                  ▲ Pico: {capitalizeFirst(mesPico)}
                </span>
              )}
              {mesBajo && (
                <span className={styles.chipBaja}>
                  ▼ Baja: {capitalizeFirst(mesBajo)}
                </span>
              )}
            </div>
          )}
        </section>
      )}

      {/* ── Sección 4: Señales detectadas ── */}
      {hasBullets && explicacion_bullets && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Señales detectadas</p>
          <div className={styles.signalTags}>
            {explicacion_bullets.map((bullet, i) => (
              <span key={i} className={styles.signalTag}>
                {bullet}
              </span>
            ))}
          </div>
        </section>
      )}

    </div>
  );
}
