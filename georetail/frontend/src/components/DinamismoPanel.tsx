"use client";

import styles from "./DinamismoPanel.module.css";

interface DinamismoPanelProps {
  tendencia?: string | null;
  tasa_supervivencia_3a?: number | null;
  ratio_apertura_cierre_1a?: number | null;
  hhi_sectorial?: number | null;
  negocios_historico_count?: number | null;
  renta_variacion_3a?: number | null;
  score?: number | null;
  explicacion_bullets?: string[];
  /** New: raw counts of openings (last 12 months) */
  licencias_abiertas_1a?: number | null;
  /** New: raw counts of closings (last 12 months) */
  licencias_cerradas_1a?: number | null;
  /** New: vacancy ratio (0-1) of empty commercial premises */
  vacantes_ratio?: number | null;
  /** New: 3y population variation as decimal fraction (0.04 = 4%) */
  poblacion_variacion_3a?: number | null;
  /** New: 1y commercial-rent variation as decimal fraction */
  precio_alquiler_variacion_1a?: number | null;
}

/* ── Tendencia mapping (backend → UI) ── */
type TendenciaKey = "emergente" | "estable" | "declinante" | "en_riesgo";

const TENDENCIA_BACKEND_MAP: Record<string, TendenciaKey> = {
  emergente: "emergente",
  estable:   "estable",
  saturado:  "en_riesgo",
  declive:   "declinante",
  sin_datos: "estable",
};

const TENDENCIA_META: Record<
  TendenciaKey,
  { arrow: string; label: string; color: "green" | "yellow" | "red" }
> = {
  emergente:  { arrow: "↑", label: "Zona emergente",   color: "green"  },
  estable:    { arrow: "→", label: "Zona estable",     color: "yellow" },
  declinante: { arrow: "↓", label: "Tendencia bajista", color: "red"   },
  en_riesgo:  { arrow: "⚠", label: "Mercado saturado", color: "red"    },
};

/* ── HHI label aligned with backend thresholds ── */
function hhiLabel(
  hhi: number,
): { label: string; state: "fragmented" | "moderate" | "concentrated" } {
  if (hhi < 0.20) return { label: "Fragmentado",  state: "fragmented" };
  if (hhi < 0.50) return { label: "Moderado",     state: "moderate" };
  return               { label: "Concentrado",  state: "concentrated" };
}

/* ── Vacantes color tier ── */
function vacantesColor(ratio: number): "green" | "yellow" | "red" {
  if (ratio < 0.08) return "green";
  if (ratio <= 0.15) return "yellow";
  return "red";
}

/* ── Confianza tier (matches backend) ── */
function confianzaInfo(
  count: number,
): { label: string; tier: "alta" | "media" | "baja" } {
  if (count >= 20) return { label: "Confianza alta",  tier: "alta"  };
  if (count >= 10) return { label: "Confianza media", tier: "media" };
  return               { label: "Confianza baja",  tier: "baja"  };
}

function fmtSignedPct(value: number): string {
  const sign = value >= 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(1)}%`;
}

export default function DinamismoPanel({
  tendencia,
  tasa_supervivencia_3a,
  ratio_apertura_cierre_1a,
  hhi_sectorial,
  negocios_historico_count,
  renta_variacion_3a,
  explicacion_bullets,
  licencias_abiertas_1a,
  licencias_cerradas_1a,
  vacantes_ratio,
  poblacion_variacion_3a,
  precio_alquiler_variacion_1a,
}: DinamismoPanelProps) {
  const hasTendencia = tendencia != null;
  const hasSupervivencia = tasa_supervivencia_3a != null;
  const hasRatio = ratio_apertura_cierre_1a != null;
  const hasHHI = hhi_sectorial != null;
  const hasBullets =
    Array.isArray(explicacion_bullets) && explicacion_bullets.length > 0;
  const hasCount = negocios_historico_count != null;
  const hasVacantes = vacantes_ratio != null;
  const hasRenta = renta_variacion_3a != null;
  const hasPoblacion = poblacion_variacion_3a != null;
  const hasAperturas = licencias_abiertas_1a != null;
  const hasCierres = licencias_cerradas_1a != null;
  const hasTension = precio_alquiler_variacion_1a != null;

  const hasAnyData =
    hasTendencia ||
    hasSupervivencia ||
    hasRatio ||
    hasHHI ||
    hasRenta ||
    hasPoblacion ||
    hasVacantes ||
    hasTension ||
    hasBullets;

  if (!hasAnyData) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Datos de dinamismo en proceso de cálculo para esta zona.
        </div>
      </div>
    );
  }

  /* ── Tendencia meta ── */
  const tendenciaUI: TendenciaKey | null = tendencia
    ? TENDENCIA_BACKEND_MAP[tendencia] ?? null
    : null;
  const tMeta = tendenciaUI ? TENDENCIA_META[tendenciaUI] : null;

  /* ── Supervivencia gauge ── */
  const survPct = hasSupervivencia
    ? Math.round((tasa_supervivencia_3a as number) * 100)
    : null;
  const survDeg = survPct != null ? Math.round((survPct / 100) * 360) : 0;

  /* ── Ratio apertura/cierre ── */
  let apertPct = 50;
  let cierrePct = 50;
  if (hasRatio && ratio_apertura_cierre_1a != null) {
    const r = ratio_apertura_cierre_1a;
    if (r >= 1) {
      apertPct = Math.min(Math.round((r / (r + 1)) * 100), 85);
      cierrePct = 100 - apertPct;
    } else {
      cierrePct = Math.min(Math.round((1 / (r + 1)) * 100), 85);
      apertPct = 100 - cierrePct;
    }
  }

  /* ── HHI ── */
  const hhiInfo = hasHHI ? hhiLabel(hhi_sectorial as number) : null;

  /* ── Sample size hint ── */
  const sampleCount = (negocios_historico_count as number | null) ?? 0;
  const sampleReducida = hasCount && sampleCount < 5;

  /* ── Vacantes ── */
  // The bar visually maxes out at 30% vacancy (0-30% range).
  const vacantesPct =
    hasVacantes && vacantes_ratio != null
      ? Math.min((vacantes_ratio * 100) / 30 * 100, 100)
      : 0;
  const vacantesTier =
    hasVacantes && vacantes_ratio != null
      ? vacantesColor(vacantes_ratio)
      : "green";
  const vacantesAlerta =
    hasVacantes && vacantes_ratio != null && vacantes_ratio > 0.15;
  const vacantesSubtext =
    hasVacantes && vacantes_ratio != null
      ? vacantes_ratio < 0.08
        ? "Señal de vitalidad"
        : vacantes_ratio <= 0.15
        ? "Estancamiento"
        : "Declive"
      : "";

  /* ── Confianza chip ── */
  const confianza = hasCount ? confianzaInfo(sampleCount) : null;

  /* ── Tension inmobiliaria ── */
  const tensionPositive =
    hasTension &&
    precio_alquiler_variacion_1a != null &&
    precio_alquiler_variacion_1a >= 0;
  const tensionAlerta =
    hasTension &&
    precio_alquiler_variacion_1a != null &&
    precio_alquiler_variacion_1a > 0.10;

  /* ── Apertura/cierre raw counts presentation ── */
  const useRawCounts = hasAperturas;
  const ratioValue = ratio_apertura_cierre_1a ?? null;

  return (
    <div className={styles.panel}>

      {/* ── Sección 1: Tendencia hero ── */}
      {hasTendencia && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Tendencia de la zona</div>
          <div className={styles.trendHero} data-color={tMeta?.color ?? "muted"}>
            <span className={styles.trendArrow}>{tMeta?.arrow ?? "–"}</span>
            <div className={styles.trendInfo}>
              <span className={styles.trendLabel}>{tMeta?.label ?? "Sin datos"}</span>
              <span className={styles.trendSub}>Dinámica comercial histórica</span>
              {hasCount && (
                sampleReducida ? (
                  <span className={`${styles.trendSampleChip} ${styles.chipYellow}`}>
                    Muestra reducida
                  </span>
                ) : (
                  <span className={styles.trendSample}>
                    Basado en {sampleCount} negocios históricos
                  </span>
                )
              )}
            </div>
          </div>
        </section>
      )}

      {/* ── Sección 2: Confianza footer chip (placed before bullets per spec) ── */}
      {confianza && (
        <div className={styles.confianzaWrap}>
          <span
            className={`${styles.confianzaChip} ${
              confianza.tier === "alta"
                ? styles.chipGreen
                : confianza.tier === "media"
                ? styles.chipYellow
                : styles.chipNeutral
            }`}
          >
            {confianza.label}
          </span>
        </div>
      )}

      {/* ── Sección 3: Survival rate gauge ── */}
      {hasSupervivencia && survPct != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Supervivencia 3 años</div>
          <div className={styles.gaugeRow}>
            <div className={styles.gaugeWrap}>
              <div
                className={styles.gaugeRing}
                role="img"
                aria-label={`Tasa de supervivencia: ${survPct}%`}
                style={{
                  background: `conic-gradient(
                    var(--green) 0deg ${survDeg}deg,
                    var(--surface-3) ${survDeg}deg 360deg
                  )`,
                }}
              >
                <div className={styles.gaugeInner}>
                  <span className={styles.gaugeValue}>{survPct}%</span>
                </div>
              </div>
            </div>
            <div className={styles.gaugeDesc}>
              <span className={styles.gaugeDescTitle}>
                {survPct >= 70
                  ? "Alta supervivencia"
                  : survPct >= 50
                  ? "Supervivencia media"
                  : "Supervivencia baja"}
              </span>
              <span className={styles.gaugeDescSub}>
                {survPct >= 70
                  ? "La mayoría de negocios aguantan los primeros 3 años en esta zona."
                  : survPct >= 50
                  ? "Alrededor de la mitad supera los 3 años de actividad."
                  : "El entorno es exigente — menos de la mitad sobrevive 3 años."}
              </span>
            </div>
          </div>
        </section>
      )}

      {/* ── Sección 4: Aperturas vs cierres ── */}
      {hasRatio && ratio_apertura_cierre_1a != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Aperturas vs. cierres (último año)</div>
          <div className={styles.ratioBar}>
            <div
              className={styles.ratioSegmentGreen}
              style={{ width: `${apertPct}%` }}
            />
            <div
              className={styles.ratioSegmentRed}
              style={{ width: `${cierrePct}%` }}
            />
          </div>

          {useRawCounts ? (
            <>
              <div className={styles.aperturaCierreRow}>
                <span className={styles.apertCount}>
                  <span className={styles.countNum}>{licencias_abiertas_1a}</span>
                  <span className={styles.countLabel}>↑ aperturas</span>
                </span>
                <span className={styles.cierreCount}>
                  <span className={styles.countLabel}>↓ cierres</span>
                  <span className={styles.countNum}>{licencias_cerradas_1a ?? 0}</span>
                </span>
              </div>
              {ratioValue != null && (
                <div className={styles.ratioChipWrap}>
                  <span
                    className={`${styles.ratioChip} ${
                      ratioValue >= 1 ? styles.chipGreen : styles.chipRed
                    }`}
                  >
                    Ratio &times;{ratioValue.toFixed(1)}
                  </span>
                </div>
              )}
            </>
          ) : (
            <div className={styles.ratioLegend}>
              <span className={styles.ratioLegendItem} data-color="green">
                <span className={styles.ratioLegendDot} />
                Aperturas
              </span>
              <span
                className={styles.ratioBadge}
                data-positive={ratio_apertura_cierre_1a >= 1}
              >
                &times;{ratio_apertura_cierre_1a.toFixed(1)}
              </span>
              <span className={styles.ratioLegendItem} data-color="red">
                Cierres
                <span className={styles.ratioLegendDot} />
              </span>
            </div>
          )}
        </section>
      )}

      {/* ── Sección 5: Locales vacíos ── */}
      {hasVacantes && vacantes_ratio != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Locales vacíos en la zona</div>
          <div className={styles.vacantesHeader}>
            <span
              className={styles.vacantesBig}
              data-color={vacantesTier}
            >
              {(vacantes_ratio * 100).toFixed(1)}%
            </span>
            <span className={styles.vacantesSub}>{vacantesSubtext}</span>
          </div>
          <div
            className={styles.vacantesBarTrack}
            role="img"
            aria-label={`${(vacantes_ratio * 100).toFixed(1)}% de locales vacíos`}
          >
            <div
              className={styles.vacantesBarFill}
              data-color={vacantesTier}
              style={{ width: `${vacantesPct}%` }}
            />
          </div>
          {vacantesAlerta && (
            <span className={`${styles.vacantesAlert} ${styles.chipRed}`}>
              ⚠ Riesgo de declive
            </span>
          )}
        </section>
      )}

      {/* ── Sección 6: Concentración de mercado (HHI) ── */}
      {hasHHI && hhiInfo && hhi_sectorial != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Concentración de mercado (HHI)</div>
          <div className={styles.hhiRow}>
            <div className={styles.hhiSegments}>
              <div
                className={`${styles.hhiSeg} ${
                  hhiInfo.state === "fragmented" ? styles.hhiSegActive : ""
                }`}
                data-label="Fragmentado"
              />
              <div
                className={`${styles.hhiSeg} ${
                  hhiInfo.state === "moderate" ? styles.hhiSegActive : ""
                }`}
                data-label="Moderado"
              />
              <div
                className={`${styles.hhiSeg} ${
                  hhiInfo.state === "concentrated" ? styles.hhiSegActive : ""
                }`}
                data-label="Concentrado"
              />
            </div>
            <span className={styles.hhiLabel}>{hhiInfo.label}</span>
          </div>
          <p className={styles.hhiNote}>
            {hhiInfo.state === "fragmented"
              ? "Mercado muy diversificado — ningún actor domina el sector en esta zona."
              : hhiInfo.state === "moderate"
              ? "Concentración moderada — hay líderes pero con espacio para competidores."
              : "Mercado concentrado — pocos actores dominan. Entrada más difícil."}
          </p>
          <p className={styles.hhiHint}>
            HHI = {hhi_sectorial.toFixed(2)} (1.0 = monopolio)
          </p>
        </section>
      )}

      {/* ── Sección 7: Variación demográfica (renta + población) ── */}
      {(hasRenta || hasPoblacion) && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Variación demográfica</div>
          <div
            className={
              hasPoblacion ? styles.demoGrid : styles.demoGridSingle
            }
          >
            {hasRenta && renta_variacion_3a != null && (
              <div className={styles.demoCell}>
                <span
                  className={styles.demoBig}
                  data-positive={renta_variacion_3a >= 0}
                >
                  {fmtSignedPct(renta_variacion_3a)}
                </span>
                <span className={styles.demoLabel}>Renta</span>
                <span className={styles.demoDesc}>vs. hace 3 años</span>
              </div>
            )}
            {hasPoblacion && poblacion_variacion_3a != null && (
              <div className={styles.demoCell}>
                <span
                  className={styles.demoBig}
                  data-positive={poblacion_variacion_3a >= 0}
                >
                  {fmtSignedPct(poblacion_variacion_3a)}
                </span>
                <span className={styles.demoLabel}>Población</span>
                <span className={styles.demoDesc}>vs. hace 3 años</span>
              </div>
            )}
          </div>
        </section>
      )}

      {/* ── Sección 8: Tensión inmobiliaria ── */}
      {hasTension && precio_alquiler_variacion_1a != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Tensión inmobiliaria</div>
          <div className={styles.tensionRow}>
            <span className={styles.tensionText}>
              Alquiler comercial{" "}
              <span
                className={styles.tensionVal}
                data-positive={tensionPositive}
              >
                {fmtSignedPct(precio_alquiler_variacion_1a)}
              </span>{" "}
              vs hace 1 año
            </span>
          </div>
          {tensionAlerta && (
            <span className={`${styles.tensionAlert} ${styles.chipRed}`}>
              ⚠ Gentrificación rápida
            </span>
          )}
        </section>
      )}

      {/* ── Sección 9: Señales detectadas ── */}
      {hasBullets && explicacion_bullets && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Señales detectadas</div>
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
