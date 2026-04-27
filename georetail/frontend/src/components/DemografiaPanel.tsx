"use client";

import styles from "./DemografiaPanel.module.css";

interface DemografiaPanelProps {
  renta_media_hogar?: number | null;
  edad_media?: number | null;
  pct_extranjeros?: number | null;
  nivel_estudios_alto_pct?: number | null;
  delta_renta_3a?: number | null;
  indice_potencial_consumo?: number | null;
  explicacion_bullets?: string[];
}

const BCN_RENTA_REFERENCIA = 35000;

function formatEuros(valor: number): string {
  return valor.toLocaleString("es-ES", { maximumFractionDigits: 0 }) + " €/año";
}

function formatPct(valor: number, decimals = 1): string {
  return (
    valor.toLocaleString("es-ES", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }) + "%"
  );
}

function formatEdad(valor: number): string {
  return Math.round(valor).toLocaleString("es-ES") + " años";
}

function chipRenta(renta: number): { label: string; variant: "green" | "red" | "neutral" } {
  if (renta >= BCN_RENTA_REFERENCIA) {
    return { label: "Por encima de la media BCN", variant: "green" };
  }
  return { label: "Por debajo de la media BCN", variant: "red" };
}

function chipEdad(edad: number): { label: string; variant: "green" | "amber" | "neutral" } {
  if (edad < 35) return { label: "Zona joven", variant: "green" };
  if (edad <= 50) return { label: "Adulto activo", variant: "neutral" };
  return { label: "Zona madura", variant: "amber" };
}

function chipExtranjeros(pct: number): { label: string; variant: "neutral" } {
  if (pct < 15) return { label: "Barrio tradicional", variant: "neutral" };
  if (pct <= 30) return { label: "Barrio diverso", variant: "neutral" };
  return { label: "Alta internacionalización", variant: "neutral" };
}

function chipEstudios(pct: number): { label: string; variant: "green" | "amber" | "neutral" } {
  if (pct < 30) return { label: "Nivel bajo", variant: "amber" };
  if (pct <= 50) return { label: "Nivel medio", variant: "neutral" };
  return { label: "Nivel alto", variant: "green" };
}

function generarLecturaInterpretativa(props: DemografiaPanelProps): string {
  const { renta_media_hogar, edad_media, pct_extranjeros, nivel_estudios_alto_pct } = props;

  const hayDatos =
    renta_media_hogar != null ||
    edad_media != null ||
    pct_extranjeros != null ||
    nivel_estudios_alto_pct != null;

  if (!hayDatos) {
    return "Sin datos demográficos disponibles.";
  }

  if (
    renta_media_hogar != null &&
    renta_media_hogar > 40000 &&
    nivel_estudios_alto_pct != null &&
    nivel_estudios_alto_pct > 45
  ) {
    return "Zona de renta alta y perfil cualificado: favorable para ticket medio-alto.";
  }

  if (
    edad_media != null &&
    edad_media > 50 &&
    pct_extranjeros != null &&
    pct_extranjeros < 10
  ) {
    return "Barrio maduro y consolidado: clientela fiel pero con menor impulsividad de compra.";
  }

  if (
    pct_extranjeros != null &&
    pct_extranjeros > 30 &&
    renta_media_hogar != null &&
    renta_media_hogar >= BCN_RENTA_REFERENCIA
  ) {
    return "Alta presencia internacional en un entorno de renta media: clientela diversa con poder adquisitivo sostenido.";
  }

  if (
    edad_media != null &&
    edad_media < 35 &&
    nivel_estudios_alto_pct != null &&
    nivel_estudios_alto_pct > 40
  ) {
    return "Zona joven y con alto nivel formativo: perfil de consumo orientado a tendencias y experiencias.";
  }

  if (renta_media_hogar != null && renta_media_hogar < 25000) {
    return "Zona con renta por debajo de la media: mayor sensibilidad al precio, favorable para propuestas de valor accesible.";
  }

  return "Perfil demográfico mixto: revisa los indicadores individuales para afinar la estrategia comercial.";
}

function Chip({
  label,
  variant,
}: {
  label: string;
  variant: "green" | "amber" | "red" | "neutral";
}) {
  const cls =
    variant === "green"
      ? styles.chipGreen
      : variant === "amber"
      ? styles.chipAmber
      : variant === "red"
      ? styles.chipRed
      : styles.chipNeutral;

  return <span className={`${styles.chip} ${cls}`}>{label}</span>;
}

export default function DemografiaPanel({
  renta_media_hogar,
  edad_media,
  pct_extranjeros,
  nivel_estudios_alto_pct,
  delta_renta_3a,
  indice_potencial_consumo,
  explicacion_bullets,
}: DemografiaPanelProps) {
  const lecturaInterpretativa = generarLecturaInterpretativa({
    renta_media_hogar,
    edad_media,
    pct_extranjeros,
    nivel_estudios_alto_pct,
    delta_renta_3a,
    indice_potencial_consumo,
  });

  // Income hero: position as % of a scale from 0 to 60k (cap)
  const RENTA_SCALE_MAX = 60000;
  const rentaBarPct =
    renta_media_hogar != null
      ? Math.min(100, Math.round((renta_media_hogar / RENTA_SCALE_MAX) * 100))
      : 0;
  const bcnRefPct = Math.round((BCN_RENTA_REFERENCIA / RENTA_SCALE_MAX) * 100);
  const rentaAboveBcn =
    renta_media_hogar != null && renta_media_hogar >= BCN_RENTA_REFERENCIA;

  const consumoPct =
    indice_potencial_consumo != null
      ? Math.min(100, Math.max(0, indice_potencial_consumo))
      : 0;

  return (
    <div className={styles.panel}>

      {/* ── Income hero ── */}
      {renta_media_hogar != null && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Renta media del hogar</p>
          <div
            className={styles.heroNum}
            data-color={rentaAboveBcn ? "green" : "red"}
          >
            {formatEuros(renta_media_hogar)}
          </div>

          <div className={styles.refBarWrap}>
            <div className={styles.refBarTrack}>
              <div
                className={styles.refBarFill}
                data-color={rentaAboveBcn ? "green" : "red"}
                style={{ width: `${rentaBarPct}%` }}
              />
              <div
                className={styles.refBarMarker}
                style={{ left: `${bcnRefPct}%` }}
              />
            </div>
            <div className={styles.refBarLabels}>
              <span className={styles.refBarLabelLeft}>0 €</span>
              <span
                className={styles.refBarLabelCenter}
                style={{ left: `${bcnRefPct}%` }}
              >
                BCN media: 35.000 €
              </span>
            </div>
          </div>

          <Chip
            label={chipRenta(renta_media_hogar).label}
            variant={chipRenta(renta_media_hogar).variant}
          />
        </section>
      )}

      {/* ── 4-metric grid ── */}
      {(edad_media != null ||
        pct_extranjeros != null ||
        nivel_estudios_alto_pct != null ||
        delta_renta_3a != null) && (
        <div className={styles.statGrid}>
          {edad_media != null && (() => {
            const chip = chipEdad(edad_media);
            return (
              <div className={styles.statCard}>
                <div className={styles.statValue}>{formatEdad(edad_media)}</div>
                <div className={styles.statLabel}>Edad media</div>
                <Chip label={chip.label} variant={chip.variant} />
              </div>
            );
          })()}

          {pct_extranjeros != null && (() => {
            const chip = chipExtranjeros(pct_extranjeros);
            return (
              <div className={styles.statCard}>
                <div className={styles.statValue}>{formatPct(pct_extranjeros)}</div>
                <div className={styles.statLabel}>Población extranjera</div>
                <Chip label={chip.label} variant={chip.variant} />
              </div>
            );
          })()}

          {nivel_estudios_alto_pct != null && (() => {
            const chip = chipEstudios(nivel_estudios_alto_pct);
            return (
              <div className={styles.statCard}>
                <div className={styles.statValue}>{formatPct(nivel_estudios_alto_pct, 0)}</div>
                <div className={styles.statLabel}>Estudios superiores</div>
                <Chip label={chip.label} variant={chip.variant} />
              </div>
            );
          })()}

          {delta_renta_3a != null && (
            <div className={styles.statCard}>
              <div className={styles.statValue}>
                {delta_renta_3a >= 0 ? "+" : ""}
                {formatPct(delta_renta_3a)}
              </div>
              <div className={styles.statLabel}>Evolución renta (3a)</div>
              <Chip
                label={delta_renta_3a >= 0 ? "Tendencia positiva" : "Tendencia negativa"}
                variant={delta_renta_3a >= 0 ? "green" : "red"}
              />
            </div>
          )}
        </div>
      )}

      {/* ── Consumption index bar ── */}
      {indice_potencial_consumo != null && (
        <section className={styles.section}>
          <div className={styles.potencialHeader}>
            <p className={styles.sectionTitle}>Índice de potencial de consumo</p>
            <span className={styles.potencialScore}>
              {Math.round(indice_potencial_consumo)}
              <span className={styles.potencialMax}>/100</span>
            </span>
          </div>
          <div className={styles.potencialTrack}>
            <div
              className={styles.potencialFill}
              style={{ width: `${consumoPct}%` }}
            />
          </div>
        </section>
      )}

      {/* ── Interpretive summary ── */}
      <div className={styles.interpretacion}>
        <div className={styles.interpKicker}>Lectura interpretativa</div>
        <p className={styles.interpTexto}>{lecturaInterpretativa}</p>
      </div>

      {/* ── Signal bullets ── */}
      {explicacion_bullets && explicacion_bullets.length > 0 && (
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
