"use client";

import type { ZonaDetalle } from "@/lib/types";
import styles from "./VizDemografia.module.css";

interface Props {
  zona: ZonaDetalle | null;
}

type Signal = {
  label: string;
  value: string;
  hint: string;
  width: string;
};

function numberOrNull(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function pctUnit(value: number | null | undefined): number | null {
  const n = numberOrNull(value);
  if (n == null) return null;
  return n > 1 ? n / 100 : n;
}

function fmtPct(value: number | null | undefined, decimals = 0): string {
  const unit = pctUnit(value);
  if (unit == null) return "Sin dato";
  return `${(unit * 100).toLocaleString("es-ES", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })}%`;
}

function fmtEur(value: number | null | undefined): string {
  const n = numberOrNull(value);
  if (n == null) return "Sin dato";
  return `${Math.round(n).toLocaleString("es-ES")} €`;
}

function fmtNumber(value: number | null | undefined): string {
  const n = numberOrNull(value);
  if (n == null) return "Sin dato";
  return Math.round(n).toLocaleString("es-ES");
}

function barWidth(value: number | null | undefined, max: number): string {
  const n = numberOrNull(value);
  if (n == null || max <= 0) return "0%";
  return `${Math.max(3, Math.min(100, (n / max) * 100))}%`;
}

function pctWidth(value: number | null | undefined): string {
  const unit = pctUnit(value);
  if (unit == null) return "0%";
  return `${Math.max(3, Math.min(100, unit * 100))}%`;
}

function giniLabel(value: number | null | undefined): string {
  const n = numberOrNull(value);
  if (n == null) return "Sin dato";
  return (n > 1 ? n / 100 : n).toFixed(2);
}

function buildSummary(zona: ZonaDetalle): string {
  const bits: string[] = [];
  if (zona.edad_media != null) bits.push(`edad media ${Math.round(zona.edad_media)} años`);
  if (zona.renta_media_hogar != null) bits.push(`renta hogar ${fmtEur(zona.renta_media_hogar)}`);
  if (zona.pct_poblacio_25_44 != null) bits.push(`${fmtPct(zona.pct_poblacio_25_44)} entre 25 y 44`);
  if (bits.length === 0) return "No hay desglose demográfico suficiente para esta zona.";
  return bits.join(", ") + ".";
}

export default function VizDemografia({ zona }: Props) {
  if (!zona) {
    return (
      <div className={styles.empty}>
        <span className={styles.emptyEyebrow}>FIG. PERFIL DEMOGRAFICO</span>
        <p>Sin datos demográficos para esta zona.</p>
      </div>
    );
  }

  const poblacion = numberOrNull(zona.poblacion);
  const densidad = numberOrNull(zona.densidad_hab_km2);
  const renta = numberOrNull(zona.renta_media_hogar);
  const rentaUc = numberOrNull(zona.renta_media_uc);
  const rentaMedianaUc = numberOrNull(zona.renta_mediana_uc);
  const edad = numberOrNull(zona.edad_media);
  const pct2544 = pctUnit(zona.pct_poblacio_25_44);
  const pctExtranjeros = pctUnit(zona.pct_extranjeros);
  const pctUniv = pctUnit(zona.nivel_estudios_alto_pct);
  const deltaRenta = numberOrNull(zona.delta_renta_3a);
  const gini = numberOrNull(zona.gini);
  const p80p20 = numberOrNull(zona.p80_p20);
  const tamanoHogar = numberOrNull(zona.tamano_hogar);
  const hogaresMenores = pctUnit(zona.hogares_con_menores);
  const personasSolas = pctUnit(zona.personas_solas);
  const headline =
    poblacion != null
      ? { value: fmtNumber(poblacion), label: "residentes" }
      : renta != null
        ? { value: fmtEur(renta), label: "renta hogar" }
        : rentaUc != null
          ? { value: fmtEur(rentaUc), label: "renta ajustada" }
          : { value: "Dato parcial", label: "perfil incompleto" };

  const profileBars: Signal[] = [
    renta != null && {
      label: "Renta hogar",
      value: fmtEur(renta),
      hint: "capacidad de gasto familiar",
      width: barWidth(renta, 70000),
    },
    rentaUc != null && {
      label: "Renta por unidad",
      value: fmtEur(rentaUc),
      hint: "poder adquisitivo ajustado",
      width: barWidth(rentaUc, 40000),
    },
    edad != null && {
      label: "Edad media",
      value: `${Math.round(edad)} años`,
      hint: "madurez del barrio",
      width: barWidth(edad, 80),
    },
    pct2544 != null && {
      label: "25-44 años",
      value: fmtPct(pct2544),
      hint: "público adulto activo",
      width: pctWidth(pct2544),
    },
    pctUniv != null && {
      label: "Estudios altos",
      value: fmtPct(pctUniv),
      hint: "nivel formativo",
      width: pctWidth(pctUniv),
    },
    pctExtranjeros != null && {
      label: "Población extranjera",
      value: fmtPct(pctExtranjeros),
      hint: "diversidad residente",
      width: pctWidth(pctExtranjeros),
    },
  ].filter(Boolean) as Signal[];

  const extraKpis = [
    poblacion != null && { label: "Población", value: fmtNumber(poblacion), hint: "residentes", tone: "" },
    densidad != null && { label: "Densidad", value: fmtNumber(densidad), hint: "hab/km²", tone: "" },
    rentaMedianaUc != null && { label: "Renta mediana", value: fmtEur(rentaMedianaUc), hint: "unidad consumo", tone: "" },
    deltaRenta != null && {
      label: "Cambio renta 3a",
      value: `${deltaRenta > 0 ? "+" : ""}${deltaRenta.toFixed(1)}%`,
      hint: "evolución reciente",
      tone: deltaRenta > 0 ? styles.kpiUp : styles.kpiDown,
    },
    gini != null && { label: "Gini", value: giniLabel(gini), hint: "desigualdad interna", tone: "" },
    p80p20 != null && { label: "P80/P20", value: p80p20.toFixed(1), hint: "brecha de renta", tone: "" },
  ].filter(Boolean) as { label: string; value: string; hint: string; tone: string }[];

  const hogarItems = [
    tamanoHogar != null && {
      label: "Tamaño medio",
      value: `${tamanoHogar.toFixed(1)} pers/hog`,
      width: barWidth(tamanoHogar, 4),
    },
    hogaresMenores != null && {
      label: "Hogares con menores",
      value: fmtPct(hogaresMenores),
      width: pctWidth(hogaresMenores),
    },
    personasSolas != null && {
      label: "Personas solas",
      value: fmtPct(personasSolas),
      width: pctWidth(personasSolas),
    },
  ].filter(Boolean) as { label: string; value: string; width: string }[];

  return (
    <div className={styles.fig}>
      <div className={styles.figHead}>
        <div className={styles.figTitle}>
          <strong>Fig.</strong> Perfil demográfico
        </div>
        <div className={styles.figSub}>
          {headline.label}
        </div>
      </div>

      <div className={styles.profileGrid}>
        <div className={styles.profileHero}>
          <span>lectura rápida</span>
          <strong>{headline.value}</strong>
          <p>{buildSummary(zona)}</p>
        </div>

        <div className={styles.profileBars}>
          {profileBars.length > 0 ? (
            profileBars.map((item) => (
              <div key={item.label} className={styles.profileBar}>
                <div className={styles.profileBarHead}>
                  <span>{item.label}</span>
                  <strong>{item.value}</strong>
                </div>
                <div className={styles.profileTrack}>
                  <div className={styles.profileFill} style={{ width: item.width }} />
                </div>
                <p>{item.hint}</p>
              </div>
            ))
          ) : (
            <div className={styles.inlineEmpty}>
              No hay indicadores demográficos desglosados para esta zona.
            </div>
          )}
        </div>
      </div>

      {extraKpis.length > 0 && (
        <div className={styles.kpiRow}>
          {extraKpis.map((kpi) => (
            <div key={kpi.label} className={styles.kpi}>
              <span className={styles.kpiLabel}>{kpi.label}</span>
              <span className={`${styles.kpiValue} ${kpi.tone}`}>{kpi.value}</span>
              <span className={styles.kpiHint}>{kpi.hint}</span>
            </div>
          ))}
        </div>
      )}

      {hogarItems.length > 0 && (
        <div className={styles.hogarBlock}>
          <div className={styles.hogarTitle}>Composición del hogar</div>
          <div className={styles.hogarBars}>
            {hogarItems.map((item) => (
              <div key={item.label} className={styles.hogarBar}>
                <div className={styles.hogarLabel}>{item.label}</div>
                <div className={styles.hogarTrack}>
                  <div className={styles.hogarFill} style={{ width: item.width }} />
                </div>
                <div className={styles.hogarValue}>{item.value}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className={styles.foot}>
        <span>perfil residente</span>
        {densidad != null && <span>{fmtNumber(densidad)} hab/km²</span>}
      </div>
    </div>
  );
}
