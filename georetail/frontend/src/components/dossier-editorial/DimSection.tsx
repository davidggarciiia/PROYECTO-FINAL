"use client";

import type {
  ZonaDetalle,
  ExplicacionDimension,
} from "@/lib/types";
import {
  type DimensionMeta,
  type DimensionKey,
  scoreBand,
  bandLabel,
} from "./dimensions";
import { getImplicaciones } from "./implicaciones";
import { normalizeList, clampScore, formatDist } from "./helpers";

import VizFlujo from "./viz/VizFlujo";
import VizDemografia from "./viz/VizDemografia";
import VizSeguridad from "./viz/VizSeguridad";
import VizTurismo from "./viz/VizTurismo";
import VizDinamismo from "./viz/VizDinamismo";
import VizTransporte from "./viz/VizTransporte";
import DimensionInsight from "./DimensionInsight";
import NarrativaPanel from "./NarrativaPanel";

import styles from "./DimSection.module.css";

interface Props {
  /** Índice de sección (0-based para los §). */
  idx: number;
  dim: DimensionMeta;
  zona: ZonaDetalle | null;
  explicacion?: ExplicacionDimension;
  pesoBase?: number;
  pesoMod?: number;
  /** Necesario para algunas viz que hacen fetch (Turismo). */
  sessionId: string;
}

function asFiniteNumber(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function fmtInt(value: number): string {
  return Math.round(value).toLocaleString("es-ES");
}

function fmtMoney(value: number): string {
  return `${fmtInt(value)} €`;
}

function fmtPctValue(value: number): string {
  const normalized = value > 0 && value <= 1 ? value * 100 : value;
  return `${Math.round(normalized).toLocaleString("es-ES")}%`;
}

function addFact(
  facts: string[],
  label: string,
  value: number | null,
  format: (n: number) => string,
) {
  if (value == null) return;
  facts.push(`${label}: ${format(value)}.`);
}

function hechosDesdeZona(dimKey: DimensionKey, zona: ZonaDetalle | null): string[] {
  if (!zona) return [];
  const facts: string[] = [];

  if (dimKey === "flujo_peatonal") {
    addFact(facts, "Flujo medio diario", asFiniteNumber(zona.vcity_flujo_peatonal), fmtInt);
    const fd = zona.flujo_peatonal_dia;
    if (fd) {
      const total = fd.manana + fd.tarde + fd.noche;
      if (total > 0) {
        const franja = [
          ["mañana", fd.manana],
          ["tarde", fd.tarde],
          ["noche", fd.noche],
        ].sort((a, b) => Number(b[1]) - Number(a[1]))[0];
        facts.push(`Franja dominante: ${franja[0]} (${Math.round((Number(franja[1]) / total) * 100)}%).`);
      }
    }
  }

  if (dimKey === "demografia") {
    if (zona.barrio || zona.distrito) {
      facts.push(`Contexto: ${[zona.barrio, zona.distrito].filter(Boolean).join(", ")}.`);
    }
    addFact(facts, "Renta hogar", asFiniteNumber(zona.renta_media_hogar), fmtMoney);
    addFact(facts, "Renta por unidad de consumo", asFiniteNumber(zona.renta_media_uc), fmtMoney);
    addFact(facts, "Población residente", asFiniteNumber(zona.poblacion), fmtInt);
    addFact(facts, "Edad media", asFiniteNumber(zona.edad_media), (n) => `${Math.round(n)} años`);
    addFact(facts, "Población 25-44", asFiniteNumber(zona.pct_poblacio_25_44), fmtPctValue);
    addFact(facts, "Estudios altos", asFiniteNumber(zona.nivel_estudios_alto_pct), fmtPctValue);
  }

  if (dimKey === "transporte") {
    addFact(facts, "Líneas cercanas", asFiniteNumber(zona.num_lineas_transporte), fmtInt);
    addFact(facts, "Paradas cercanas", asFiniteNumber(zona.num_paradas_transporte), fmtInt);
    addFact(facts, "Bicing a 400 m", asFiniteNumber(zona.num_bicing_400m), fmtInt);
  }

  if (dimKey === "seguridad") {
    const detalle = zona.seguridad_detalle;
    addFact(facts, "Incidencias por 1.000 hab", asFiniteNumber(detalle?.incidencias_por_1000hab), (n) =>
      n.toLocaleString("es-ES", { maximumFractionDigits: 1 }),
    );
    addFact(facts, "Actividad nocturna", asFiniteNumber(detalle?.incidencias_noche_pct), fmtPctValue);
    addFact(facts, "Comisarías en 1 km", asFiniteNumber(detalle?.comisarias_1km), fmtInt);
  }

  if (dimKey === "turismo") {
    addFact(facts, "Airbnb en 500 m", asFiniteNumber(zona.airbnb_density_500m), fmtInt);
    addFact(facts, "Hoteles en 500 m", asFiniteNumber(zona.booking_hoteles_500m), fmtInt);
    addFact(facts, "Landmark top-3", asFiniteNumber(zona.dist_landmark_top3_m), formatDist);
    addFact(facts, "Playa", asFiniteNumber(zona.dist_playa_m), formatDist);
  }

  if (dimKey === "dinamismo") {
    addFact(facts, "Locales activos", asFiniteNumber(zona.num_negocios_activos), fmtInt);
    addFact(facts, "Locales vacíos", asFiniteNumber(zona.pct_locales_vacios), fmtPctValue);
    addFact(facts, "Supervivencia 3 años", asFiniteNumber(zona.tasa_supervivencia_3a), fmtPctValue);
    addFact(facts, "Aperturas/cierres", asFiniteNumber(zona.ratio_apertura_cierre_1a), (n) =>
      n.toLocaleString("es-ES", { maximumFractionDigits: 2 }),
    );
  }

  return facts;
}

/* ── Visual dispatcher: cada dim sabe qué panel del repo usar ──────── */

function VisualForDim({
  dim,
  zona,
  zonaId,
  sessionId,
}: {
  dim: DimensionMeta;
  zona: ZonaDetalle | null;
  zonaId: string;
  sessionId: string;
}) {
  if (!zona) {
    return <div className={styles.emptyVisual}>Sin datos para esta dimensión</div>;
  }

  // Nota: la dimensión "competencia" no se renderiza con DimSection — el
  // DossierEditorial intercepta y monta CompetenciaSection (panel grande
  // antiguo con Leaflet, lista LLM, etc.). Aquí solo cubrimos las otras 6.

  if (dim.key === "flujo_peatonal") return <VizFlujo zona={zona} />;
  if (dim.key === "demografia")     return <VizDemografia zona={zona} />;
  if (dim.key === "transporte")     return <VizTransporte zonaId={zonaId} />;
  if (dim.key === "seguridad")      return <VizSeguridad zona={zona} />;
  if (dim.key === "turismo")        return <VizTurismo zona={zona} sessionId={sessionId} />;
  if (dim.key === "dinamismo")      return <VizDinamismo zona={zona} />;

  return <div className={styles.emptyVisual}>—</div>;
}

/* ── DimSection ──────────────────────────────────────────────────── */

export default function DimSection({
  idx,
  dim,
  zona,
  explicacion,
  pesoBase,
  pesoMod,
  sessionId,
}: Props) {
  const score = zona?.scores_dimensiones?.[dim.key as DimensionKey];
  const band = scoreBand(score);
  const scoreClamped = clampScore(score);

  const hechos = [
    ...hechosDesdeZona(dim.key, zona),
    ...normalizeList(explicacion?.hechos_clave),
  ];
  const positivos = normalizeList(explicacion?.porque_sube);
  const negativos = normalizeList(explicacion?.porque_baja);

  const sectorCodigo = zona?.sector_codigo;
  const titular = explicacion?.titular?.trim() ||
    `${dim.name} · ${bandLabel(band)}`;
  const explicacionCorta = explicacion?.explicacion_corta?.trim() || dim.subtitle;
  const pesoActivo = pesoMod ?? pesoBase;
  const pesoPct = pesoActivo != null && pesoActivo > 0
    ? Math.round(pesoActivo * 100)
    : null;

  const implicaciones = getImplicaciones(dim.key, sectorCodigo);

  const sectionId = `sec-${dim.key}`;
  const num = String(idx + 2).padStart(2, "0"); // §02..§08 (lede es §I)

  return (
    <section
      id={sectionId}
      className={`${styles.section} ${styles[`section_${band}`]}`}
      aria-label={`Dimensión ${dim.name}`}
    >
      <header className={styles.head}>
        <div className={styles.headLeft}>
          <span className={styles.kicker}>§ {num}</span>
          <h2 className={styles.title}>
            {dim.name}
            <em className={styles.titleShort}>· {dim.short}</em>
          </h2>
          <p className={styles.lead}>{dim.subtitle}</p>
        </div>
        <div className={styles.headRight}>
          <div className={`${styles.scoreBig} ${styles[`scoreBig_${band}`]}`}>
            {score != null ? scoreClamped : "—"}
            <sup>/100</sup>
          </div>
          <div className={`${styles.bandPill} ${styles[`bandPill_${band}`]}`}>
            {bandLabel(band)}
          </div>
          {pesoPct != null && (
            <div className={styles.weightPill}>
              peso {pesoPct}%
            </div>
          )}
        </div>
      </header>

      {zona?.zona_id && sessionId && (
        <NarrativaPanel
          dimKey={dim.key}
          dimNombre={dim.name}
          zonaId={zona.zona_id}
          sessionId={sessionId}
        />
      )}

      <div className={styles.body}>
        <div className={styles.visualCol}>
          <VisualForDim
            dim={dim}
            zona={zona}
            zonaId={zona?.zona_id ?? ""}
            sessionId={sessionId}
          />
        </div>

        <DimensionInsight
          dim={dim}
          band={band}
          score={score}
          scoreClamped={scoreClamped}
          pesoPct={pesoPct}
          hechos={hechos}
          positivos={positivos}
          negativos={negativos}
          titular={titular}
          explicacionCorta={explicacionCorta}
          implicaciones={implicaciones}
          showActions={implicaciones.length > 0}
        />
      </div>
    </section>
  );
}
