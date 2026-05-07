"use client";

import dynamic from "next/dynamic";
import type {
  ZonaPreview,
  ZonaDetalle,
  CompetenciaDetalle,
  ExplicacionDimension,
} from "@/lib/types";
import { DIM_BY_KEY, scoreBand, bandLabel } from "./dimensions";
import { clampScore, normalizeList } from "./helpers";
import { getImplicaciones } from "./implicaciones";
import DimensionInsight from "./DimensionInsight";
import styles from "./DimSection.module.css";

// CompetenciaPanel usa Leaflet → no se puede SSR.
const CompetenciaPanel = dynamic(() => import("../CompetenciaPanel"), {
  ssr: false,
  loading: () => (
    <div className={styles.emptyVisual}>Cargando análisis de competencia…</div>
  ),
});

interface Props {
  idx: number;
  zone: ZonaPreview;
  sessionId: string;
  zona: ZonaDetalle | null;
  explicacion?: ExplicacionDimension;
  pesoBase?: number;
  pesoMod?: number;
  competencia: CompetenciaDetalle | null;
  competenciaLoading: boolean;
}

function pct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "";
  const normalized = value > 0 && value <= 1 ? value * 100 : value;
  return `${Math.round(normalized)}%`;
}

function hechosCompetencia(data: CompetenciaDetalle | null, zona: ZonaDetalle | null): string[] {
  const facts: string[] = [];
  if (data) {
    const hhi = typeof data.hhi_index === "number" && Number.isFinite(data.hhi_index)
      ? data.hhi_index
      : null;
    facts.push(`${data.num_directos ?? 0} competidores directos en ${data.radio_m ?? 500} m.`);
    if (hhi != null) facts.push(`HHI ${hhi.toFixed(2)}: ${hhi < 0.15 ? "mercado fragmentado" : hhi < 0.4 ? "presión media" : "mercado concentrado"}.`);
    if (data.pct_vulnerables != null) facts.push(`Vulnerables: ${pct(data.pct_vulnerables)} del mercado directo.`);
    const cobertura = data.resenas_cobertura;
    if (cobertura?.resenas_total) facts.push(`${cobertura.resenas_total.toLocaleString("es-ES")} reseñas reales analizadas.`);
    return facts;
  }

  if (zona?.num_directos != null) facts.push(`${zona.num_directos} competidores directos detectados.`);
  if (zona?.hhi_index != null) facts.push(`HHI ${zona.hhi_index.toFixed(2)}.`);
  return facts;
}

function fuerzasCompetencia(data: CompetenciaDetalle | null): { up: string[]; down: string[] } {
  if (!data) return { up: [], down: [] };
  const up: string[] = [];
  const down: string[] = [];

  if ((data.oportunidad_mercado ?? 0) >= 65) up.push("Hay hueco de mercado para una propuesta mejor posicionada.");
  if ((data.score_complementarios ?? 0) >= 60) up.push("Los negocios complementarios pueden aportar flujo compatible.");
  if ((data.pct_vulnerables ?? 0) >= 25) up.push("Parte del mercado directo parece desplazable por reseñas o tracción débil.");
  if ((data.amenaza_incumbentes ?? 0) >= 65) down.push("Los incumbentes cercanos tienen suficiente fuerza como para exigir diferenciación clara.");
  if ((data.num_directos ?? 0) >= 4) down.push("La densidad de directos puede encarecer la captación inicial.");
  if ((data.hhi_index ?? 0) >= 0.4) down.push("El mercado está concentrado y hay pocos huecos obvios.");

  return { up, down };
}

/**
 * CompetenciaSection — sección editorial específica para la dimensión
 * Competencia. Misma estructura narrativa que DimSection (kicker §, título,
 * conclusión, drivers, datos clave, implicaciones) pero el visual de la
 * derecha es el CompetenciaPanel grande antiguo (con Leaflet, listas
 * clasificadas, ratings, reseñas, panel "ver más", etc.) en lugar del
 * mini-panel resumen.
 */
export default function CompetenciaSection({
  idx,
  zone,
  sessionId,
  zona,
  explicacion,
  pesoBase,
  pesoMod,
  competencia,
  competenciaLoading,
}: Props) {
  const dim = DIM_BY_KEY.competencia;
  const score = zona?.scores_dimensiones?.competencia;
  const band = scoreBand(score);
  const scoreClamped = clampScore(score);
  const pesoActivo = pesoMod ?? pesoBase;
  const pesoPct = pesoActivo != null && pesoActivo > 0
    ? Math.round(pesoActivo * 100)
    : null;
  const sectorCodigo = zona?.sector_codigo ?? competencia?.sector;
  const titular = explicacion?.titular?.trim() ||
    `${dim.name} · ${bandLabel(band)}`;
  const explicacionCorta = explicacion?.explicacion_corta?.trim() ||
    "La lectura combina densidad, fuerza de rivales, reseñas reales y negocios complementarios del radio cercano.";
  const fuerzas = fuerzasCompetencia(competencia);
  const hechos = [
    ...hechosCompetencia(competencia, zona),
    ...normalizeList(explicacion?.hechos_clave),
  ];
  const positivos = [
    ...fuerzas.up,
    ...normalizeList(explicacion?.porque_sube),
  ];
  const negativos = [
    ...fuerzas.down,
    ...normalizeList(explicacion?.porque_baja),
  ];
  const implicaciones = getImplicaciones("competencia", sectorCodigo);

  const sectionId = "sec-competencia";
  const num = String(idx + 2).padStart(2, "0");

  return (
    <section
      id={sectionId}
      className={`${styles.section} ${styles[`section_${band}`]}`}
      aria-label="Dimensión Competencia"
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

      <div className={styles.body}>
        <div className={styles.visualCol}>
          <CompetenciaPanel
            competencia={competencia}
            loading={competenciaLoading}
            zona={zone}
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
