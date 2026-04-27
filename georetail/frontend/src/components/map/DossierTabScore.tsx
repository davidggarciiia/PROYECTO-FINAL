"use client";

import { Fragment, useState, type CSSProperties } from "react";
import type {
  ZonaPreview,
  LocalDetalleResponse,
  ScoresDimensiones,
  ExplicacionDimension,
  SeguridadDetalle,
} from "@/lib/types";
import {
  DIMENSIONS,
  type DimensionKey,
  type DimensionMeta,
} from "./dimensions";
import styles from "./Dossier.module.css";
import TransportePanel from "../TransportePanel";
import FlujoPanel from "../FlujoPanel";
import DemografiaPanel from "../DemografiaPanel";
import SeguridadPanel from "../SeguridadPanel";
import TurismoPanel from "../TurismoPanel";
import DinamismoPanel from "../DinamismoPanel";
import CompetenciaDimPanel from "../CompetenciaDimPanel";
import PrecioAlquilerPanel from "../PrecioAlquilerPanel";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
}

type Band = "hi" | "mid" | "lo" | "na";
type DriverKind = "up" | "down" | "fact";

interface DrawerProps {
  dim: DimensionMeta;
  value: number | null | undefined;
  explicacion?: ExplicacionDimension;
  pesoBase?: number;
  pesoMod?: number;
  sectorCodigo?: string;
  zonaId?: string;
  detalle?: LocalDetalleResponse | null;
}

const GRID_COLS = 2;

function scoreBand(score?: number | null): Band {
  if (score == null) return "na";
  if (score >= 75) return "hi";
  if (score >= 55) return "mid";
  return "lo";
}

function bandLabel(band: Band): string {
  if (band === "hi") return "Punto fuerte";
  if (band === "mid") return "Aceptable";
  if (band === "lo") return "Punto debil";
  return "Sin datos";
}

function clampScore(value?: number | null): number {
  if (value == null || Number.isNaN(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

function normalizeList(items?: string[] | null): string[] {
  return (items ?? []).map((item) => item.trim()).filter(Boolean);
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function getExplicacion(
  explicaciones: Record<string, ExplicacionDimension>,
  key: DimensionKey,
): ExplicacionDimension | undefined {
  if (key === "dinamismo") {
    return explicaciones.dinamismo ?? explicaciones.entorno_comercial;
  }
  return explicaciones[key];
}

function scoreNarrative(dimName: string, band: Band): string {
  if (band === "hi") return `${dimName} esta empujando la recomendacion de esta zona.`;
  if (band === "mid") return `${dimName} acompana, pero no decide por si sola.`;
  if (band === "lo") return `${dimName} es uno de los frenos que conviene revisar antes de decidir.`;
  return `Aun no hay datos suficientes para interpretar ${dimName}.`;
}

function razonPeso(
  dimKey: DimensionKey,
  sectorCodigo: string | undefined,
  peso: number,
): string {
  const pct = Math.round(peso * 100);
  const sector = sectorCodigo ?? "tu negocio";
  const nivel = peso >= 0.18 ? "alto" : peso >= 0.10 ? "medio" : "bajo";
  const textos: Partial<Record<DimensionKey, string>> = {
    flujo_peatonal: `Pesa ${pct}% (${nivel}) porque el volumen de paso define cuanta demanda espontanea puede ver el local cada dia.`,
    demografia: `Pesa ${pct}% (${nivel}) porque renta, edad y perfil residencial marcan si el barrio encaja con el ticket de ${sector}.`,
    competencia: `Pesa ${pct}% (${nivel}) porque mide si el mercado cercano deja espacio real o exige una diferenciacion muy fuerte.`,
    transporte: `Pesa ${pct}% (${nivel}) porque amplia el radio de captacion mas alla de la calle inmediata.`,
    seguridad: `Pesa ${pct}% (${nivel}) porque afecta a horarios, percepcion y comodidad de visita, sobre todo en tarde-noche.`,
    turismo: `Pesa ${pct}% (${nivel}) porque determina si el negocio depende de visitantes, picos estacionales o clientela local estable.`,
    dinamismo: `Pesa ${pct}% (${nivel}) porque una zona que abre, retiene y atrae operadores reduce el riesgo de arrancar alli.`,
    precio_alquiler: `Pesa ${pct}% (${nivel}) porque condiciona el punto muerto: cuanto mas caro el local, mas ventas hacen falta para sobrevivir.`,
  };
  return textos[dimKey] ?? `Esta dimension pesa ${pct}% en el score de ${sector}.`;
}

function WeightCard({
  pesoBase,
  pesoMod,
  dim,
  sectorCodigo,
}: {
  pesoBase?: number;
  pesoMod?: number;
  dim: DimensionMeta;
  sectorCodigo?: string;
}) {
  const pesoActivo = pesoMod ?? pesoBase;
  if (pesoActivo == null || pesoActivo <= 0) return null;

  const basePct = pesoBase != null ? Math.round(pesoBase * 100) : null;
  const modPct = pesoMod != null ? Math.round(pesoMod * 100) : null;
  const activePct = Math.round(pesoActivo * 100);
  const diff = basePct != null && modPct != null ? modPct - basePct : 0;
  const hasModulation = basePct != null && modPct != null && diff !== 0;

  return (
    <section className={styles.weightStoryCard}>
      <div className={styles.weightStoryTop}>
        <span className={styles.metricKicker}>Peso en el score</span>
        <span className={styles.weightStoryValue}>{activePct}%</span>
      </div>
      <div className={styles.weightTrack} aria-hidden="true">
        <span style={{ width: `${Math.min(activePct, 100)}%` }} />
      </div>
      {hasModulation && (
        <div className={styles.weightCompare}>
          <span>Base {basePct}%</span>
          <span className={diff > 0 ? styles.weightUp : styles.weightDown}>
            {diff > 0 ? "+" : ""}
            {diff} por perfil
          </span>
        </div>
      )}
      <p className={styles.metricBody}>{razonPeso(dim.key, sectorCodigo, pesoActivo)}</p>
    </section>
  );
}

function DriverList({
  title,
  items,
  kind,
}: {
  title: string;
  items: string[];
  kind: DriverKind;
}) {
  if (items.length === 0) return null;
  return (
    <section className={`${styles.driverCard} ${styles[`driver_${kind}`]}`}>
      <div className={styles.driverTitle}>{title}</div>
      <ul className={styles.driverList}>
        {items.map((item, index) => (
          <li key={`${kind}-${index}`}>
            <span className={styles.driverMark} aria-hidden="true">
              {kind === "up" ? "+" : kind === "down" ? "-" : ""}
            </span>
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function EvidencePanel({
  hechos,
  fuentes,
  confianza,
}: {
  hechos: string[];
  fuentes: string[];
  confianza?: string;
}) {
  return (
    <section className={styles.evidencePanel}>
      <div className={styles.evidenceHeader}>
        <span className={styles.drawerSectionLabel}>Datos reales</span>
        {confianza && (
          <span className={styles.confidencePill}>Confianza {confianza}</span>
        )}
      </div>
      {hechos.length > 0 ? (
        <div className={styles.factGrid}>
          {hechos.map((hecho, index) => (
            <div key={`fact-${index}`} className={styles.factCard}>
              <span className={styles.factIndex}>{String(index + 1).padStart(2, "0")}</span>
              <p>{hecho}</p>
            </div>
          ))}
        </div>
      ) : (
        <p className={styles.drawerInterpText}>
          Todavia no hay hechos trazables suficientes para esta dimension.
        </p>
      )}
      {fuentes.length > 0 && (
        <div className={styles.drawerSources}>
          {fuentes.map((source) => (
            <span key={source} className={styles.sourceChip}>
              {source}
            </span>
          ))}
        </div>
      )}
    </section>
  );
}

function DimensionVisualPanel({
  dim,
  value,
  detalle,
  zonaId,
  sectorCodigo,
  hechos,
}: {
  dim: DimensionMeta;
  value: number | null | undefined;
  detalle?: LocalDetalleResponse | null;
  zonaId?: string;
  sectorCodigo?: string;
  hechos: string[];
}) {
  const zona = detalle?.zona ?? null;
  const zonaAny = (zona ?? {}) as Record<string, unknown>;
  const directCount =
    asNumber(zonaAny.num_directos) ??
    zona?.competidores_cercanos?.filter((c) => c.es_competencia_directa).length ??
    null;
  const precioM2 =
    asNumber(zonaAny.precio_alquiler_m2) ??
    (zona?.alquiler_mensual != null && zona.m2 ? zona.alquiler_mensual / zona.m2 : null);

  if (dim.key === "flujo_peatonal") {
    return (
      <FlujoPanel
        flujo_dia={zona?.flujo_peatonal_dia}
        vcity_peatones_dia={zona?.vcity_flujo_peatonal}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "demografia") {
    return (
      <DemografiaPanel
        renta_media_hogar={zona?.renta_media_hogar}
        edad_media={zona?.edad_media}
        pct_extranjeros={zona?.pct_extranjeros}
        nivel_estudios_alto_pct={zona?.nivel_estudios_alto_pct}
        delta_renta_3a={zona?.delta_renta_3a}
        indice_potencial_consumo={asNumber(zonaAny.indice_potencial_consumo)}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "competencia") {
    return (
      <CompetenciaDimPanel
        hhi_index={asNumber(zonaAny.hhi_index)}
        num_directos={directCount}
        competidores_cercanos={zona?.competidores_cercanos}
        score={value ?? undefined}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "transporte" && zonaId) {
    return (
      <TransportePanel
        zonaId={zonaId}
        fallbackLineas={zona?.num_lineas_transporte}
        fallbackParadas={zona?.num_paradas_transporte}
        score={value ?? undefined}
        numBicing={asNumber(zonaAny.num_bicing_400m) ?? undefined}
      />
    );
  }

  if (dim.key === "seguridad") {
    return (
      <SeguridadPanel
        detalle={zona?.seguridad_detalle as SeguridadDetalle | null | undefined}
        score={value ?? undefined}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "turismo") {
    return (
      <TurismoPanel
        score_turismo={zona?.score_turismo}
        score_turismo_airbnb={asNumber(zonaAny.score_turismo_airbnb)}
        score_turismo_hut={asNumber(zonaAny.score_turismo_hut)}
        airbnb_density_500m={zona?.airbnb_density_500m}
        airbnb_occupancy_est={zona?.airbnb_occupancy_est}
        booking_hoteles_500m={zona?.booking_hoteles_500m}
        booking_rating_medio={asNumber(zonaAny.booking_rating_medio)}
        eventos_culturales_500m={asNumber(zonaAny.eventos_culturales_500m)}
        venues_musicales_500m={asNumber(zonaAny.venues_musicales_500m)}
        dist_playa_m={asNumber(zonaAny.dist_playa_m)}
        dist_landmark_top3_m={asNumber(zonaAny.dist_landmark_top3_m)}
        vcity_tourist_rate={asNumber(zonaAny.vcity_tourist_rate)}
        seasonality_summer_lift={asNumber(zonaAny.seasonality_summer_lift)}
        sector_codigo={sectorCodigo}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "dinamismo") {
    return (
      <DinamismoPanel
        tendencia={asString(zonaAny.tendencia)}
        tasa_supervivencia_3a={asNumber(zonaAny.tasa_supervivencia_3a)}
        ratio_apertura_cierre_1a={asNumber(zonaAny.ratio_apertura_cierre_1a)}
        hhi_sectorial={asNumber(zonaAny.hhi_sectorial)}
        negocios_historico_count={asNumber(zonaAny.negocios_historico_count)}
        renta_variacion_3a={asNumber(zonaAny.renta_variacion_3a) ?? zona?.delta_renta_3a}
        licencias_abiertas_1a={asNumber(zonaAny.licencias_abiertas_1a)}
        licencias_cerradas_1a={asNumber(zonaAny.licencias_cerradas_1a)}
        vacantes_ratio={asNumber(zonaAny.vacantes_ratio)}
        poblacion_variacion_3a={asNumber(zonaAny.poblacion_variacion_3a)}
        precio_alquiler_variacion_1a={asNumber(zonaAny.precio_alquiler_variacion_1a)}
        score={value ?? undefined}
        explicacion_bullets={hechos}
      />
    );
  }

  if (dim.key === "precio_alquiler") {
    return (
      <PrecioAlquilerPanel
        alquiler_mensual={zona?.alquiler_mensual}
        precio_m2={precioM2}
        score={value ?? undefined}
        explicacion_bullets={hechos}
      />
    );
  }

  return (
    <div className={styles.emptyVisual}>
      <span className={styles.drawerInterpMuted}>Sin visual especifico</span>
    </div>
  );
}

function DimDrawer({
  dim,
  value,
  explicacion,
  pesoBase,
  pesoMod,
  sectorCodigo,
  zonaId,
  detalle,
}: DrawerProps) {
  const band = scoreBand(value ?? null);
  const label = bandLabel(band);
  const score = clampScore(value ?? null);
  const hechos = normalizeList(explicacion?.hechos_clave);
  const fuentes = normalizeList(explicacion?.fuentes).length > 0
    ? normalizeList(explicacion?.fuentes)
    : dim.sources;
  const positivos = normalizeList(explicacion?.porque_sube);
  const negativos = normalizeList(explicacion?.porque_baja);
  const titulo =
    explicacion?.titular?.trim() || scoreNarrative(dim.name, band);
  const subtitulo =
    explicacion?.explicacion_corta?.trim() || dim.what;
  const impacto = explicacion?.impacto_modelo?.trim();
  const pesoActivo = pesoMod ?? pesoBase;

  return (
    <div
      className={`${styles.dimDrawerFull} ${styles.dimensionStory} ${styles[`drawer_${band}`]}`}
      role="region"
      aria-label={`Detalle dimension ${dim.name}`}
    >
      <header className={styles.dimensionHero}>
        <div className={styles.dimensionHeroCopy}>
          <div className={styles.drawerTitleRow}>
            <h3 className={styles.drawerTitle}>{dim.name}</h3>
            <span className={`${styles.drawerBand} ${styles[`drawerBand_${band}`]}`}>
              {label}
            </span>
            {pesoActivo != null && pesoActivo > 0 && (
              <span className={styles.drawerWeightPill}>
                {Math.round(pesoActivo * 100)}% peso
              </span>
            )}
          </div>
          <p className={styles.drawerSubtitleNew}>{dim.subtitle}</p>
        </div>

        <div className={styles.dimensionGaugeWrap}>
          <div
            className={styles.dimensionGauge}
            style={{ "--score-pct": `${score}%` } as CSSProperties}
            role="img"
            aria-label={`${dim.name}: ${value != null ? score : "sin datos"} sobre 100`}
          >
            <div className={styles.dimensionGaugeInner}>
              <span>{value != null ? score : "--"}</span>
              <small>/100</small>
            </div>
          </div>
        </div>
      </header>

      <div className={styles.dimensionInsightGrid}>
        <div className={styles.dimensionNarrative}>
          <section className={styles.conclusionCard}>
            <span className={styles.metricKicker}>Conclusion</span>
            <p className={styles.conclusionTitle}>{titulo}</p>
            <p className={styles.conclusionText}>{subtitulo}</p>
          </section>

          <section className={styles.whatCard}>
            <span className={styles.metricKicker}>Que mide</span>
            <p>{dim.what}</p>
          </section>

          {(positivos.length > 0 || negativos.length > 0 || impacto) && (
            <div className={styles.driverGrid}>
              <DriverList title="A favor" items={positivos} kind="up" />
              <DriverList title="Frenos" items={negativos} kind="down" />
              {impacto && (
                <section className={`${styles.driverCard} ${styles.driver_fact}`}>
                  <div className={styles.driverTitle}>Modelo</div>
                  <p className={styles.modelImpact}>{impacto}</p>
                </section>
              )}
            </div>
          )}

          <WeightCard
            pesoBase={pesoBase}
            pesoMod={pesoMod}
            dim={dim}
            sectorCodigo={sectorCodigo}
          />

          <EvidencePanel
            hechos={hechos}
            fuentes={fuentes}
            confianza={explicacion?.confianza}
          />
        </div>

        <aside className={styles.dimensionVisual}>
          <DimensionVisualPanel
            dim={dim}
            value={value}
            detalle={detalle}
            zonaId={zonaId}
            sectorCodigo={sectorCodigo}
            hechos={hechos}
          />
        </aside>
      </div>
    </div>
  );
}

export default function DossierTabScore({ zone, detalle, loading }: Props) {
  const [openKey, setOpenKey] = useState<DimensionKey | null>(null);

  const dims: ScoresDimensiones = detalle?.zona.scores_dimensiones ?? {};
  const prob = detalle?.zona.probabilidad_supervivencia;
  const explicaciones: Record<string, ExplicacionDimension> =
    detalle?.zona.explicaciones_dimensiones ??
    detalle?.zona.analisis_ia?.explicaciones_dimensiones ??
    {};
  const pesosBase = detalle?.zona.pesos_dimensiones ?? {};
  const pesosMod = detalle?.zona.pesos_modulados ?? {};
  const pesosActivos = Object.keys(pesosMod).length > 0 ? pesosMod : pesosBase;
  const sectorCodigo = detalle?.zona.sector_codigo;

  if (loading && !detalle) {
    return (
      <div className={styles.loading}>
        <div className="spinner" />
        <span>Calculando analisis...</span>
      </div>
    );
  }

  const openIdx = openKey
    ? DIMENSIONS.findIndex((d) => d.key === openKey)
    : -1;
  const openRow = openIdx >= 0 ? Math.floor(openIdx / GRID_COLS) : -1;
  const openDim = openIdx >= 0 ? DIMENSIONS[openIdx] : null;

  const rows: DimensionMeta[][] = [];
  for (let i = 0; i < DIMENSIONS.length; i += GRID_COLS) {
    rows.push(DIMENSIONS.slice(i, i + GRID_COLS));
  }

  return (
    <div className={styles.scoreTab}>
      {prob != null && (
        <div className={styles.probBanner}>
          <div className={styles.probHeaderRow}>
            <div>
              <div className={styles.probLabel}>Probabilidad supervivencia 3 anos</div>
              <p className={styles.probHint}>Lectura global del modelo para este local.</p>
            </div>
            <div className={styles.probValue}>
              {Math.round(prob * 100)}
              <span className={styles.probPct}>%</span>
            </div>
          </div>
          <div className={styles.probBarTrack}>
            <div
              className={styles.probBarFill}
              style={{ width: `${Math.round(prob * 100)}%` }}
            />
          </div>
        </div>
      )}

      <div className={styles.dimGridStack}>
        {rows.map((rowDims, rowIdx) => (
          <Fragment key={`row-${rowIdx}`}>
            <div className={styles.dimGrid}>
              {rowDims.map((dim) => {
                const val = dims[dim.key];
                const band = scoreBand(val);
                const isOpen = openKey === dim.key;
                const peso = pesosActivos[dim.key];
                return (
                  <button
                    key={dim.key}
                    type="button"
                    className={`${styles.dimCell} ${styles[`dim_${band}`]} ${
                      isOpen ? styles.dimCellOpen : ""
                    }`}
                    onClick={() => setOpenKey(isOpen ? null : dim.key)}
                    aria-expanded={isOpen}
                  >
                    <div className={styles.dimCellHead}>
                      <span className={styles.dimCellName}>{dim.name}</span>
                      <span className={styles.dimCellVal}>
                        {val != null ? Math.round(val) : "--"}
                      </span>
                    </div>
                    <div className={styles.dimCellBarTrack}>
                      <div
                        className={styles.dimCellBarFill}
                        style={{
                          width:
                            val != null
                              ? `${Math.max(0, Math.min(100, val))}%`
                              : "0%",
                        }}
                      />
                    </div>
                    <div className={styles.dimCellFoot}>
                      <span>{bandLabel(band)}</span>
                      {peso != null && peso > 0 ? (
                        <span className={styles.dimCellWeight}>
                          {Math.round(peso * 100)}% peso
                        </span>
                      ) : (
                        <span className={styles.dimCellHint}>{dim.hint}</span>
                      )}
                    </div>
                  </button>
                );
              })}
            </div>

            {rowIdx === openRow && openDim && (
              <DimDrawer
                dim={openDim}
                value={dims[openDim.key]}
                explicacion={getExplicacion(explicaciones, openDim.key)}
                pesoBase={pesosBase[openDim.key]}
                pesoMod={pesosMod[openDim.key]}
                sectorCodigo={sectorCodigo}
                zonaId={zone.zona_id}
                detalle={detalle}
              />
            )}
          </Fragment>
        ))}
      </div>

      {detalle?.zona.analisis_ia?.resumen_global && (
        <div className={styles.summary}>
          <div className={styles.summaryEyebrow}>
            <span className={styles.tick}>*</span>RESUMEN IA
          </div>
          <p className={styles.summaryText}>{detalle.zona.analisis_ia.resumen_global}</p>
        </div>
      )}
    </div>
  );
}
