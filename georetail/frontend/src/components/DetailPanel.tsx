"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import type {
  CompetenciaDetalle,
  ExplicacionDimension,
  ImpactoModeloDimension,
  LocalDetalleResponse,
  ScoresDimensiones,
  ZonaDetalle,
  ZonaPreview,
} from "@/lib/types";
import { api } from "@/lib/api";
import FinancialPanel from "./FinancialPanel";
import LegalPanel from "./LegalPanel";
import ScoreBars from "./ScoreBars";
import TransportePanel from "./TransportePanel";
import DimensionDrawer from "./DimensionDrawer";
import styles from "./DetailPanel.module.css";

const CompetenciaPanel = dynamic(() => import("./CompetenciaPanel"), {
  ssr: false,
  loading: () => <div className={styles.inlineLoading}>Cargando análisis de competencia...</div>,
});

interface Props {
  zona: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
  sessionId: string;
  onClose: () => void;
}

type Tab = "detalles" | "competencia" | "transporte" | "financiero" | "legal";

const MIN_W = 360;
const MAX_W = 760;
const DEFAULT_W = 470;

const DIMENSION_ORDER: Array<{ key: keyof ScoresDimensiones; label: string }> = [
  { key: "flujo_peatonal", label: "Flujo peatonal" },
  { key: "demografia", label: "Demografía" },
  { key: "competencia", label: "Competencia" },
  { key: "precio_alquiler", label: "Precio de alquiler" },
  { key: "transporte", label: "Transporte" },
  { key: "seguridad", label: "Seguridad" },
  { key: "turismo", label: "Turismo" },
  { key: "dinamismo", label: "Dinamismo" },
];

function getTone(score: number | undefined) {
  if (score == null) {
    return { className: styles.toneNeutral, label: "Sin dato" };
  }
  if (score >= 75) return { className: styles.tonePositive, label: "Muy favorable" };
  if (score >= 55) return { className: styles.toneMid, label: "Favorable" };
  if (score >= 40) return { className: styles.toneWarn, label: "Con reservas" };
  return { className: styles.toneNegative, label: "Débil" };
}

function formatCurrency(value?: number) {
  if (value == null) return "—";
  return `${Math.round(value).toLocaleString("es-ES")} €`;
}

function formatPercent(value?: number | null, digits = 0) {
  if (value == null) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

function formatNumber(value?: number | null, digits = 0) {
  if (value == null) return "—";
  return Number(value).toLocaleString("es-ES", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function confidenceLabel(value?: string) {
  if (!value) return "Confianza media";
  return `Confianza ${value}`;
}

function impactSummary(impact?: ImpactoModeloDimension) {
  if (!impact) return "Sin detalle del impacto del modelo";
  if (impact.tendencia === "empuja_a_favor") return "El modelo empuja esta dimensión a favor";
  if (impact.tendencia === "empuja_en_contra") return "El modelo penaliza esta dimensión";
  return "El modelo la usa con impacto moderado";
}

function hasMeaningfulText(value?: string | null) {
  return Boolean(value && value.trim());
}

function observedFactsForDimension(key: keyof ScoresDimensiones, z: ZonaDetalle): string[] {
  switch (key) {
    case "flujo_peatonal":
      return [
        z.flujo_peatonal_dia?.manana != null ? `Mañana: ${formatNumber(z.flujo_peatonal_dia.manana)} personas/hora` : "",
        z.flujo_peatonal_dia?.tarde != null ? `Tarde: ${formatNumber(z.flujo_peatonal_dia.tarde)} personas/hora` : "",
        z.flujo_peatonal_dia?.noche != null ? `Noche: ${formatNumber(z.flujo_peatonal_dia.noche)} personas/hora` : "",
      ].filter(Boolean);
    case "demografia":
      return [
        z.renta_media_hogar != null ? `Renta media del hogar: ${formatCurrency(z.renta_media_hogar)}` : "",
        z.edad_media != null ? `Edad media: ${formatNumber(z.edad_media, 0)} años` : "",
        z.pct_extranjeros != null ? `Población extranjera: ${formatNumber(z.pct_extranjeros, 0)}%` : "",
      ].filter(Boolean);
    case "transporte":
      return [
        z.num_lineas_transporte != null ? `Líneas cercanas: ${formatNumber(z.num_lineas_transporte, 0)}` : "",
        z.num_paradas_transporte != null ? `Paradas cercanas: ${formatNumber(z.num_paradas_transporte, 0)}` : "",
      ].filter(Boolean);
    case "competencia":
      return [
        `Competidores cercanos detectados: ${formatNumber(z.competidores_cercanos.length, 0)}`,
        ...z.competidores_cercanos
          .slice(0, 3)
          .map((c) => `${c.nombre} a ${formatNumber(c.distancia_m, 0)} m`),
      ].filter(Boolean);
    case "precio_alquiler":
      return [
        z.alquiler_mensual != null ? `Alquiler mensual estimado: ${formatCurrency(z.alquiler_mensual)}` : "",
        z.m2 != null ? `Superficie: ${formatNumber(z.m2, 0)} m²` : "",
      ].filter(Boolean);
    case "seguridad":
      return [
        z.seguridad_detalle?.incidencias_por_1000hab != null ? `Incidencias por 1.000 hab: ${formatNumber(z.seguridad_detalle.incidencias_por_1000hab, 1)}` : "",
        z.seguridad_detalle?.comisarias_1km != null ? `Comisarías a 1 km: ${formatNumber(z.seguridad_detalle.comisarias_1km, 0)}` : "",
        z.seguridad_detalle?.dist_comisaria_m != null ? `Comisaría más cercana: ${formatNumber(z.seguridad_detalle.dist_comisaria_m, 0)} m` : "",
      ].filter(Boolean);
    case "turismo":
      return [z.score_turismo != null ? `Score turístico: ${formatNumber(z.score_turismo, 0)}` : ""].filter(Boolean);
    default:
      return [];
  }
}

function fallbackExplanation(key: keyof ScoresDimensiones, label: string, score: number | undefined, z: ZonaDetalle): ExplicacionDimension {
  const normalized = score != null ? `${Math.round(score)}/100` : "sin score disponible";
  return {
    score,
    titular: `${label}: ${normalized}`,
    explicacion_corta: "No hay explicación textual validada para esta dimensión. Solo se muestran los datos observados del backend.",
    porque_sube: [],
    porque_baja: [],
    hechos_clave: observedFactsForDimension(key, z),
    impacto_modelo: "Sin detalle validado del impacto del modelo para esta dimensión.",
    // Confianza baja deja claro al usuario que es un fallback, no un análisis real.
    confianza: "baja",
    fuentes: [],
  };
}

function detailMetrics(z: ZonaDetalle) {
  return [
    { label: "Alquiler", value: formatCurrency(z.alquiler_mensual), note: "coste mensual estimado" },
    { label: "Superficie", value: z.m2 != null ? `${formatNumber(z.m2, 0)} m²` : "—", note: "tamaño del local" },
    { label: "Supervivencia 3 años", value: formatPercent(z.probabilidad_supervivencia, 0), note: "estimación del modelo" },
    { label: "Negocios activos", value: z.num_negocios_activos != null ? formatNumber(z.num_negocios_activos, 0) : "—", note: "actividad comercial" },
  ];
}

function buildDafo(z: ZonaDetalle) {
  // Nunca sintetizamos DAFO desde scores. Si el LLM no validó puntos fuertes/débiles
  // el usuario debe ver "sin datos" en vez de "competencia (87/100)" presentado como fortaleza.
  const strengths = [...(z.analisis_ia?.puntos_fuertes ?? [])];
  const weaknesses = [...(z.analisis_ia?.puntos_debiles ?? [])];
  const opportunities = z.analisis_ia?.oportunidad ? [z.analisis_ia.oportunidad] : [];
  const threats = z.analisis_ia?.riesgos ? [z.analisis_ia.riesgos] : [];

  return {
    Fortalezas: strengths.length ? strengths : ["Sin fortalezas validadas por el análisis IA."],
    Debilidades: weaknesses.length ? weaknesses : ["Sin debilidades validadas por el análisis IA."],
    Oportunidades: opportunities.length ? opportunities : ["Sin oportunidades validadas por el análisis IA."],
    Amenazas: threats.length ? threats : ["Sin amenazas validadas por el análisis IA."],
  };
}

function SkeletonBlock() {
  return (
    <div className={styles.skeletonStack}>
      <div className="skeleton" style={{ height: 120, borderRadius: 18 }} />
      <div className="skeleton" style={{ height: 84, borderRadius: 16 }} />
      <div className="skeleton" style={{ height: 260, borderRadius: 18 }} />
    </div>
  );
}

export default function DetailPanel({ zona, detalle, loading, sessionId, onClose }: Props) {
  const [tab, setTab] = useState<Tab>("detalles");
  const [financiero, setFinanciero] = useState<import("@/lib/types").FinancieroResponse | null>(null);
  const [loadingFin, setLoadingFin] = useState(false);
  const [competencia, setCompetencia] = useState<CompetenciaDetalle | null>(null);
  const [loadingComp, setLoadingComp] = useState(false);
  const [competenciaError, setCompetenciaError] = useState<string | null>(null);
  // Drawer con ingredientes concretos (landmarks, venues...) de una dimensión.
  const [activeDim, setActiveDim] = useState<keyof ScoresDimensiones | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const dragState = useRef({ active: false, startX: 0, startW: DEFAULT_W });

  useEffect(() => {
    setTab("detalles");
    setFinanciero(null);
    setCompetencia(null);
    setCompetenciaError(null);
  }, [zona.zona_id]);

  const loadFinanciero = useCallback(async () => {
    if (financiero || loadingFin) return;
    setLoadingFin(true);
    try {
      const data = await api.financiero(zona.zona_id, sessionId);
      setFinanciero(data);
    } catch (error) {
      console.error("Error financiero:", error);
    } finally {
      setLoadingFin(false);
    }
  }, [financiero, loadingFin, sessionId, zona.zona_id]);

  const loadCompetencia = useCallback(async () => {
    if (competencia || loadingComp) return;
    setLoadingComp(true);
    setCompetenciaError(null);
    try {
      const data = await api.competencia(zona.zona_id, sessionId);
      setCompetencia(data);
    } catch (error) {
      console.error("Error competencia:", error);
      setCompetenciaError("No se ha podido cargar el análisis de competencia.");
    } finally {
      setLoadingComp(false);
    }
  }, [competencia, loadingComp, sessionId, zona.zona_id]);

  useEffect(() => {
    if (tab === "competencia") void loadCompetencia();
  }, [tab, loadCompetencia, loadFinanciero]);

  const onResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startW = panelRef.current?.offsetWidth ?? DEFAULT_W;
    dragState.current = { active: true, startX: e.clientX, startW };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    const onMove = (ev: MouseEvent) => {
      if (!dragState.current.active || !panelRef.current) return;
      const delta = dragState.current.startX - ev.clientX;
      const newW = Math.min(MAX_W, Math.max(MIN_W, dragState.current.startW + delta));
      panelRef.current.style.width = `${newW}px`;
    };

    const onUp = () => {
      dragState.current.active = false;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };

    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const z = detalle?.zona;
  const score = z?.score_global ?? zona.score_global ?? 0;
  const scoreTone = getTone(score);
  const summaryText = hasMeaningfulText(z?.resumen_global_llm)
    ? z!.resumen_global_llm!
    : hasMeaningfulText(z?.analisis_ia?.resumen_global)
      ? z!.analisis_ia!.resumen_global
      : hasMeaningfulText(z?.analisis_ia?.resumen)
        ? z!.analisis_ia!.resumen
        : "No hay todavía un resumen validado para este local.";

  const metrics = useMemo(() => (z ? detailMetrics(z) : []), [z]);
  const dafo = useMemo(() => (z ? buildDafo(z) : { Fortalezas: [], Debilidades: [], Oportunidades: [], Amenazas: [] }), [z]);
  const dimensions = useMemo(() => {
    if (!z) return [];
    return DIMENSION_ORDER
      .map(({ key, label }) => {
        const scoreValue = z.scores_dimensiones?.[key];
        const explicacionValidada =
          z.explicaciones_dimensiones?.[key] ??
          z.analisis_ia?.explicaciones_dimensiones?.[key];
        const explicacion =
          explicacionValidada ??
          fallbackExplanation(key, label, scoreValue, z);
        const impacto =
          z.impacto_modelo_por_dimension?.[key] ??
          z.analisis_ia?.impacto_modelo_por_dimension?.[key];
        return { key, label, score: scoreValue, explicacion, impacto, isFallback: !explicacionValidada };
      })
      .filter((item) => item.score != null || item.explicacion.titular || item.explicacion.explicacion_corta);
  }, [z]);

  return (
    <div ref={panelRef} className={`${styles.panel} slideInRight`} style={{ width: DEFAULT_W }}>
      <div className={styles.resizeHandle} onMouseDown={onResizeStart}>
        <div className={styles.resizeGrip} />
      </div>

      <div className={styles.mobileBackBar}>
        <button className={styles.mobileBackBtn} onClick={onClose} aria-label="Volver al mapa">
          <svg width="16" height="16" viewBox="0 0 18 18" fill="none">
            <path d="M11 4L6 9l5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          Volver
        </button>
      </div>

      <div className={styles.header}>
        <div className={styles.headerTop}>
          <div>
            <p className={styles.eyebrow}>Análisis del local</p>
            <h2 className={styles.zoneName}>{z?.nombre ?? zona.nombre}</h2>
            <p className={styles.zoneSub}>
              {z?.barrio ?? zona.barrio} · {z?.distrito ?? zona.distrito}
              {z?.direccion ? ` · ${z.direccion}` : ""}
            </p>
          </div>
          <button className={styles.closeBtn} onClick={onClose} title="Cerrar panel">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        <div className={styles.scoreHero}>
          <div className={styles.scoreCard}>
            <div className={styles.scoreNumber}>{Math.round(score)}</div>
            <div className={styles.scoreMeta}>
              <span className={`${styles.scoreBadge} ${scoreTone.className}`}>{scoreTone.label}</span>
              <span className={styles.scoreCaption}>Puntuación global</span>
            </div>
          </div>

          <div className={styles.heroFacts}>
            <div className={styles.heroFact}>
              <span className={styles.heroFactLabel}>Supervivencia</span>
              <strong>{formatPercent(z?.probabilidad_supervivencia, 0)}</strong>
            </div>
            <div className={styles.heroFact}>
              <span className={styles.heroFactLabel}>Alquiler</span>
              <strong>{formatCurrency(z?.alquiler_mensual)}</strong>
            </div>
            <div className={styles.heroFact}>
              <span className={styles.heroFactLabel}>Superficie</span>
              <strong>{z?.m2 != null ? `${formatNumber(z.m2, 0)} m²` : "—"}</strong>
            </div>
          </div>
        </div>
      </div>

      <div className={styles.tabs}>
        {(["detalles", "competencia", "transporte", "financiero", "legal"] as Tab[]).map((item) => (
          <button
            key={item}
            className={`${styles.tab} ${tab === item ? styles.tabActive : ""}`}
            onClick={() => setTab(item)}
          >
            {item === "detalles" && "Detalles"}
            {item === "competencia" && "Competencia"}
            {item === "transporte" && "Transporte"}
            {item === "financiero" && "Financiero"}
            {item === "legal" && "Legal"}
          </button>
        ))}
      </div>

      <div className={styles.content}>
        {tab === "detalles" && (
          <div className={styles.tabPane}>
            {loading ? (
              <SkeletonBlock />
            ) : !z ? (
              <div className={styles.emptyState}>No se ha podido cargar el detalle completo del local.</div>
            ) : (
              <>
                <section className={styles.section}>
                  <div className={styles.sectionHeader}>
                    <div>
                      <p className={styles.sectionKicker}>Por qué este local</p>
                      <h3 className={styles.sectionTitle}>La recomendación en una frase</h3>
                    </div>
                  </div>
                  <div className={styles.summaryCard}>
                    <p className={styles.summaryText}>{summaryText}</p>
                    <div className={styles.metricGrid}>
                      {metrics.map((metric) => (
                        <div key={metric.label} className={styles.metricCard}>
                          <span className={styles.metricLabel}>{metric.label}</span>
                          <strong className={styles.metricValue}>{metric.value}</strong>
                          <span className={styles.metricNote}>{metric.note}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </section>

                <section className={styles.section}>
                  <div className={styles.sectionHeader}>
                    <div>
                      <p className={styles.sectionKicker}>Visión rápida</p>
                      <h3 className={styles.sectionTitle}>Dónde destaca y dónde exige más</h3>
                    </div>
                  </div>
                  <div className={styles.sectionCard}>
                    {z.scores_dimensiones && (
                      <ScoreBars
                        scores={z.scores_dimensiones}
                        onDimensionClick={(dim) => setActiveDim(dim)}
                      />
                    )}
                    <p className={styles.scoreBarsHint}>
                      Pulsa una dimensión para ver los ingredientes concretos
                      (landmarks, hoteles, venues…).
                    </p>
                  </div>
                </section>

                <section className={styles.section}>
                  <div className={styles.sectionHeader}>
                    <div>
                      <p className={styles.sectionKicker}>DAFO</p>
                      <h3 className={styles.sectionTitle}>Lectura estratégica del local</h3>
                    </div>
                  </div>
                  <div className={styles.dafoGrid}>
                    {Object.entries(dafo).map(([title, items]) => (
                      <div key={title} className={styles.dafoCard}>
                        <h4 className={styles.dafoTitle}>{title}</h4>
                        <ul className={styles.cleanList}>
                          {items.map((item, index) => (
                            <li key={`${title}-${index}`}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </section>

                <section className={styles.section}>
                  <div className={styles.sectionHeader}>
                    <div>
                      <p className={styles.sectionKicker}>Dimensión por dimensión</p>
                      <h3 className={styles.sectionTitle}>Qué ha usado el modelo para llegar a la puntuación</h3>
                    </div>
                  </div>
                  <div className={styles.dimensionStack}>
                    {dimensions.map(({ key, label, score: scoreValue, explicacion, impacto, isFallback }) => {
                      const tone = getTone(scoreValue);
                      return (
                        <article key={key} className={styles.dimensionCard}>
                          <div className={styles.dimensionHeader}>
                            <div>
                              <h4 className={styles.dimensionTitle}>{label}</h4>
                              <p className={styles.dimensionSubtitle}>{explicacion.titular || explicacion.explicacion_corta}</p>
                            </div>
                            <div className={styles.dimensionScoreWrap}>
                              <span className={`${styles.scoreBadge} ${tone.className}`}>{scoreValue != null ? Math.round(scoreValue) : "—"}</span>
                            </div>
                          </div>

                          {explicacion.explicacion_corta && (
                            <p className={styles.dimensionBody}>{explicacion.explicacion_corta}</p>
                          )}

                          <div className={styles.dimensionGrid}>
                            <div className={styles.dimensionBlock}>
                              <span className={styles.blockLabel}>Lo que suma</span>
                              <ul className={styles.cleanList}>
                                {(explicacion.porque_sube.length ? explicacion.porque_sube : [isFallback ? "Sin drivers positivos validados todavía." : "No hay factores positivos destacados en el detalle actual."]).map((item, index) => (
                                  <li key={`up-${key}-${index}`}>{item}</li>
                                ))}
                              </ul>
                            </div>
                            <div className={styles.dimensionBlock}>
                              <span className={styles.blockLabel}>Lo que frena</span>
                              <ul className={styles.cleanList}>
                                {(explicacion.porque_baja.length ? explicacion.porque_baja : [isFallback ? "Sin drivers negativos validados todavía." : "No aparece una fricción clara en esta dimensión."]).map((item, index) => (
                                  <li key={`down-${key}-${index}`}>{item}</li>
                                ))}
                              </ul>
                            </div>
                          </div>

                          {(explicacion.hechos_clave.length > 0 || impacto) && (
                            <div className={styles.dimensionMeta}>
                              {explicacion.hechos_clave.length > 0 && (
                                <div className={styles.dimensionMetaBlock}>
                                  <span className={styles.blockLabel}>Hechos clave</span>
                                  <div className={styles.factPills}>
                                    {explicacion.hechos_clave.map((fact, index) => (
                                      <span key={`fact-${key}-${index}`} className={styles.factPill}>{fact}</span>
                                    ))}
                                  </div>
                                </div>
                              )}

                              <div className={styles.dimensionMetaBlock}>
                                <span className={styles.blockLabel}>Impacto del modelo</span>
                                <p className={styles.modelImpact}>{explicacion.impacto_modelo || impactSummary(impacto)}</p>
                                {impacto?.top_features?.length ? (
                                  <ul className={styles.featureList}>
                                    {impacto.top_features.slice(0, 3).map((feature, index) => (
                                      <li key={`feature-${key}-${index}`}>
                                        <strong>{feature.feature ?? "Factor"}</strong>
                                        {feature.descripcion ? ` · ${feature.descripcion}` : ""}
                                      </li>
                                    ))}
                                  </ul>
                                ) : null}
                              </div>
                            </div>
                          )}

                          <div className={styles.dimensionFooter}>
                            <span className={styles.footerMeta}>{confidenceLabel(explicacion.confianza)}</span>
                            {explicacion.fuentes.length > 0 && (
                              <div className={styles.sourceList}>
                                {explicacion.fuentes.map((source, index) => (
                                  <span key={`source-${key}-${index}`} className={styles.sourceTag}>{source}</span>
                                ))}
                              </div>
                            )}
                          </div>
                        </article>
                      );
                    })}
                  </div>
                </section>
              </>
            )}
          </div>
        )}

        {tab === "competencia" && (
          <div className={styles.tabPane}>
            {competenciaError ? (
              <div className={styles.emptyState}>{competenciaError}</div>
            ) : (
              <CompetenciaPanel competencia={competencia} loading={loadingComp} zona={zona} />
            )}
          </div>
        )}

        {tab === "transporte" && (
          <div className={styles.tabPane}>
            <TransportePanel
              zonaId={zona.zona_id}
              fallbackLineas={z?.num_lineas_transporte ?? undefined}
              fallbackParadas={z?.num_paradas_transporte ?? undefined}
            />
          </div>
        )}

        {tab === "financiero" && (
          <FinancialPanel
            financiero={financiero}
            loading={loadingFin}
            zonaId={zona.zona_id}
            sessionId={sessionId}
            onUpdate={setFinanciero}
          />
        )}

        {tab === "legal" && <LegalPanel zona={zona} sessionId={sessionId} />}
      </div>

      <DimensionDrawer
        zona_id={zona.zona_id}
        session_id={sessionId}
        dimension={activeDim}
        onClose={() => setActiveDim(null)}
      />
    </div>
  );
}
