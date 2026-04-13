"use client";

import React, { useState, useCallback, useRef } from "react";
import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis, ResponsiveContainer,
} from "recharts";
import type { ZonaPreview, LocalDetalleResponse, FinancieroResponse } from "@/lib/types";
import type { ScoresDimensiones } from "@/lib/types";
import { api } from "@/lib/api";
import FinancialPanel from "./FinancialPanel";
import LegalPanel from "./LegalPanel";
import ScoreBars from "./ScoreBars";
import styles from "./DetailPanel.module.css";

interface Props {
  zona: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
  sessionId: string;
  onClose: () => void;
}

type Tab = "analisis" | "financiero" | "legal";

function ScoreRing({ score, size = 72 }: { score: number; size?: number }) {
  const r = (size / 2) - 6;
  const circ = 2 * Math.PI * r;
  const fill = circ * (score / 100);
  const color = score >= 75 ? "var(--green)" : score >= 50 ? "var(--yellow)" : "var(--red)";
  const glowRgb = score >= 75 ? "16,185,129" : score >= 50 ? "245,158,11" : "239,68,68";
  const filterId = `score-glow-${size}-${Math.round(score)}`;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} style={{ overflow: "visible" }}>
      <defs>
        <filter id={filterId} x="-40%" y="-40%" width="180%" height="180%">
          <feGaussianBlur in="SourceAlpha" stdDeviation="3" result="blur"/>
          <feFlood floodColor={`rgb(${glowRgb})`} floodOpacity="0.3" result="glowColor"/>
          <feComposite in="glowColor" in2="blur" operator="in" result="glow"/>
          <feMerge>
            <feMergeNode in="glow"/>
            <feMergeNode in="SourceGraphic"/>
          </feMerge>
        </filter>
      </defs>
      <circle cx={size/2} cy={size/2} r={r} fill="none" style={{ stroke: "var(--surface-3)" }} strokeWidth="5"/>
      <circle
        cx={size/2} cy={size/2} r={r} fill="none"
        stroke={color} strokeWidth="5"
        strokeDasharray={`${fill} ${circ - fill}`}
        strokeLinecap="round"
        transform={`rotate(-90 ${size/2} ${size/2})`}
        filter={`url(#${filterId})`}
        style={{ transition: "stroke-dasharray 0.6s cubic-bezier(0.16,1,0.3,1)" }}
      />
      <text x={size/2} y={size/2 - 3} textAnchor="middle" fontSize="16" fontWeight="800" fill={color}>
        {Math.round(score)}
      </text>
      <text x={size/2} y={size/2 + 12} textAnchor="middle" fontSize="9" style={{ fill: "var(--text-muted)" }} fontWeight="500">
        / 100
      </text>
    </svg>
  );
}

const RADAR_DIMS: { key: keyof ScoresDimensiones; label: string }[] = [
  { key: "flujo_peatonal",    label: "Flujo"    },
  { key: "demografia",        label: "Demog."   },
  { key: "competencia",       label: "Compet."  },
  { key: "precio_alquiler",   label: "Alquiler" },
  { key: "transporte",        label: "Transp."  },
  { key: "seguridad",         label: "Segur."   },
  { key: "turismo",           label: "Turismo"  },
  { key: "entorno_comercial", label: "Entorno"  },
];

function ScoreRadar({ scores }: { scores: ScoresDimensiones }) {
  const data = RADAR_DIMS
    .filter(d => scores[d.key] != null)
    .map(d => ({ dim: d.label, value: Math.round(scores[d.key] as number) }));
  return (
    <div className={styles.radarWrap}>
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart data={data} margin={{ top: 20, right: 48, bottom: 20, left: 48 }}>
          <PolarGrid stroke="var(--border)" />
          <PolarAngleAxis
            dataKey="dim"
            tick={{ fontSize: 11, fill: "var(--text-muted)", fontWeight: 600 }}
          />
          <Radar
            dataKey="value"
            stroke="var(--accent)"
            fill="var(--accent)"
            fillOpacity={0.18}
            strokeWidth={2}
            dot={{ fill: "var(--accent)", r: 3 }}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}

function Skeleton({ h = 16, w = "100%" }: { h?: number; w?: string }) {
  return <div className="skeleton" style={{ height: h, width: w, borderRadius: 6, marginBottom: 8 }} />;
}

const MIN_W = 340;
const MAX_W = 800;
const DEFAULT_W = 440;

export default function DetailPanel({ zona, detalle, loading, sessionId, onClose }: Props) {
  const [tab, setTab] = useState<Tab>("analisis");
  const [financiero, setFinanciero] = useState<FinancieroResponse | null>(null);
  const [loadingFin, setLoadingFin] = useState(false);
  const [scoreView, setScoreView] = useState<"bars" | "radar">("bars");
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    scores: true, analisis: true, flujo: false, competidores: false, alertas: true,
  });
  const toggleSection = (key: string) =>
    setOpenSections((prev: Record<string, boolean>) => ({ ...prev, [key]: !prev[key] }));

  const [expanded, setExpanded] = useState(false);

  const panelRef   = useRef<HTMLDivElement>(null);
  const dragState  = useRef({ active: false, startX: 0, startW: DEFAULT_W });

  const toggleExpand = () => {
    setExpanded((prev) => {
      const next = !prev;
      if (panelRef.current) {
        panelRef.current.style.width = `${next ? MAX_W : DEFAULT_W}px`;
      }
      return next;
    });
  };

  const onResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startW = panelRef.current?.offsetWidth ?? DEFAULT_W;
    dragState.current = { active: true, startX: e.clientX, startW };
    document.body.style.cursor     = "col-resize";
    document.body.style.userSelect = "none";

    const onMove = (ev: MouseEvent) => {
      if (!dragState.current.active || !panelRef.current) return;
      const delta  = dragState.current.startX - ev.clientX;
      const newW   = Math.min(MAX_W, Math.max(MIN_W, dragState.current.startW + delta));
      panelRef.current.style.width = `${newW}px`;
    };

    const onUp = () => {
      dragState.current.active       = false;
      document.body.style.cursor     = "";
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup",   onUp);
    };

    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup",   onUp);
  }, []);

  const loadFinanciero = useCallback(async () => {
    if (financiero || loadingFin) return;
    setLoadingFin(true);
    try {
      const data = await api.financiero(zona.zona_id, sessionId);
      setFinanciero(data);
    } catch (e) {
      console.error("Error financiero:", e);
    } finally {
      setLoadingFin(false);
    }
  }, [zona.zona_id, sessionId, financiero, loadingFin]);

  const handleTab = (t: Tab) => {
    setTab(t);
    if (t === "financiero") loadFinanciero();
  };

  // Use detalle data when available, fall back to ZonaPreview
  const z = detalle?.zona;
  const score = z?.score_global ?? zona.score_global ?? 0;
  const scoreClass = score >= 75 ? "verde" : score >= 50 ? "amarillo" : "rojo";
  const scoreLabel = score >= 75 ? "Alta viabilidad" : score >= 50 ? "Viabilidad media" : "Baja viabilidad";

  // Preview KPIs always available from ZonaPreview
  const alquiler = z?.alquiler_mensual ?? zona.alquiler_mensual;
  const m2       = z?.m2             ?? zona.m2;

  return (
    <div ref={panelRef} className={`${styles.panel} slideInRight`} style={{ width: DEFAULT_W }}>
      {/* ── Resize handle (desktop only) ── */}
      <div className={styles.resizeHandle} onMouseDown={onResizeStart}>
        <div className={styles.resizeGrip} />
      </div>
      {/* ── Mobile back bar (only visible on small screens) ── */}
      <div className={styles.mobileBackBar}>
        <button className={styles.mobileBackBtn} onClick={onClose} aria-label="Volver al mapa">
          <svg width="16" height="16" viewBox="0 0 18 18" fill="none">
            <path d="M11 4L6 9l5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Volver
        </button>
      </div>

      {/* ── Header ── */}
      <div className={styles.header}>
        <div className={styles.headerContent}>
          <div className={styles.scoreArea}>
            <ScoreRing score={score} />
          </div>
          <div className={styles.headerInfo}>
            <div className={styles.zoneName}>{z?.nombre ?? zona.nombre}</div>
            <div className={styles.zoneSub}>{z?.barrio ?? zona.barrio} · {z?.distrito ?? zona.distrito}</div>
            <span className={`badge badge-${scoreClass} ${styles.viabilidadBadge}`}>
              {scoreLabel}
            </span>
          </div>
        </div>
        <button className={styles.expandBtn} onClick={toggleExpand} title={expanded ? "Minimizar panel" : "Expandir panel"}>
          {expanded ? (
            <svg width="20" height="20" viewBox="0 0 16 16" fill="none">
              <path d="M1.5 8H5M5 8l-2-2M5 8l-2 2M14.5 8H11M11 8l2-2M11 8l2 2"
                stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          ) : (
            <svg width="20" height="20" viewBox="0 0 16 16" fill="none">
              <path d="M5 8H1.5M1.5 8l2-2M1.5 8l2 2M11 8H14.5M14.5 8l-2-2M14.5 8l-2 2"
                stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          )}
        </button>
        <button className={styles.closeBtn} onClick={onClose} title="Cerrar panel">
          <svg width="20" height="20" viewBox="0 0 16 16" fill="none">
            <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
          </svg>
        </button>
      </div>

      {/* ── Tabs ── */}
      <div className={styles.tabs}>
        {(["analisis", "financiero", "legal"] as Tab[]).map(t => (
          <button
            key={t}
            className={`${styles.tab} ${tab === t ? styles.tabActive : ""}`}
            onClick={() => handleTab(t)}
          >
            {t === "analisis"   && "Análisis"}
            {t === "financiero" && "Financiero"}
            {t === "legal"      && "Legal"}
          </button>
        ))}
      </div>

      {/* ── Content ── */}
      <div className={styles.content}>

        {/* ── Análisis tab ── */}
        {tab === "analisis" && (
          <div className={styles.tabPane}>
            {loading ? (
              /* Skeleton while loading */
              <div className={styles.skeletonWrap}>
                <Skeleton h={80} />
                <Skeleton h={14} w="60%" />
                <Skeleton h={14} />
                <Skeleton h={14} w="80%" />
                <Skeleton h={120} />
              </div>
            ) : (
              <>
                {/* KPIs — always shown from preview data */}
                <div className={styles.kpiGrid}>
                  {alquiler && (
                    <div className={styles.kpi}>
                      <span className={styles.kpiVal}>{alquiler.toLocaleString("es-ES")} €</span>
                      <span className={styles.kpiLabel}>Alquiler / mes</span>
                    </div>
                  )}
                  {m2 && (
                    <div className={styles.kpi}>
                      <span className={styles.kpiVal}>{m2} m²</span>
                      <span className={styles.kpiLabel}>Superficie</span>
                    </div>
                  )}
                  {z?.probabilidad_supervivencia && (
                    <div className={styles.kpi}>
                      <span className={styles.kpiVal} style={{ color: "var(--green)" }}>
                        {Math.round(z.probabilidad_supervivencia * 100)}%
                      </span>
                      <span className={styles.kpiLabel}>Supervivencia 3a</span>
                    </div>
                  )}
                  {z?.num_negocios_activos != null && (
                    <div className={styles.kpi}>
                      <span className={styles.kpiVal}>{z.num_negocios_activos}</span>
                      <span className={styles.kpiLabel}>Negocios activos</span>
                    </div>
                  )}
                </div>

                {/* Score bars / radar (from full detail) */}
                {z?.scores_dimensiones && (
                  <div className={styles.accordion}>
                    <button className={styles.accordionHeader} onClick={() => toggleSection("scores")}>
                      <span className={styles.accordionTitleWrap}>
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                          <rect x="1" y="7" width="2" height="4" rx="1"/>
                          <rect x="5" y="4" width="2" height="7" rx="1"/>
                          <rect x="9" y="1" width="2" height="10" rx="1"/>
                        </svg>
                        <span className={styles.accordionTitle}>Puntuaciones por dimensión</span>
                      </span>
                      <svg width="10" height="10" viewBox="0 0 10 10" className={styles.accordionChevron}
                        style={{ transform: openSections.scores ? "rotate(180deg)" : "none" }}>
                        <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
                      </svg>
                    </button>
                    {openSections.scores && (
                      <div className={styles.accordionBody}>
                        <div className={styles.viewToggleRow}>
                          <button
                            className={`${styles.viewBtn} ${scoreView === "bars" ? styles.viewBtnActive : ""}`}
                            onClick={() => setScoreView("bars")}
                            title="Barras"
                          >
                            <svg width="13" height="13" viewBox="0 0 13 13" fill="currentColor">
                              <rect x="1" y="7" width="2.5" height="5" rx="0.8"/>
                              <rect x="5" y="4" width="2.5" height="8" rx="0.8"/>
                              <rect x="9" y="1" width="2.5" height="11" rx="0.8"/>
                            </svg>
                          </button>
                          <button
                            className={`${styles.viewBtn} ${scoreView === "radar" ? styles.viewBtnActive : ""}`}
                            onClick={() => setScoreView("radar")}
                            title="Radar"
                          >
                            <svg width="13" height="13" viewBox="0 0 13 13" fill="none" stroke="currentColor" strokeWidth="1.3">
                              <polygon points="6.5,1 12,4.5 12,8.5 6.5,12 1,8.5 1,4.5" />
                              <polygon points="6.5,3.5 9.5,5.5 9.5,7.5 6.5,9.5 3.5,7.5 3.5,5.5" />
                              <line x1="6.5" y1="1" x2="6.5" y2="12"/>
                              <line x1="1" y1="4.5" x2="12" y2="8.5"/>
                              <line x1="12" y1="4.5" x2="1" y2="8.5"/>
                            </svg>
                          </button>
                        </div>
                        {scoreView === "bars"
                          ? <ScoreBars scores={z.scores_dimensiones} />
                          : <ScoreRadar scores={z.scores_dimensiones} />
                        }
                      </div>
                    )}
                  </div>
                )}

                {/* AI Analysis */}
                {z?.analisis_ia && (
                  <div className={styles.accordion}>
                    <button className={styles.accordionHeader} onClick={() => toggleSection("analisis")}>
                      <span className={styles.accordionTitleWrap}>
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                          <circle cx="6" cy="6" r="5" stroke="currentColor" strokeWidth="1.5"/>
                          <path d="M4 5c0-1.1.9-2 2-2s2 .9 2 2c0 .8-.5 1.5-1.2 1.8L6 9" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
                          <circle cx="6" cy="10.5" r=".5" fill="currentColor"/>
                        </svg>
                        <span className={styles.accordionTitle}>Análisis IA</span>
                      </span>
                      <svg width="10" height="10" viewBox="0 0 10 10" className={styles.accordionChevron}
                        style={{ transform: openSections.analisis ? "rotate(180deg)" : "none" }}>
                        <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
                      </svg>
                    </button>
                    {openSections.analisis && (
                      <div className={styles.accordionBody}>
                        {z.analisis_ia.resumen && (
                          <p className={styles.analisisText}>{z.analisis_ia.resumen}</p>
                        )}
                        {(z.analisis_ia.puntos_fuertes?.length > 0 || z.analisis_ia.puntos_debiles?.length > 0) && (
                          <div className={styles.prosConsGrid}>
                            {z.analisis_ia.puntos_fuertes?.length > 0 && (
                              <div className={styles.prosBox}>
                                <div className={styles.prosHeader}>
                                  <span className={styles.prosIcon}>✓</span> Puntos fuertes
                                </div>
                                <ul className={styles.dotList}>
                                  {z.analisis_ia.puntos_fuertes.map((p, i) => <li key={i}>{p}</li>)}
                                </ul>
                              </div>
                            )}
                            {z.analisis_ia.puntos_debiles?.length > 0 && (
                              <div className={styles.consBox}>
                                <div className={styles.consHeader}>
                                  <span className={styles.consIcon}>✗</span> Puntos débiles
                                </div>
                                <ul className={styles.dotList}>
                                  {z.analisis_ia.puntos_debiles.map((p, i) => <li key={i}>{p}</li>)}
                                </ul>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {/* Flujo peatonal */}
                {z?.flujo_peatonal_dia && (
                  <div className={styles.accordion}>
                    <button className={styles.accordionHeader} onClick={() => toggleSection("flujo")}>
                      <span className={styles.accordionTitleWrap}>
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                          <circle cx="6" cy="3" r="1.5" stroke="currentColor" strokeWidth="1.2"/>
                          <path d="M4 6c0-1.1.9-2 2-2s2 .9 2 2v3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                        </svg>
                        <span className={styles.accordionTitle}>Flujo peatonal</span>
                      </span>
                      <svg width="10" height="10" viewBox="0 0 10 10" className={styles.accordionChevron}
                        style={{ transform: openSections.flujo ? "rotate(180deg)" : "none" }}>
                        <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
                      </svg>
                    </button>
                    {openSections.flujo && (
                      <div className={styles.accordionBody}>
                        <div className={styles.flujoGrid}>
                          {([
                            {
                              label: "Mañana",
                              val: z.flujo_peatonal_dia.manana,
                              icon: (
                                <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
                                  <circle cx="10" cy="10" r="3.5" fill="currentColor" fillOpacity="0.15"/>
                                  <path d="M10 3v1.5M10 15.5V17M3 10h1.5M15.5 10H17M5.1 5.1l1.1 1.1M13.8 13.8l1.1 1.1M5.1 14.9l1.1-1.1M13.8 6.2l1.1-1.1"/>
                                  <path d="M4 17h12" strokeWidth="1.2"/>
                                </svg>
                              ),
                            },
                            {
                              label: "Tarde",
                              val: z.flujo_peatonal_dia.tarde,
                              icon: (
                                <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
                                  <circle cx="10" cy="10" r="4.5" fill="currentColor" fillOpacity="0.15"/>
                                  <path d="M10 2.5V4M10 16v1.5M2.5 10H4M16 10h1.5M4.6 4.6l1.1 1.1M14.3 14.3l1.1 1.1M4.6 15.4l1.1-1.1M14.3 5.7l1.1-1.1"/>
                                </svg>
                              ),
                            },
                            {
                              label: "Noche",
                              val: z.flujo_peatonal_dia.noche,
                              icon: (
                                <svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
                                  <path d="M15 13.5A7 7 0 0 1 6.5 5a7.5 7.5 0 1 0 8.5 8.5z" fill="currentColor" fillOpacity="0.15"/>
                                  <circle cx="15" cy="5" r="1" fill="currentColor" stroke="none" opacity="0.6"/>
                                  <circle cx="17" cy="9" r="0.6" fill="currentColor" stroke="none" opacity="0.4"/>
                                </svg>
                              ),
                            },
                          ] as { label: string; val: number | undefined; icon: React.ReactNode }[]).map(({ label, val, icon }) => (
                            <div key={label} className={styles.flujoItem}>
                              <span className={styles.flujoIcon}>{icon}</span>
                              <span className={styles.flujoVal}>{(val ?? 0).toLocaleString()}</span>
                              <span className={styles.flujoLabel}>{label}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Competitors */}
                {z?.competidores_cercanos && z.competidores_cercanos.length > 0 && (
                  <div className={styles.accordion}>
                    <button className={styles.accordionHeader} onClick={() => toggleSection("competidores")}>
                      <span className={styles.accordionTitleWrap}>
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                          <circle cx="4.5" cy="4" r="2" stroke="currentColor" strokeWidth="1.2"/>
                          <circle cx="8.5" cy="4" r="2" stroke="currentColor" strokeWidth="1.2"/>
                          <path d="M1 10c0-1.7 1.6-3 3.5-3M7 10c0-1.7 1.6-3 3.5-3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                        </svg>
                        <span className={styles.accordionTitle}>Competidores cercanos</span>
                      </span>
                      <svg width="10" height="10" viewBox="0 0 10 10" className={styles.accordionChevron}
                        style={{ transform: openSections.competidores ? "rotate(180deg)" : "none" }}>
                        <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
                      </svg>
                    </button>
                    {openSections.competidores && (
                      <div className={styles.accordionBody}>
                        <div className={styles.competitors}>
                          {z.competidores_cercanos.slice(0, 6).map((c, i) => (
                            <div key={i} className={`${styles.competitor} ${c.es_competencia_directa ? styles.competitorDirect : ""}`}>
                              <div className={styles.competitorLeft}>
                                {c.es_competencia_directa && <span className={styles.directTag}>directo</span>}
                                <span className={styles.competitorName}>{c.nombre}</span>
                              </div>
                              <div className={styles.competitorRight}>
                                {c.rating    && <span className={styles.rating}>★ {c.rating.toFixed(1)}</span>}
                                {c.distancia_m && <span className={styles.distance}>{Math.round(c.distancia_m)}m</span>}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Alertas */}
                {z?.alertas && z.alertas.length > 0 && (
                  <div className={styles.accordion}>
                    <button className={styles.accordionHeader} onClick={() => toggleSection("alertas")}>
                      <span className={styles.accordionTitleWrap}>
                        <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                          <path d="M6 1l5 9H1L6 1z" stroke="currentColor" strokeWidth="1.2"/>
                          <path d="M6 5v2M6 8.5h.01" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                        </svg>
                        <span className={styles.accordionTitle}>Alertas</span>
                      </span>
                      <svg width="10" height="10" viewBox="0 0 10 10" className={styles.accordionChevron}
                        style={{ transform: openSections.alertas ? "rotate(180deg)" : "none" }}>
                        <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
                      </svg>
                    </button>
                    {openSections.alertas && (
                      <div className={styles.accordionBody}>
                        {z.alertas.map((a, i) => (
                          <div key={i} className={styles.alerta}>
                            <span className={styles.alertaTipo}>{a.tipo}</span>
                            <span className={styles.alertaTexto}>{a.texto}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}

                {/* If detalle failed but we have ZonaPreview data, show a note */}
                {!z && !loading && (
                  <div className={styles.fallbackNote}>
                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                      <circle cx="7" cy="7" r="6" stroke="currentColor" strokeWidth="1.3"/>
                      <path d="M7 4v3.5M7 9.5h.01" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
                    </svg>
                    <span>El análisis completo no está disponible. Los datos de puntuación están en la ficha.</span>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {/* ── Financiero tab ── */}
        {tab === "financiero" && (
          <FinancialPanel
            financiero={financiero}
            loading={loadingFin}
            zonaId={zona.zona_id}
            sessionId={sessionId}
            onUpdate={setFinanciero}
          />
        )}

        {/* ── Legal tab ── */}
        {tab === "legal" && (
          <LegalPanel zona={zona} sessionId={sessionId} />
        )}
      </div>
    </div>
  );
}