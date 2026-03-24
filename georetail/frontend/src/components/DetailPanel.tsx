"use client";

import { useState, useCallback } from "react";
import type { ZonaPreview, LocalDetalleResponse, FinancieroResponse } from "@/lib/types";
import { api } from "@/lib/api";
import FinancialPanel from "./FinancialPanel";
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
  const glow  = score >= 75 ? "rgba(16,185,129,0.4)" : score >= 50 ? "rgba(245,158,11,0.4)" : "rgba(239,68,68,0.4)";
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
      <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="5"/>
      <circle
        cx={size/2} cy={size/2} r={r} fill="none"
        stroke={color} strokeWidth="5"
        strokeDasharray={`${fill} ${circ - fill}`}
        strokeLinecap="round"
        transform={`rotate(-90 ${size/2} ${size/2})`}
        style={{ filter: `drop-shadow(0 0 6px ${glow})`, transition: "stroke-dasharray 0.6s cubic-bezier(0.16,1,0.3,1)" }}
      />
      <text x={size/2} y={size/2 - 3} textAnchor="middle" fontSize="16" fontWeight="800" fill={color}>
        {Math.round(score)}
      </text>
      <text x={size/2} y={size/2 + 12} textAnchor="middle" fontSize="9" fill="rgba(255,255,255,0.4)" fontWeight="500">
        / 100
      </text>
    </svg>
  );
}

function Skeleton({ h = 16, w = "100%" }: { h?: number; w?: string }) {
  return <div className="skeleton" style={{ height: h, width: w, borderRadius: 6, marginBottom: 8 }} />;
}

export default function DetailPanel({ zona, detalle, loading, sessionId, onClose }: Props) {
  const [tab, setTab] = useState<Tab>("analisis");
  const [financiero, setFinanciero] = useState<FinancieroResponse | null>(null);
  const [loadingFin, setLoadingFin] = useState(false);

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
    <div className={`${styles.panel} slideInRight`}>
      {/* ── Mobile back bar (only visible on small screens) ── */}
      <div className={styles.mobileBackBar}>
        <button className={styles.mobileBackBtn} onClick={onClose} aria-label="Volver al mapa">
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
            <path d="M11 4L6 9l5 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Volver al mapa
        </button>
        <span className={styles.mobileBackLabel}>{z?.nombre ?? zona.nombre}</span>
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
        <button className={styles.closeBtn} onClick={onClose} title="Cerrar panel">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
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

                {/* Score bars (from full detail) */}
                {z?.scores_dimensiones && (
                  <section className={styles.section}>
                    <h3 className={styles.sectionTitle}>
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor">
                        <rect x="1" y="7" width="2" height="4" rx="1"/>
                        <rect x="5" y="4" width="2" height="7" rx="1"/>
                        <rect x="9" y="1" width="2" height="10" rx="1"/>
                      </svg>
                      Puntuaciones por dimensión
                    </h3>
                    <ScoreBars scores={z.scores_dimensiones} />
                  </section>
                )}

                {/* AI Analysis */}
                {z?.analisis_ia && (
                  <section className={styles.section}>
                    <h3 className={styles.sectionTitle}>
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                        <circle cx="6" cy="6" r="5" stroke="currentColor" strokeWidth="1.5"/>
                        <path d="M4 5c0-1.1.9-2 2-2s2 .9 2 2c0 .8-.5 1.5-1.2 1.8L6 9" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round"/>
                        <circle cx="6" cy="10.5" r=".5" fill="currentColor"/>
                      </svg>
                      Análisis IA
                    </h3>
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
                  </section>
                )}

                {/* Flujo peatonal */}
                {z?.flujo_peatonal_dia && (
                  <section className={styles.section}>
                    <h3 className={styles.sectionTitle}>
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                        <circle cx="6" cy="3" r="1.5" stroke="currentColor" strokeWidth="1.2"/>
                        <path d="M4 6c0-1.1.9-2 2-2s2 .9 2 2v3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                      </svg>
                      Flujo peatonal
                    </h3>
                    <div className={styles.flujoGrid}>
                      {[
                        { label: "Mañana", val: z.flujo_peatonal_dia.manana, icon: "🌅" },
                        { label: "Tarde",  val: z.flujo_peatonal_dia.tarde,  icon: "☀️" },
                        { label: "Noche",  val: z.flujo_peatonal_dia.noche,  icon: "🌙" },
                      ].map(({ label, val, icon }) => (
                        <div key={label} className={styles.flujoItem}>
                          <span className={styles.flujoIcon}>{icon}</span>
                          <span className={styles.flujoVal}>{(val ?? 0).toLocaleString()}</span>
                          <span className={styles.flujoLabel}>{label}</span>
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                {/* Competitors */}
                {z?.competidores_cercanos && z.competidores_cercanos.length > 0 && (
                  <section className={styles.section}>
                    <h3 className={styles.sectionTitle}>
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                        <circle cx="4.5" cy="4" r="2" stroke="currentColor" strokeWidth="1.2"/>
                        <circle cx="8.5" cy="4" r="2" stroke="currentColor" strokeWidth="1.2"/>
                        <path d="M1 10c0-1.7 1.6-3 3.5-3M7 10c0-1.7 1.6-3 3.5-3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                      </svg>
                      Competidores cercanos
                    </h3>
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
                  </section>
                )}

                {/* Alertas */}
                {z?.alertas && z.alertas.length > 0 && (
                  <section className={styles.section}>
                    <h3 className={styles.sectionTitle}>
                      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                        <path d="M6 1l5 9H1L6 1z" stroke="currentColor" strokeWidth="1.2"/>
                        <path d="M6 5v2M6 8.5h.01" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                      </svg>
                      Alertas
                    </h3>
                    {z.alertas.map((a, i) => (
                      <div key={i} className={styles.alerta}>
                        <span className={styles.alertaTipo}>{a.tipo}</span>
                        <span className={styles.alertaTexto}>{a.texto}</span>
                      </div>
                    ))}
                  </section>
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
          <div className={styles.tabPane}>
            <div className={styles.legalCard}>
              <div className={styles.legalIcon}>
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                  <path d="M12 3L2 7l10 4 10-4-10-4zM2 17l10 4 10-4M2 12l10 4 10-4" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              </div>
              <p className={styles.legalText}>
                Consulta los requisitos legales completos para tu sector con el Ayuntamiento de Barcelona y verifica el Plan de Usos de la zona.
              </p>
              <div className={styles.legalInfo}>
                <div className={styles.legalRow}>
                  <span>Uso comercial</span>
                  <span className="badge badge-verde">Verificable</span>
                </div>
                <div className={styles.legalRow}>
                  <span>Actividades clasificadas</span>
                  <span className="badge badge-amarillo">Consultar IVU</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}