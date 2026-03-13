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

function scoreColor(score?: number | null): string {
  if (!score) return "amarillo";
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

export default function DetailPanel({ zona, detalle, loading, sessionId, onClose }: Props) {
  const [tab, setTab] = useState<"info" | "financiero" | "legal">("info");
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

  const handleTabChange = (t: "info" | "financiero" | "legal") => {
    setTab(t);
    if (t === "financiero") loadFinanciero();
  };

  const z = detalle?.zona;
  const score = z?.score_global ?? zona.score_global;
  const color = scoreColor(score);

  return (
    <div className={styles.panel}>
      {/* Header */}
      <div className={styles.header}>
        <div className={styles.headerInfo}>
          <div className={styles.zoneName}>{z?.nombre ?? zona.nombre}</div>
          <div className={styles.zoneSub}>{z?.barrio ?? zona.barrio} · {z?.distrito ?? zona.distrito}</div>
        </div>
        <div className={styles.headerRight}>
          {score !== undefined && score !== null && (
            <div className={`${styles.scoreBadge} badge badge-${color}`}>
              {Math.round(score)}
            </div>
          )}
          <button className={styles.closeBtn} onClick={onClose} title="Cerrar">✕</button>
        </div>
      </div>

      {/* Tabs */}
      <div className={styles.tabs}>
        <button className={`${styles.tab} ${tab === "info" ? styles.tabActive : ""}`} onClick={() => handleTabChange("info")}>Análisis</button>
        <button className={`${styles.tab} ${tab === "financiero" ? styles.tabActive : ""}`} onClick={() => handleTabChange("financiero")}>Financiero</button>
        <button className={`${styles.tab} ${tab === "legal" ? styles.tabActive : ""}`} onClick={() => handleTabChange("legal")}>Legal</button>
      </div>

      <div className={styles.content}>
        {loading && (
          <div className={styles.loadingState}><div className="spinner" /><span>Analizando zona...</span></div>
        )}

        {!loading && tab === "info" && (
          <div className={styles.tabContent}>
            {/* Métricas rápidas */}
            <div className={styles.metrics}>
              {(z?.alquiler_mensual ?? zona.alquiler_mensual) && (
                <div className={styles.metric}>
                  <span className={styles.metricVal}>{(z?.alquiler_mensual ?? zona.alquiler_mensual)!.toLocaleString("es-ES")} €</span>
                  <span className={styles.metricLabel}>Alquiler/mes</span>
                </div>
              )}
              {z?.m2 && (
                <div className={styles.metric}>
                  <span className={styles.metricVal}>{z.m2} m²</span>
                  <span className={styles.metricLabel}>Superficie</span>
                </div>
              )}
              {z?.probabilidad_supervivencia && (
                <div className={styles.metric}>
                  <span className={styles.metricVal}>{Math.round(z.probabilidad_supervivencia * 100)}%</span>
                  <span className={styles.metricLabel}>Supervivencia 3a</span>
                </div>
              )}
              {z?.num_negocios_activos !== undefined && (
                <div className={styles.metric}>
                  <span className={styles.metricVal}>{z.num_negocios_activos}</span>
                  <span className={styles.metricLabel}>Negocios activos</span>
                </div>
              )}
            </div>

            {/* Score por dimensiones */}
            {z?.scores_dimensiones && (
              <section>
                <h3 className={styles.sectionTitle}>Puntuaciones</h3>
                <ScoreBars scores={z.scores_dimensiones} />
              </section>
            )}

            {/* Análisis IA */}
            {z?.analisis_ia && (
              <section>
                <h3 className={styles.sectionTitle}>Análisis IA</h3>
                <p className={styles.analisisText}>{z.analisis_ia.resumen}</p>
                {z.analisis_ia.puntos_fuertes.length > 0 && (
                  <div className={styles.prosCons}>
                    <div>
                      <div className={styles.prosLabel}>✓ Puntos fuertes</div>
                      <ul className={styles.list}>{z.analisis_ia.puntos_fuertes.map((p, i) => <li key={i}>{p}</li>)}</ul>
                    </div>
                    {z.analisis_ia.puntos_debiles.length > 0 && (
                      <div>
                        <div className={styles.consLabel}>✗ Puntos débiles</div>
                        <ul className={styles.list}>{z.analisis_ia.puntos_debiles.map((p, i) => <li key={i}>{p}</li>)}</ul>
                      </div>
                    )}
                  </div>
                )}
              </section>
            )}

            {/* Competidores */}
            {z?.competidores_cercanos && z.competidores_cercanos.length > 0 && (
              <section>
                <h3 className={styles.sectionTitle}>Competidores cercanos</h3>
                <div className={styles.competidores}>
                  {z.competidores_cercanos.slice(0, 5).map((c, i) => (
                    <div key={i} className={styles.competidor}>
                      <div className={styles.competidorName}>
                        {c.es_competencia_directa && <span className={styles.directoBadge}>directo</span>}
                        {c.nombre}
                      </div>
                      <div className={styles.competidorMeta}>
                        {c.distancia_m && <span>{Math.round(c.distancia_m)}m</span>}
                        {c.rating && <span>★ {c.rating.toFixed(1)}</span>}
                      </div>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {/* Alertas */}
            {z?.alertas && z.alertas.length > 0 && (
              <section>
                <h3 className={styles.sectionTitle}>Alertas</h3>
                {z.alertas.map((a, i) => (
                  <div key={i} className={styles.alerta}>
                    <span className={styles.alertaTipo}>{a.tipo}</span>
                    <span>{a.texto}</span>
                  </div>
                ))}
              </section>
            )}

            {/* Flujo peatonal */}
            {z?.flujo_peatonal_dia && (
              <section>
                <h3 className={styles.sectionTitle}>Flujo peatonal</h3>
                <div className={styles.metrics}>
                  <div className={styles.metric}><span className={styles.metricVal}>{z.flujo_peatonal_dia.manana?.toLocaleString()}</span><span className={styles.metricLabel}>Mañana</span></div>
                  <div className={styles.metric}><span className={styles.metricVal}>{z.flujo_peatonal_dia.tarde?.toLocaleString()}</span><span className={styles.metricLabel}>Tarde</span></div>
                  <div className={styles.metric}><span className={styles.metricVal}>{z.flujo_peatonal_dia.noche?.toLocaleString()}</span><span className={styles.metricLabel}>Noche</span></div>
                </div>
              </section>
            )}
          </div>
        )}

        {!loading && tab === "financiero" && (
          <FinancialPanel
            financiero={financiero}
            loading={loadingFin}
            zonaId={zona.zona_id}
            sessionId={sessionId}
            onUpdate={setFinanciero}
          />
        )}

        {!loading && tab === "legal" && z?.analisis_ia && (
          <div className={styles.tabContent}>
            <div className={`${styles.legalViabilidad} badge badge-verde`}>
              {/* mostramos la sección legal si está en el detalle */}
              Consulta el panel de legal para más información
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
