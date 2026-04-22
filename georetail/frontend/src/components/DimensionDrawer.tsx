"use client";

import { useEffect, useState } from "react";
import { api } from "@/lib/api";
import type { DimensionTurismoDetalle, ScoresDimensiones } from "@/lib/types";
import styles from "./DimensionDrawer.module.css";

interface Props {
  zona_id: string;
  session_id: string;
  dimension: keyof ScoresDimensiones | null;
  onClose: () => void;
}

/**
 * Drawer lateral que se abre al pulsar una barra de score en el DetailPanel.
 * Pide los "ingredientes concretos" (landmarks con nombre, hoteles, venues…)
 * al endpoint lazy `/api/dimension/{dimension}/{zona_id}` y los renderiza como
 * listas legibles tipo "Sagrada Família a 254 m".
 *
 * Estado inicial: sólo implementada la dimensión "turismo". El resto muestra
 * un placeholder — se irán añadiendo a medida que los endpoints backend
 * estén listos.
 */
export default function DimensionDrawer({ zona_id, session_id, dimension, onClose }: Props) {
  const [turismo, setTurismo] = useState<DimensionTurismoDetalle | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Cerrar con Escape.
  useEffect(() => {
    if (!dimension) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [dimension, onClose]);

  // Cargar datos cuando cambia la dimensión activa.
  useEffect(() => {
    let cancelled = false;
    setError(null);
    setTurismo(null);

    if (dimension === "turismo") {
      setLoading(true);
      api
        .dimensionTurismo(zona_id, session_id)
        .then((r) => {
          if (!cancelled) setTurismo(r);
        })
        .catch((e) => {
          if (!cancelled) setError(String((e as Error).message || e));
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }
    return () => {
      cancelled = true;
    };
  }, [dimension, zona_id, session_id]);

  if (!dimension) return null;

  return (
    <div className={styles.backdrop} onClick={onClose}>
      <aside
        className={styles.drawer}
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        <header className={styles.header}>
          <div>
            <p className={styles.kicker}>Detalle · dimensión</p>
            <h3 className={styles.title}>{labelDimension(dimension)}</h3>
          </div>
          <button
            type="button"
            className={styles.close}
            onClick={onClose}
            aria-label="Cerrar"
          >
            ×
          </button>
        </header>

        <div className={styles.body}>
          {loading && <p className={styles.muted}>Cargando datos reales…</p>}
          {error && <p className={styles.error}>No se pudo cargar: {error}</p>}

          {dimension === "turismo" && turismo && <TurismoBody data={turismo} />}

          {dimension !== "turismo" && !loading && !error && (
            <p className={styles.muted}>
              El drill-down detallado para <strong>{labelDimension(dimension)}</strong> aún
              no está disponible. De momento solo se muestran los ingredientes
              concretos de Turismo. Próximamente: Demografía, Competencia,
              Transporte, Entorno y Seguridad.
            </p>
          )}
        </div>
      </aside>
    </div>
  );
}

function labelDimension(d: keyof ScoresDimensiones): string {
  const L: Record<string, string> = {
    flujo_peatonal: "Flujo peatonal",
    demografia: "Demografía",
    competencia: "Competencia",
    precio_alquiler: "Precio alquiler",
    transporte: "Transporte",
    seguridad: "Seguridad",
    turismo: "Turismo",
    dinamismo: "Dinamismo comercial",
  };
  return L[d as string] ?? (d as string);
}

// ── Cuerpo de Turismo ────────────────────────────────────────────────────────

function TurismoBody({ data }: { data: DimensionTurismoDetalle }) {
  const { resumen, hechos, landmarks, hoteles, venues, radio_m, score_turismo } = data;

  return (
    <>
      {score_turismo != null && (
        <div className={styles.scoreBadge}>
          <span className={styles.scoreNum}>{Math.round(score_turismo)}</span>
          <span className={styles.scoreSub}>Score turismo (0–100)</span>
        </div>
      )}

      <p className={styles.resumen}>{resumen}</p>

      <div className={styles.kpis}>
        <KPI label="Landmarks" value={hechos.landmarks_count} />
        <KPI label="Hoteles" value={hechos.hoteles_count} />
        <KPI label="Venues cultura" value={hechos.venues_count} />
        <KPI
          label="Airbnb 500 m"
          value={hechos.airbnb_density_500m ?? "—"}
          muted={hechos.airbnb_density_500m == null}
        />
        <KPI
          label="Playa"
          value={hechos.dist_playa_m != null ? `${hechos.dist_playa_m} m` : "—"}
        />
      </div>

      <Section title="Landmarks turísticos">
        {landmarks.length === 0 ? (
          <EmptyMsg>No hay landmarks con Wikidata en el radio.</EmptyMsg>
        ) : (
          <ul className={styles.list}>
            {landmarks.map((l, i) => (
              <li key={i} className={styles.listItem}>
                <span className={styles.itemName}>
                  {l.wikidata_id ? (
                    <a
                      href={`https://www.wikidata.org/wiki/${l.wikidata_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {l.nombre} ↗
                    </a>
                  ) : (
                    l.nombre
                  )}
                </span>
                <span className={styles.itemMeta}>{l.distancia_m} m</span>
              </li>
            ))}
          </ul>
        )}
      </Section>

      <Section title={`Hoteles y alojamientos (≤ ${radio_m} m)`}>
        {hoteles.length === 0 ? (
          <EmptyMsg>Sin hoteles registrados en el radio.</EmptyMsg>
        ) : (
          <ul className={styles.list}>
            {hoteles.map((h, i) => (
              <li key={i} className={styles.listItem}>
                <span className={styles.itemName}>
                  {h.nombre}
                  {h.estrellas ? <span className={styles.stars}> · {"★".repeat(h.estrellas)}</span> : null}
                  {h.rating != null ? (
                    <span className={styles.rating}> · ★ {h.rating.toFixed(1)}</span>
                  ) : null}
                </span>
                <span className={styles.itemMeta}>{h.distancia_m} m</span>
              </li>
            ))}
          </ul>
        )}
      </Section>

      <Section title="Venues culturales / musicales">
        {venues.length === 0 ? (
          <EmptyMsg>Sin teatros, museos o salas cercanos.</EmptyMsg>
        ) : (
          <ul className={styles.list}>
            {venues.map((v, i) => (
              <li key={i} className={styles.listItem}>
                <span className={styles.itemName}>
                  {v.nombre}
                  <span className={styles.itemTipo}> · {v.tipo}</span>
                </span>
                <span className={styles.itemMeta}>{v.distancia_m} m</span>
              </li>
            ))}
          </ul>
        )}
      </Section>
    </>
  );
}

// ── Primitivas de UI ─────────────────────────────────────────────────────────

function KPI({ label, value, muted }: { label: string; value: number | string; muted?: boolean }) {
  return (
    <div className={`${styles.kpi} ${muted ? styles.kpiMuted : ""}`}>
      <span className={styles.kpiVal}>{value}</span>
      <span className={styles.kpiLabel}>{label}</span>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className={styles.section}>
      <h4 className={styles.sectionTitle}>{title}</h4>
      {children}
    </section>
  );
}

function EmptyMsg({ children }: { children: React.ReactNode }) {
  return <p className={styles.muted}>{children}</p>;
}
