"use client";

import React, { useState, useCallback, useEffect, useRef } from "react";
import type { LegalRoadmapResponse, FaseRoadmap, TramiteLegal } from "@/lib/types";
import type { ZonaPreview } from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./LegalPanel.module.css";

interface Props {
  zona: ZonaPreview;
  sessionId: string;
}

// ─── Skeleton ─────────────────────────────────────────────────────────────────

function Skeleton({ h = 16, w = "100%" }: { h?: number; w?: string }) {
  return (
    <div
      className="skeleton"
      style={{ height: h, width: w, borderRadius: 6, marginBottom: 8 }}
    />
  );
}

// ─── Fase colors ──────────────────────────────────────────────────────────────

const FASE_COLORS = [
  "var(--accent)",
  "#10B981",
  "#F59E0B",
  "#A78BFA",
  "#EC4899",
];

// ─── Tramite card ─────────────────────────────────────────────────────────────

function TramiteCard({ tramite, isLast }: { tramite: TramiteLegal; isLast: boolean }) {
  const [open, setOpen] = useState(false);

  return (
    <div className={`${styles.tramiteRow} ${open ? styles.tramiteRowOpen : ""}`}>
      {/* Left: timeline node + vertical connector */}
      <div className={styles.tramiteTimelineCol}>
        <div className={styles.tramiteNode}>
          <span className={styles.tramiteNodeNum}>{tramite.numero}</span>
        </div>
        {!isLast && <div className={styles.tramiteConnector} />}
      </div>

      {/* Right: collapsible card */}
      <div className={`${styles.tramiteContent} ${isLast ? styles.tramiteContentLast : ""}`}>
        <button className={styles.tramiteHeader} onClick={() => setOpen((o) => !o)}>
          <span className={styles.tramiteTitleBlock}>
            <span className={styles.tramiteTitulo}>{tramite.titulo}</span>
            {!open && (tramite.donde || tramite.tiempo_estimado) && (
              <span className={styles.tramiteSubtitle}>
                {tramite.donde}
                {tramite.donde && tramite.tiempo_estimado && " · "}
                {tramite.tiempo_estimado}
              </span>
            )}
          </span>
          <div className={styles.tramiteHeaderRight}>
            {tramite.enlace && !open && (
              <span className={styles.tramiteLinkBadge} aria-label="Enlace oficial disponible">
                <svg width="9" height="9" viewBox="0 0 9 9" fill="none">
                  <path d="M3.5 1.5H1.5a1 1 0 00-1 1v5a1 1 0 001 1h5a1 1 0 001-1V5.5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                  <path d="M5.5 1.5h2v2M7.5 1.5L4.5 4.5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                </svg>
                Enlace
              </span>
            )}
            {tramite.alerta && !open && (
              <span className={styles.tramiteAlertaBadge}>
                <svg width="9" height="9" viewBox="0 0 9 9" fill="none">
                  <path d="M4.5 1L8 7.5H1L4.5 1z" stroke="currentColor" strokeWidth="1.2"/>
                  <path d="M4.5 3.5v1.5M4.5 6.5h.01" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
                </svg>
              </span>
            )}
            <svg
              width="10"
              height="10"
              viewBox="0 0 10 10"
              className={styles.tramiteChevron}
              style={{ transform: open ? "rotate(180deg)" : "none" }}
            >
              <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round"/>
            </svg>
          </div>
        </button>

        {open && (
          <div className={styles.tramiteBody}>
            {tramite.alerta && (
              <div className={styles.tramiteAlerta}>
                <svg width="13" height="13" viewBox="0 0 13 13" fill="none" style={{ flexShrink: 0 }}>
                  <path d="M6.5 1.5l5.2 9H1.3l5.2-9z" stroke="var(--yellow)" strokeWidth="1.2" />
                  <path d="M6.5 5.5v2M6.5 9h.01" stroke="var(--yellow)" strokeWidth="1.2" strokeLinecap="round" />
                </svg>
                <span>{tramite.alerta}</span>
              </div>
            )}

            {tramite.nombre_oficial && (
              <p className={styles.tramiteNombreOficial}>
                Nombre oficial: <em>{tramite.nombre_oficial}</em>
              </p>
            )}

            <div className={styles.tramiteField}>
              <span className={styles.tramiteFieldLabel}>Qué es</span>
              <p className={styles.tramiteFieldValue}>{tramite.que_es}</p>
            </div>

            <div className={styles.tramiteField}>
              <span className={styles.tramiteFieldLabel}>Dónde se hace</span>
              <p className={styles.tramiteFieldValue}>{tramite.donde}</p>
            </div>

            {tramite.documentos && tramite.documentos.length > 0 && (
              <div className={styles.tramiteField}>
                <span className={styles.tramiteFieldLabel}>Documentos necesarios</span>
                <ul className={styles.tramiteDocList}>
                  {tramite.documentos.map((doc, i) => (
                    <li key={i}>{doc}</li>
                  ))}
                </ul>
              </div>
            )}

            <div className={styles.tramiteMeta}>
              {tramite.tiempo_estimado && (
                <span className={styles.tramiteMetaItem}>
                  <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                    <circle cx="5.5" cy="5.5" r="4.5" stroke="currentColor" strokeWidth="1.2" />
                    <path d="M5.5 3v2.5l1.5 1.5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
                  </svg>
                  {tramite.tiempo_estimado}
                </span>
              )}
              {tramite.coste_estimado && (
                <span className={styles.tramiteMetaItem}>
                  <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                    <circle cx="5.5" cy="5.5" r="4.5" stroke="currentColor" strokeWidth="1.2" />
                    <path d="M5.5 3v5M4 4.5h2a1 1 0 010 2H4" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
                  </svg>
                  {tramite.coste_estimado}
                </span>
              )}
            </div>

            {tramite.enlace && (
              <a
                href={tramite.enlace}
                target="_blank"
                rel="noopener noreferrer"
                className={styles.tramiteEnlace}
              >
                <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
                  <path
                    d="M4.5 2.5H2.5a1 1 0 00-1 1v5a1 1 0 001 1h5a1 1 0 001-1V6.5"
                    stroke="currentColor"
                    strokeWidth="1.2"
                    strokeLinecap="round"
                  />
                  <path d="M6.5 1.5h3v3M9.5 1.5L5.5 5.5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
                </svg>
                Acceder al trámite oficial
              </a>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Fase accordion (controlled) ─────────────────────────────────────────────

interface FaseProps {
  fase: FaseRoadmap;
  index: number;
  isOpen: boolean;
  onToggle: () => void;
}

function FaseAccordion({ fase, index, isOpen, onToggle }: FaseProps) {
  const color = FASE_COLORS[index % FASE_COLORS.length];
  const ref = useRef<HTMLDivElement>(null);

  // Scroll into view when opened via stepper
  useEffect(() => {
    if (isOpen && ref.current) {
      ref.current.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [isOpen]);

  return (
    <div
      ref={ref}
      className={styles.fase}
      style={{ "--fase-color": color } as React.CSSProperties}
    >
      <button className={styles.faseHeader} onClick={onToggle}>
        <span className={styles.faseNumBadge} style={{ "--step-color": color } as React.CSSProperties}>
          {fase.numero}
        </span>
        <div className={styles.faseTitleBlock}>
          <span className={styles.faseTitulo}>{fase.titulo}</span>
          {fase.descripcion && !isOpen && (
            <span className={styles.faseDesc}>{fase.descripcion}</span>
          )}
        </div>
        <span className={styles.faseTramiteCount} style={{ color }}>
          {fase.tramites.length} trámite{fase.tramites.length !== 1 ? "s" : ""}
        </span>
        <svg
          width="10"
          height="10"
          viewBox="0 0 10 10"
          className={styles.faseChevron}
          style={{ transform: isOpen ? "rotate(180deg)" : "none", color }}
        >
          <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" />
        </svg>
      </button>

      {isOpen && (
        <div className={styles.faseBody}>
          {fase.descripcion && (
            <p className={styles.faseDescExpanded}>{fase.descripcion}</p>
          )}
          <div className={styles.tramiteList}>
            {fase.tramites.map((t, idx) => (
              <TramiteCard key={t.numero} tramite={t} isLast={idx === fase.tramites.length - 1} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function LegalPanel({ zona, sessionId }: Props) {
  const [roadmap, setRoadmap] = useState<LegalRoadmapResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [openFaseIndex, setOpenFaseIndex] = useState<number>(0);
  const hasFetched = useRef(false);
  const currentZoneRef = useRef(zona.zona_id);
  const isLoadingRef = useRef(false);

  // Reset al cambiar de local para evitar mostrar datos de la zona anterior.
  useEffect(() => {
    currentZoneRef.current = zona.zona_id;
    hasFetched.current = false;
    setRoadmap(null);
    setError("");
    setOpenFaseIndex(0);
  }, [zona.zona_id]);

  const load = useCallback(async () => {
    if (hasFetched.current || isLoadingRef.current) return;
    isLoadingRef.current = true;
    hasFetched.current = true;
    const zonaIdSnapshot = zona.zona_id;
    setLoading(true);
    setError("");
    try {
      const data = await api.legal(zonaIdSnapshot, sessionId);
      if (currentZoneRef.current !== zonaIdSnapshot) return;
      setRoadmap(data);
    } catch (e) {
      console.error("Error cargando roadmap legal:", e);
      if (currentZoneRef.current !== zonaIdSnapshot) return;
      hasFetched.current = false;
      setError(e instanceof Error ? e.message : "No se pudo cargar el análisis legal.");
    } finally {
      isLoadingRef.current = false;
      if (currentZoneRef.current === zonaIdSnapshot) setLoading(false);
    }
  }, [zona.zona_id, sessionId]);

  useEffect(() => {
    load();
  }, [load]);

  const handleFaseToggle = (i: number) => {
    setOpenFaseIndex((prev) => (prev === i ? -1 : i));
  };

  if (loading) {
    return (
      <div className={styles.container}>
        <div className={styles.loadingState}>
          <div className="spinner" />
          <span>Generando roadmap burocrático con IA...</span>
        </div>
      </div>
    );
  }

  if (error !== "") {
    return (
      <div className={styles.container}>
        <div className={styles.errorState}>
          <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
            <path d="M14 3L25 23H3L14 3z" stroke="var(--red)" strokeWidth="1.5" />
            <path d="M14 11v5M14 19.5h.01" stroke="var(--red)" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
          <span>No se pudo cargar el análisis legal.</span>
          <button className={styles.retryBtn} onClick={load}>
            Reintentar
          </button>
        </div>
      </div>
    );
  }

  if (!roadmap) {
    return (
      <div className={styles.container}>
        <Skeleton h={60} />
        <Skeleton h={20} w="70%" />
        <Skeleton h={52} />
        <Skeleton h={52} />
        <Skeleton h={52} />
      </div>
    );
  }

  return (
    <div className={styles.container}>

      {/* ── Hero ── */}
      <div className={styles.hero}>
        <div className={styles.heroLeft}>
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
            <path d="M10 2L2 6v5c0 4.4 3.4 8.5 8 9.5 4.6-1 8-5.1 8-9.5V6L10 2z" stroke="var(--accent)" strokeWidth="1.4" strokeLinejoin="round" />
            <path d="M7 10l2 2 4-4" stroke="var(--accent)" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <div>
            <div className={styles.heroTitle}>Roadmap Burocrático</div>
            <div className={styles.heroSub}>{roadmap.tipo_negocio} · {roadmap.distrito}</div>
          </div>
        </div>
        <div className={styles.heroRight}>
          {roadmap.zona_restringida ? (
            <span className={`badge badge-rojo ${styles.zonaBadge}`}>Zona restringida</span>
          ) : (
            <span className={`badge badge-verde ${styles.zonaBadge}`}>Zona libre</span>
          )}
          <span className={styles.iaBadge}>
            <svg width="9" height="9" viewBox="0 0 9 9" fill="currentColor">
              <circle cx="4.5" cy="4.5" r="3.5" opacity="0.25" />
              <circle cx="4.5" cy="4.5" r="1.5" />
            </svg>
            IA
          </span>
        </div>
      </div>

      {/* ── Equipo externo ── */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
            <circle cx="4" cy="3.5" r="1.8" stroke="currentColor" strokeWidth="1.2" />
            <circle cx="8.5" cy="3.5" r="1.8" stroke="currentColor" strokeWidth="1.2" />
            <path d="M1 9.5c0-1.7 1.3-3 3-3M8.5 6.5c1.7 0 3 1.3 3 3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
          </svg>
          Equipo externo imprescindible
        </h3>
        <p className={styles.equipoNote}>No podrás hacer esto solo. Contrata estos perfiles antes de empezar.</p>
        <div className={styles.equipoList}>
          {roadmap.equipo_externo.map((p, i) => (
            <div key={i} className={styles.equipoItem}>
              <div className={styles.equipoIcon}>
                {i === 0 && (
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                    <rect x="2" y="3" width="12" height="10" rx="1.5" stroke="var(--accent)" strokeWidth="1.2" />
                    <path d="M5 7h6M5 10h4" stroke="var(--accent)" strokeWidth="1.2" strokeLinecap="round" />
                  </svg>
                )}
                {i === 1 && (
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                    <path d="M3 13V7l5-4 5 4v6" stroke="var(--accent)" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
                    <rect x="6" y="9" width="4" height="4" rx="0.5" stroke="var(--accent)" strokeWidth="1.1" />
                  </svg>
                )}
                {i >= 2 && (
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                    <path d="M8 2L2 5v4c0 3 2.5 5.5 6 6.5 3.5-1 6-3.5 6-6.5V5L8 2z" stroke="var(--accent)" strokeWidth="1.2" strokeLinejoin="round" />
                  </svg>
                )}
              </div>
              <div className={styles.equipoInfo}>
                <div className={styles.equipoNombre}>{p.nombre}</div>
                <div className={styles.equipoDesc}>{p.descripcion}</div>
              </div>
              <div className={styles.equipoCoste}>{p.coste_aprox}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── Fases (controlled accordions) ── */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
            <path d="M2 2h8M2 6h6M2 10h4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
          </svg>
          Trámites paso a paso
        </h3>
        <div className={styles.faseList}>
          {roadmap.fases.map((fase, i) => (
            <FaseAccordion
              key={fase.id}
              fase={fase}
              index={i}
              isOpen={openFaseIndex === i}
              onToggle={() => handleFaseToggle(i)}
            />
          ))}
        </div>
      </section>

      {/* ── Costes ── */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
            <circle cx="6" cy="6" r="4.5" stroke="currentColor" strokeWidth="1.2" />
            <path d="M6 3.5v5M4.5 5h2.5a1 1 0 010 2H4.5" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" />
          </svg>
          Resumen de costes
        </h3>
        <div className={styles.costesTable}>
          {roadmap.costes_resumen.map((c, i) => (
            <div
              key={i}
              className={`${styles.costeRow} ${i === roadmap.costes_resumen.length - 1 ? styles.costeRowTotal : ""}`}
            >
              <span className={styles.costeConcepto}>{c.concepto}</span>
              <span className={styles.costeValor}>{c.coste}</span>
            </div>
          ))}
        </div>
        <p className={styles.costesNota}>
          * Estimaciones orientativas. No incluye reforma interior, decoración ni stock inicial.
        </p>
      </section>

      {/* ── Próximos pasos ── */}
      {roadmap.proximos_pasos && roadmap.proximos_pasos.length > 0 && (
        <section className={styles.section}>
          <h3 className={styles.sectionTitle}>
            <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
              <path d="M2 6h8M7 3l3 3-3 3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            Próximos pasos
          </h3>
          <div className={styles.pasosList}>
            {roadmap.proximos_pasos.map((paso, i) => (
              <div key={i} className={styles.pasoItem}>
                <span className={styles.pasoNum}>{i + 1}</span>
                <span className={styles.pasoTexto}>{paso}</span>
              </div>
            ))}
          </div>
        </section>
      )}

    </div>
  );
}
