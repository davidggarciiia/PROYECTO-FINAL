"use client";

import { useCallback, useEffect, useState } from "react";
import dynamic from "next/dynamic";
import type {
  ZonaPreview,
  LocalDetalleResponse,
  CompetenciaDetalle,
} from "@/lib/types";
import { api } from "@/lib/api";
import DossierTabScore from "./DossierTabScore";
import DossierTabPerfil from "./DossierTabPerfil";
import DossierTabStub from "./DossierTabStubs";
import ProfileChipBar from "./ProfileChipBar";
import styles from "./Dossier.module.css";

// Leaflet no puede ejecutarse en SSR → carga lazy sólo en cliente.
const CompetenciaPanel = dynamic(
  () => import("../CompetenciaPanel"),
  {
    ssr: false,
    loading: () => (
      <div className={styles.stubWrap}>
        <div className={styles.stubCard}>
          <div className={styles.stubEyebrow}>
            <span className={styles.stubTick}>●</span>
            CARGANDO COMPETENCIA
          </div>
          <p className={styles.stubBody}>Preparando análisis de mercado y minimapa…</p>
        </div>
      </div>
    ),
  },
);

type Tab = "score" | "competencia" | "perfil" | "financiero" | "legal";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
  onClose: () => void;
  sessionId: string;
}

export default function Dossier({ zone, detalle, loading, onClose, sessionId }: Props) {
  const [tab, setTab] = useState<Tab>("score");
  const [competencia, setCompetencia] = useState<CompetenciaDetalle | null>(null);
  const [loadingComp, setLoadingComp] = useState(false);
  const [competenciaError, setCompetenciaError] = useState<string | null>(null);

  // Reset del estado de competencia cuando cambia la zona.
  useEffect(() => {
    setCompetencia(null);
    setCompetenciaError(null);
  }, [zone.zona_id]);

  const loadCompetencia = useCallback(async () => {
    if (competencia || loadingComp) return;
    setLoadingComp(true);
    setCompetenciaError(null);
    try {
      const data = await api.competencia(zone.zona_id, sessionId);
      setCompetencia(data);
    } catch (error) {
      console.error("Error competencia:", error);
      setCompetenciaError("No se ha podido cargar el análisis de competencia.");
    } finally {
      setLoadingComp(false);
    }
  }, [competencia, loadingComp, sessionId, zone.zona_id]);

  useEffect(() => {
    if (tab === "competencia") void loadCompetencia();
  }, [tab, loadCompetencia]);

  const score = zone.score_global ?? 0;
  const band = score >= 75 ? "hi" : score >= 55 ? "mid" : "lo";

  // Perfil refinado — lazy: puede no venir en la respuesta. Si es null,
  // ProfileChipBar devuelve null y el tab Perfil renderiza el empty state.
  const perfil = detalle?.zona.perfil_refinado ?? null;
  const sector = detalle?.zona.sector_codigo ?? null;
  const subsector = detalle?.zona.subsector_codigo ?? null;

  return (
    <div className={styles.overlay}>
      <aside className={`${styles.panel} ${styles[`band_${band}`]}`} role="dialog" aria-label="Dossier de zona">
        <header className={styles.header}>
          <div className={styles.headLeft}>
            <div className={styles.eyebrow}>
              <span className={styles.tick}>●</span>
              DOSSIER · {zone.barrio ?? ""}
            </div>
            <h2 className={styles.name}>{zone.nombre ?? "Zona"}</h2>
            <div className={styles.meta}>
              <span>{zone.distrito ?? ""}</span>
              {zone.alquiler_mensual != null && (
                <>
                  <span className={styles.dot}>·</span>
                  <span>{zone.alquiler_mensual.toLocaleString("es-ES")} €/mes</span>
                </>
              )}
              {zone.m2 != null && (
                <>
                  <span className={styles.dot}>·</span>
                  <span>{zone.m2} m²</span>
                </>
              )}
            </div>
            {perfil && (
              <ProfileChipBar perfil={perfil} sector={sector} subsector={subsector} />
            )}
          </div>

          <div className={styles.headRight}>
            <div className={`${styles.scoreBadge} ${styles[`sb_${band}`]}`}>
              <div className={styles.scoreNum}>{Math.round(score)}</div>
              <div className={styles.scoreLabel}>SCORE</div>
            </div>
            <button
              type="button"
              onClick={onClose}
              aria-label="Cerrar dossier"
              className={styles.closeBtn}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round">
                <path d="M3 3l8 8M11 3l-8 8" />
              </svg>
            </button>
          </div>
        </header>

        <nav className={styles.tabs} aria-label="Secciones del dossier">
          {(
            [
              { id: "score", label: "Score" },
              { id: "competencia", label: "Competencia" },
              { id: "perfil", label: "Perfil" },
              { id: "financiero", label: "Financiero" },
              { id: "legal", label: "Legal" },
            ] as { id: Tab; label: string }[]
          ).map((t) => (
            <button
              key={t.id}
              type="button"
              className={`${styles.tab} ${tab === t.id ? styles.tabActive : ""}`}
              onClick={() => setTab(t.id)}
              aria-selected={tab === t.id}
            >
              {t.label}
            </button>
          ))}
        </nav>

        <div className={styles.body}>
          {tab === "score" && (
            <DossierTabScore zone={zone} detalle={detalle} loading={loading} />
          )}
          {tab === "competencia" && (
            competenciaError ? (
              <div className={styles.stubWrap}>
                <div className={styles.stubCard}>
                  <div className={styles.stubEyebrow}>
                    <span className={styles.stubTick}>●</span>
                    COMPETENCIA
                  </div>
                  <p className={styles.stubBody}>{competenciaError}</p>
                </div>
              </div>
            ) : (
              <CompetenciaPanel competencia={competencia} loading={loadingComp} zona={zone} />
            )
          )}
          {tab === "perfil" && (
            <DossierTabPerfil zona={detalle?.zona ?? null} />
          )}
          {tab === "financiero" && <DossierTabStub variant="financiero" />}
          {tab === "legal" && <DossierTabStub variant="legal" />}
        </div>
      </aside>
    </div>
  );
}
