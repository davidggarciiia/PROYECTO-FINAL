"use client";

import { useState } from "react";
import type { ZonaPreview, LocalDetalleResponse } from "@/lib/types";
import DossierTabScore from "./DossierTabScore";
import DossierTabStub from "./DossierTabStubs";
import styles from "./Dossier.module.css";

type Tab = "score" | "financiero" | "legal";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
  onClose: () => void;
}

export default function Dossier({ zone, detalle, loading, onClose }: Props) {
  const [tab, setTab] = useState<Tab>("score");

  const score = zone.score_global ?? 0;
  const band = score >= 75 ? "hi" : score >= 55 ? "mid" : "lo";

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
          {tab === "financiero" && <DossierTabStub variant="financiero" />}
          {tab === "legal" && <DossierTabStub variant="legal" />}
        </div>
      </aside>
    </div>
  );
}
