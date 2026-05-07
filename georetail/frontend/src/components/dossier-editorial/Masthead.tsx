"use client";

import type { ZonaPreview, PerfilRefinado } from "@/lib/types";
import ProfileChipBar from "../map/ProfileChipBar";
import { fechaEditorial } from "./helpers";
import styles from "./Masthead.module.css";

interface Props {
  zone: ZonaPreview;
  perfil?: PerfilRefinado | null;
  sector?: string | null;
  subsector?: string | null;
  onClose?: () => void;
}

/**
 * Masthead — la cabecera grande del informe editorial.
 * Coherencia visual: misma eyebrow tick que ZoneIndex (● accent + uppercase wide),
 * mismo tabular-nums para la referencia, y misma fila barrio · distrito · m² · €.
 */
export default function Masthead({
  zone,
  perfil,
  sector,
  subsector,
  onClose,
}: Props) {
  const fecha = fechaEditorial();
  // Separar la última palabra del nombre para destacarla en cursiva (estilo
  // editorial: "Carrer del *Parlament* 32"). Si solo hay una palabra, no hay
  // emfasis especial.
  const nombre = zone.nombre ?? "Zona";
  const partes = nombre.split(" ");
  const partePrincipal =
    partes.length >= 2
      ? partes.slice(0, partes.length - 1).join(" ")
      : nombre;
  const parteEmfasis = partes.length >= 2 ? partes[partes.length - 1] : "";

  return (
    <header className={styles.mast}>
      <div className={styles.mastInner}>
        <div className={styles.mastLeft}>
          <span className={styles.eyebrow}>
            <span className={styles.tick}>●</span>
            DOSSIER · GEORETAIL
          </span>
          <h1 className={styles.title}>
            {partePrincipal}{" "}
            {parteEmfasis && <em className={styles.titleEm}>{parteEmfasis}</em>}
          </h1>
          <div className={styles.meta}>
            {zone.barrio && <span>{zone.barrio}</span>}
            {zone.barrio && zone.distrito && <span className={styles.dot}>·</span>}
            {zone.distrito && <span>{zone.distrito}</span>}
            {zone.m2 != null && (
              <>
                <span className={styles.dot}>·</span>
                <span>{zone.m2} m²</span>
              </>
            )}
            {zone.alquiler_mensual != null && (
              <>
                <span className={styles.dot}>·</span>
                <span>
                  {Math.round(zone.alquiler_mensual).toLocaleString("es-ES")} €/mes
                </span>
              </>
            )}
          </div>
          {perfil && (
            <div className={styles.chipBarWrap}>
              <ProfileChipBar perfil={perfil} sector={sector} subsector={subsector} />
            </div>
          )}
        </div>

        <div className={styles.mastRight}>
          <div className={styles.refBlock}>
            <span className={styles.refLabel}>Ref.</span>
            <span className={styles.refNum}>{zone.zona_id}</span>
            <span className={styles.refDate}>{fecha}</span>
            <span className={styles.refVol}>vol. 1</span>
          </div>
          {onClose && (
            <button
              type="button"
              onClick={onClose}
              aria-label="Cerrar dossier"
              className={styles.closeBtn}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none"
                   stroke="currentColor" strokeWidth="1.6" strokeLinecap="round">
                <path d="M3 3l8 8M11 3l-8 8" />
              </svg>
            </button>
          )}
        </div>
      </div>
    </header>
  );
}
