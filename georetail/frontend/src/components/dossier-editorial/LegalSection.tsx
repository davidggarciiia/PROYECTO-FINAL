"use client";

import type { ZonaPreview } from "@/lib/types";
import LegalPanel from "../LegalPanel";
import styles from "./FinancieroSection.module.css";

interface Props {
  zone: ZonaPreview;
  sessionId: string;
  num: string;
}

/**
 * Sección Legal del editorial. Reusa LegalPanel existente y le añade el
 * mismo header de sección que FinancieroSection — coherencia editorial.
 *
 * LegalPanel se autogestiona: hace fetch al cargar y resuelve roadmap
 * personalizado con LLM si es necesario. Aquí solo le pasamos zone+session.
 */
export default function LegalSection({ zone, sessionId, num }: Props) {
  return (
    <section
      id="sec-legal"
      className={styles.section}
      aria-label="Roadmap legal"
    >
      <header className={styles.head}>
        <span className={styles.kicker}>§ {num}</span>
        <h2 className={styles.title}>
          Roadmap <em>legal</em>
        </h2>
        <p className={styles.lead}>
          Trámites, fases y costes administrativos para abrir el local en
          Barcelona. El roadmap se personaliza con LLM cuando hay restricciones
          conocidas (Plan de Usos, modalidades sectoriales) en la zona.
        </p>
      </header>

      <div className={styles.panelWrap}>
        <LegalPanel zona={zone} sessionId={sessionId} />
      </div>
    </section>
  );
}
