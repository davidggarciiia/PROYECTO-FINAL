"use client";

import styles from "./Dossier.module.css";

interface Props {
  variant: "financiero" | "legal";
}

const COPY: Record<Props["variant"], { eyebrow: string; title: string; body: string }> = {
  financiero: {
    eyebrow: "MÓDULO EN DESARROLLO",
    title: "Pendiente de implementar",
    body: "Esta sección se habilitará cuando el backend correspondiente esté conectado. Mientras tanto puedes cerrar este panel y seguir explorando zonas.",
  },
  legal: {
    eyebrow: "MÓDULO EN DESARROLLO",
    title: "Pendiente de implementar",
    body: "Esta sección se habilitará cuando el backend correspondiente esté conectado. Mientras tanto puedes cerrar este panel y seguir explorando zonas.",
  },
};

export default function DossierTabStub({ variant }: Props) {
  const copy = COPY[variant];

  return (
    <div className={styles.stubWrap}>
      <div className={styles.stubCard}>
        <div className={styles.stubEyebrow}>
          <span className={styles.stubTick}>●</span>
          {copy.eyebrow}
        </div>
        <h3 className={styles.stubTitle}>{copy.title}</h3>
        <p className={styles.stubBody}>{copy.body}</p>
      </div>
    </div>
  );
}
