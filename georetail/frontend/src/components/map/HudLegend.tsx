"use client";

import styles from "./HudLegend.module.css";

export default function HudLegend() {
  return (
    <div className={styles.legend}>
      <span>
        <span className={styles.dot} style={{ background: "var(--green)" }} />
        VIABLE
      </span>
      <span>
        <span className={styles.dot} style={{ background: "var(--yellow)" }} />
        MIXTA
      </span>
      <span>
        <span className={styles.dot} style={{ background: "var(--red)" }} />
        DESCARTADA
      </span>
      <span className={styles.active}>
        <span className={styles.dot} style={{ background: "var(--accent)" }} />
        ACTIVA
      </span>
    </div>
  );
}
