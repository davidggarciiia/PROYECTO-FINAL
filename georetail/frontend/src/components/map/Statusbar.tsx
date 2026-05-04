"use client";

import styles from "./Statusbar.module.css";

interface Props {
  query: string;
  numZonas: number;
  numFuentes?: number;
  fecha?: string;
  onRestart: () => void;
  onTutorial: () => void;
}

function formatFecha(d: Date = new Date()): string {
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  return `${dd}.${mm}.${yy}`;
}

export default function Statusbar({
  query,
  numZonas,
  numFuentes = 12,
  fecha,
  onRestart,
  onTutorial,
}: Props) {
  const displayDate = fecha ?? formatFecha();
  const displayQuery = query || "cafetería de especialidad para público joven";

  return (
    <div className={styles.statusbar}>
      <div className={styles.brand}>
        <span className={styles.brandDot} />
        KnowPoints
      </div>

      <div className={styles.query} title={displayQuery}>
        <span className={styles.queryPrefix}>PROMPT</span>
        <em>&ldquo;{displayQuery}&rdquo;</em>
      </div>

      <div className={styles.meta}>
        <span>
          <span className={styles.tick}>●</span>
          {numZonas} ZONAS
        </span>
        <span>
          <span className={styles.tick}>●</span>
          {numFuentes} FUENTES
        </span>
        <span>
          <span className={styles.tick}>●</span>
          {displayDate}
        </span>
      </div>

      <button
        className={styles.help}
        onClick={onTutorial}
        aria-label="Abrir tutorial"
        title="Ver tutorial"
      >
        ?
      </button>

      <button className={styles.restart} onClick={onRestart}>
        + Nuevo análisis
      </button>
    </div>
  );
}
