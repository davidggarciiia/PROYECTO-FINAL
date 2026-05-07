"use client";

import { fechaEditorial } from "./helpers";
import styles from "./Colofon.module.css";

interface Props {
  zonaId: string;
  modeloVersion?: string | null;
}

/**
 * Colofón — pie del informe editorial. Muestra autoría de los datos,
 * fecha de compilación y referencia. Mismo eyebrow ● del resto.
 */
export default function Colofon({ zonaId }: Props) {
  const fecha = fechaEditorial();

  return (
    <footer className={styles.colofon}>
      <div className={styles.colofonInner}>
        <div className={styles.left}>
          <span className={styles.eyebrow}>
            <span className={styles.tick}>●</span>
            COLOFÓN
          </span>
          <p className={styles.body}>
            Informe preparado para comparar ubicación, demanda, entorno y
            viabilidad con una lectura clara por dimensiones.
          </p>
        </div>
        <div className={styles.right}>
          <div className={styles.refRow}>
            <span className={styles.refLabel}>Ref.</span>
            <span className={styles.refValue}>{zonaId}</span>
          </div>
          <div className={styles.refRow}>
            <span className={styles.refLabel}>Compilado</span>
            <span className={styles.refValue}>{fecha}</span>
          </div>
          <div className={styles.refRow}>
            <span className={styles.refLabel}>Editor</span>
            <span className={styles.refValue}>GeoRetail BCN · vol. 1</span>
          </div>
        </div>
      </div>
    </footer>
  );
}
