"use client";

import { DIMENSIONS, scoreBand, bandLabel, type DimensionKey } from "./dimensions";
import type { ScoresDimensiones } from "@/lib/types";
import styles from "./Ledger.module.css";

interface Props {
  scores?: ScoresDimensiones;
  pesosBase?: Record<string, number>;
  pesosMod?: Record<string, number>;
  /** id activo (sec-{key}) para resaltar la fila desde el TOC. */
  activeId?: string | null;
  /** Click hace smooth-scroll a la sección. */
  onSelect?: (id: string) => void;
  /** Keys de dimensiones cuyo score se está calibrando todavía (LLM async). */
  loadingKeys?: readonly DimensionKey[];
}

/**
 * Ledger — "libro mayor" de las 7 dimensiones.
 *
 * Coherencia visual con ZoneIndex (la lista pre-dossier):
 *  - Numeración tabular grande a la izquierda (mismo estilo que el ranking de zonas).
 *  - Nombre + subtítulo legible al centro.
 *  - Score grande con color semántico verde/amarillo/rojo (mismo que ranking).
 *  - Pill de peso modulado (% por perfil) si difiere del base.
 *  - Barra horizontal de progreso al fondo.
 *
 * Sirve a la vez como "índice ejecutivo" (resumen rápido de las 7 dims antes
 * de leerlas en detalle) y como navegación click → scroll al detalle.
 */
export default function Ledger({
  scores = {},
  pesosBase = {},
  pesosMod = {},
  activeId,
  onSelect,
  loadingKeys,
}: Props) {
  const pesosActivos = Object.keys(pesosMod).length > 0 ? pesosMod : pesosBase;
  const loadingSet = new Set(loadingKeys ?? []);

  function handleClick(key: DimensionKey) {
    const id = `sec-${key}`;
    onSelect?.(id);
    if (typeof document === "undefined") return;
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <section className={styles.ledger} aria-label="Libro mayor de las 7 dimensiones">
      <div className={styles.head}>
        <span className={styles.eyebrow}>LIBRO MAYOR</span>
        <span className={styles.headHint}>
          Pulsa una dimensión para ampliar
        </span>
      </div>

      <div className={styles.list}>
        {DIMENSIONS.map((d, i) => {
          const score = scores[d.key as DimensionKey];
          const isLoading = loadingSet.has(d.key as DimensionKey);
          const band = isLoading ? "na" : scoreBand(score);
          const label = isLoading ? "calibrando…" : bandLabel(band);
          const peso = pesosActivos[d.key];
          const pesoBase = pesosBase[d.key];
          const pesoModVal = pesosMod[d.key];
          const diff =
            pesoBase != null && pesoModVal != null
              ? Math.round((pesoModVal - pesoBase) * 100)
              : 0;
          const id = `sec-${d.key}`;
          const isActive = activeId === id;

          return (
            <button
              key={d.key}
              type="button"
              className={`${styles.row} ${styles[`row_${band}`]} ${isActive ? styles.rowActive : ""}`}
              onClick={() => handleClick(d.key as DimensionKey)}
              aria-label={`Ir a la sección ${d.name}, score ${score ?? "sin datos"}`}
            >
              <span className={styles.num}>{String(i + 1).padStart(2, "0")}</span>
              <div className={styles.body}>
                <div className={styles.nameTop}>{d.name}</div>
                <div className={styles.nameSub}>{d.subtitle}</div>
              </div>
              <div className={styles.scoreCol}>
                <span className={`${styles.score} ${styles[`score_${band}`]}`}>
                  {isLoading ? "···" : (score != null ? Math.round(score) : "—")}
                </span>
                <span className={`${styles.bandLabel} ${styles[`bandLabel_${band}`]}`}>
                  {label}
                </span>
              </div>
              <div className={styles.weightCol}>
                {peso != null && peso > 0 ? (
                  <>
                    <span className={styles.weight}>{Math.round(peso * 100)}%</span>
                    {Math.abs(diff) >= 1 && (
                      <span
                        className={diff > 0 ? styles.weightUp : styles.weightDown}
                      >
                        {diff > 0 ? `+${Math.abs(diff)}` : `-${Math.abs(diff)}`}
                      </span>
                    )}
                  </>
                ) : (
                  <span className={styles.weightHint}>sin peso</span>
                )}
              </div>
              <div className={styles.barTrack}>
                <div
                  className={`${styles.barFill} ${styles[`barFill_${band}`]}`}
                  style={{
                    width:
                      score != null
                        ? `${Math.max(0, Math.min(100, score))}%`
                        : "0%",
                  }}
                />
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}
