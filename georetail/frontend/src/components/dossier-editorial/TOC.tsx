"use client";

import { type ReactNode } from "react";
import { DIMENSIONS, scoreBand, type DimensionKey, type Band } from "./dimensions";
import type { ScoresDimensiones } from "@/lib/types";
import styles from "./TOC.module.css";

interface ExtraEntry {
  id: string;
  label: string;
  badge?: ReactNode;
}

interface Props {
  /** Mapa scores → para pintar el dot semáforo de cada dimensión. */
  scores?: ScoresDimensiones;
  /** Sección activa para resaltar la línea correspondiente. */
  activeId?: string | null;
  /** Callback al seleccionar (smooth-scroll). */
  onSelect: (id: string) => void;
  /** Entradas extra que se añaden al final (Financiero, Legal). */
  extras?: ExtraEntry[];
}

const DOT_BY_BAND: Record<Band, string> = {
  hi: styles.dotHi,
  mid: styles.dotMid,
  lo: styles.dotLo,
  na: styles.dotNa,
};

/**
 * TOC — sumario lateral sticky con las 7 dimensiones + extras (Financiero,
 * Legal). Pulsando hace smooth-scroll a la sección. Marca la activa con la
 * misma barra lateral 3px accent que ZoneIndex.
 */
export default function TOC({
  scores = {},
  activeId,
  onSelect,
  extras = [],
}: Props) {
  function handleClick(id: string) {
    onSelect(id);
    if (typeof document === "undefined") return;
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <aside className={styles.toc} aria-label="Sumario del dossier">
      <div className={styles.head}>
        <span className={styles.eyebrow}>
          <span className={styles.tick}>●</span>
          SUMARIO
        </span>
        <span className={styles.live}>{DIMENSIONS.length} DIMS</span>
      </div>

      <ol className={styles.list}>
        {DIMENSIONS.map((d, i) => {
          const score = scores[d.key as DimensionKey];
          const band = scoreBand(score);
          const id = `sec-${d.key}`;
          const isActive = activeId === id;
          return (
            <li key={d.key}>
              <button
                type="button"
                className={`${styles.item} ${isActive ? styles.itemActive : ""}`}
                onClick={() => handleClick(id)}
              >
                <span className={styles.num}>{String(i + 1).padStart(2, "0")}</span>
                <span className={styles.name}>{d.name}</span>
                <span className={styles.score}>
                  {score != null ? Math.round(score) : "—"}
                </span>
                <span className={`${styles.dot} ${DOT_BY_BAND[band]}`} />
              </button>
            </li>
          );
        })}

        {extras.map((e, i) => {
          const isActive = activeId === e.id;
          return (
            <li key={e.id}>
              <button
                type="button"
                className={`${styles.item} ${styles.itemExtra} ${isActive ? styles.itemActive : ""}`}
                onClick={() => handleClick(e.id)}
              >
                <span className={styles.num}>
                  {String(DIMENSIONS.length + 1 + i).padStart(2, "0")}
                </span>
                <span className={styles.name}>{e.label}</span>
                {e.badge && <span className={styles.score}>{e.badge}</span>}
                <span className={`${styles.dot} ${styles.dotAccent}`} />
              </button>
            </li>
          );
        })}
      </ol>

      <div className={styles.foot}>
        <span>SCROLL VERTICAL</span>
      </div>
    </aside>
  );
}
