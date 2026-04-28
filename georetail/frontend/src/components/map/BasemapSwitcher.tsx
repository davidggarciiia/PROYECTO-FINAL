"use client";

import { useState } from "react";
import type { BasemapId } from "./MapCanvas";
import styles from "./BasemapSwitcher.module.css";

interface Props {
  value: BasemapId;
  onChange: (b: BasemapId) => void;
}

const OPTIONS: [BasemapId, string][] = [
  ["dark", "Oscuro"],
  ["light", "Claro"],
  ["voya", "Voyager"],
  ["sat", "Satélite"],
];

export default function BasemapSwitcher({ value, onChange }: Props) {
  const [open, setOpen] = useState(false);

  const activeLabel = OPTIONS.find(([k]) => k === value)?.[1] ?? value;

  return (
    <div className={`${styles.switcher} ${open ? styles.open : ""}`}>
      <button
        className={styles.toggle}
        onClick={() => setOpen((v) => !v)}
        title={open ? "Colapsar estilos de mapa" : "Estilos de mapa"}
        type="button"
      >
        <span className={styles.toggleLeft}>
          <span className={styles.toggleLabel}>ESTILOS</span>
          <span className={styles.toggleValue}>{activeLabel}</span>
        </span>
        <svg
          className={`${styles.chevron} ${open ? styles.chevronOpen : ""}`}
          width="12"
          height="12"
          viewBox="0 0 12 12"
          fill="none"
          aria-hidden
        >
          <path
            d="M2.5 4.5L6 8L9.5 4.5"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>

      <div className={styles.optionsWrap}>
        <div className={styles.options}>
          {OPTIONS.map(([k, l]) => (
            <button
              key={k}
              className={`${styles.opt} ${value === k ? styles.on : ""}`}
              onClick={() => onChange(k)}
              type="button"
            >
              {l}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
