"use client";

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
  return (
    <div className={styles.switcher}>
      <div className={styles.label}>BASE</div>
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
  );
}
