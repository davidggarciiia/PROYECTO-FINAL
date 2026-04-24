"use client";

import type { ZonaPreview, ScoresDimensiones } from "@/lib/types";
import { DIMENSIONS, type DimensionKey } from "./dimensions";
import styles from "./ActiveDock.module.css";

interface Props {
  zone: ZonaPreview | null;
  zones: ZonaPreview[];
  dims?: ScoresDimensiones | null;
  loading?: boolean;
  onExpand: () => void;
  onNav: (dir: -1 | 1) => void;
}

function band(score?: number | null): "hi" | "mid" | "lo" {
  const s = score ?? 0;
  if (s >= 75) return "hi";
  if (s >= 55) return "mid";
  return "lo";
}

export default function ActiveDock({
  zone,
  zones,
  dims,
  loading,
  onExpand,
  onNav,
}: Props) {
  if (!zone) return null;
  const b = band(zone.score_global);
  const idx = zones.findIndex((z) => z.zona_id === zone.zona_id);
  const scoreStr = zone.score_global != null ? Math.round(zone.score_global) : "—";

  return (
    <div className={styles.dock}>
      <div className={styles.head}>
        <div className={styles.eyebrow}>
          <span className={styles.tick}>●</span>
          {String(Math.max(0, idx) + 1).padStart(2, "0")} /{" "}
          {String(zones.length).padStart(2, "0")} · FICHA ACTIVA
        </div>
        <div className={styles.nav}>
          <button onClick={() => onNav(-1)} aria-label="Zona anterior">
            &lsaquo;
          </button>
          <button onClick={() => onNav(1)} aria-label="Zona siguiente">
            &rsaquo;
          </button>
        </div>
      </div>

      <div className={`${styles.title} ${styles[b]}`}>
        <div className={styles.score}>{scoreStr}</div>
        <div className={styles.meta}>
          <div className={styles.name}>{zone.nombre}</div>
          <div className={styles.loc}>
            {zone.barrio ? `${zone.barrio} · ` : ""}
            {zone.distrito || "BARCELONA"} · BARCELONA
          </div>
        </div>
      </div>

      <div className={styles.bars}>
        {DIMENSIONS.map((d) => {
          const v = dims?.[d.key as DimensionKey];
          const hasV = typeof v === "number";
          const barB = hasV ? band(v) : "lo";
          const barPct = hasV ? Math.max(4, Math.min(100, v)) : 0;
          return (
            <div
              key={d.key}
              className={`${styles.bar} ${hasV ? styles[barB] : styles.empty}`}
              title={`${d.name}: ${hasV ? Math.round(v as number) : "—"}`}
            >
              <div className={styles.track}>
                <div className={styles.fill} style={{ height: `${barPct}%` }} />
              </div>
              <div className={styles.barLabel}>{d.short}</div>
              <div className={styles.barN}>{hasV ? Math.round(v as number) : "—"}</div>
            </div>
          );
        })}
      </div>

      <div className={styles.actions}>
        <button className={styles.actionGhost} type="button" disabled>
          Guardar
        </button>
        <button
          className={styles.actionPrimary}
          type="button"
          onClick={onExpand}
          disabled={loading}
        >
          {loading ? "Cargando dossier…" : "Ver dossier completo →"}
        </button>
      </div>
    </div>
  );
}
