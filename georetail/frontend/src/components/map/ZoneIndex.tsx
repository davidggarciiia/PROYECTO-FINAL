"use client";

import { useMemo, useState } from "react";
import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneIndex.module.css";

type Filter = "all" | "hi" | "mid" | "lo";

interface Props {
  zonas: ZonaPreview[];
  activeId?: string | null;
  onPick: (id: string) => void;
  fecha?: string;
}

function band(score?: number | null): "hi" | "mid" | "lo" {
  const s = score ?? 0;
  if (s >= 75) return "hi";
  if (s >= 55) return "mid";
  return "lo";
}

export default function ZoneIndex({ zonas, activeId, onPick, fecha }: Props) {
  const [filter, setFilter] = useState<Filter>("all");

  const counts = useMemo(
    () => ({
      all: zonas.length,
      hi: zonas.filter((z) => (z.score_global ?? 0) >= 75).length,
      mid: zonas.filter((z) => {
        const s = z.score_global ?? 0;
        return s >= 55 && s < 75;
      }).length,
      lo: zonas.filter((z) => (z.score_global ?? 0) < 55).length,
    }),
    [zonas],
  );

  const filtered = useMemo(() => {
    if (filter === "all") return zonas;
    return zonas.filter((z) => band(z.score_global) === filter);
  }, [zonas, filter]);

  return (
    <aside className={styles.index}>
      <div className={styles.head}>
        <div className={styles.eyebrow}>
          <span className={styles.tick}>●</span>
          RANKING · {zonas.length} ZONAS
        </div>
        <span className={styles.live}>LIVE</span>
      </div>

      <div className={styles.filters}>
        {(
          [
            ["all", "Todas"],
            ["hi", "Viables"],
            ["mid", "Mixtas"],
            ["lo", "Descart."],
          ] as [Filter, string][]
        ).map(([k, l]) => (
          <button
            key={k}
            className={`${styles.filter} ${filter === k ? styles.filterOn : ""}`}
            onClick={() => setFilter(k)}
          >
            {l} · {counts[k]}
          </button>
        ))}
      </div>

      <div className={styles.list}>
        {filtered.length === 0 && (
          <div className={styles.empty}>
            Ninguna zona en este tramo. Prueba otro filtro o refina la búsqueda.
          </div>
        )}
        {filtered.map((z, i) => {
          const b = band(z.score_global);
          const isActive = z.zona_id === activeId;
          return (
            <div
              key={z.zona_id}
              className={`${styles.zone} ${styles[b]} ${isActive ? styles.isActive : ""}`}
              onClick={() => onPick(z.zona_id)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onPick(z.zona_id);
                }
              }}
            >
              <div className={styles.num}>{String(i + 1).padStart(2, "0")}</div>
              <div className={styles.body}>
                <div className={styles.name}>{z.nombre}</div>
                <div className={styles.district}>
                  {z.barrio ? `${z.barrio} · ` : ""}
                  {z.distrito}
                </div>
                <div className={styles.stats}>
                  {z.alquiler_mensual != null ? (
                    <span>{Math.round(z.alquiler_mensual).toLocaleString("es-ES")}€</span>
                  ) : (
                    <span>— €</span>
                  )}
                  <span>·</span>
                  {z.m2 != null ? <span>{z.m2} m²</span> : <span>— m²</span>}
                </div>
              </div>
              <div className={styles.score}>
                <div className={styles.scoreN}>
                  {z.score_global != null ? Math.round(z.score_global) : "—"}
                </div>
                <div className={styles.scoreL}>score</div>
              </div>
            </div>
          );
        })}
      </div>

      <div className={styles.foot}>
        <span>8 DIMENSIONES</span>
        <span className={styles.footTick}>
          <span className={styles.tick}>●</span> ACTUALIZADO {fecha ?? "HOY"}
        </span>
      </div>
    </aside>
  );
}
