"use client";

import { useMemo, useState } from "react";
import type { ZonaPreview } from "@/lib/types";
import styles from "./ZoneIndex.module.css";

type Sort = "score" | "price_m2" | "alquiler";

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

const DEFAULT_DIR: Record<Sort, "asc" | "desc"> = {
  score:    "desc",
  price_m2: "asc",
  alquiler: "asc",
};

export default function ZoneIndex({ zonas, activeId, onPick, fecha }: Props) {
  const [sort, setSort] = useState<Sort>("score");
  const [dir, setDir]   = useState<"asc" | "desc">("desc");
  const [collapsed, setCollapsed] = useState(false);

  function handleSort(k: Sort) {
    if (k === sort) {
      setDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSort(k);
      setDir(DEFAULT_DIR[k]);
    }
  }

  const sorted = useMemo(() => {
    const arr = [...zonas];
    const sign = dir === "asc" ? 1 : -1;
    if (sort === "score") {
      arr.sort((a, b) => sign * ((a.score_global ?? 0) - (b.score_global ?? 0)));
    } else if (sort === "price_m2") {
      arr.sort((a, b) => {
        const pa = a.m2 ? (a.alquiler_mensual ?? 0) / a.m2 : Infinity;
        const pb = b.m2 ? (b.alquiler_mensual ?? 0) / b.m2 : Infinity;
        return sign * (pa - pb);
      });
    } else {
      arr.sort((a, b) => sign * ((a.alquiler_mensual ?? Infinity) - (b.alquiler_mensual ?? Infinity)));
    }
    return arr;
  }, [zonas, sort, dir]);

  return (
    <aside className={`${styles.index} ${collapsed ? styles.collapsed : ""}`}>
      <div className={styles.head}>
        <div className={styles.eyebrow}>
          <span className={styles.tick}>●</span>
          RANKING · {zonas.length} ZONAS
        </div>
        <div className={styles.headRight}>
          <span className={styles.live}>LIVE</span>
          <button
            className={styles.collapseBtn}
            onClick={() => setCollapsed((v) => !v)}
            title={collapsed ? "Expandir lista de zonas" : "Colapsar lista de zonas"}
            type="button"
          >
            <svg
              className={`${styles.collapseIcon} ${collapsed ? styles.collapseIconClosed : ""}`}
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
        </div>
      </div>

      <div className={styles.body}>
        <div className={styles.filters}>
          {(
            [
              ["score",    "Score"],
              ["price_m2", "€/m²"],
              ["alquiler", "Alquiler"],
            ] as [Sort, string][]
          ).map(([k, l]) => (
            <button
              key={k}
              className={`${styles.filter} ${sort === k ? styles.filterOn : ""}`}
              onClick={() => handleSort(k)}
            >
              {l}{sort === k ? (dir === "asc" ? " ↑" : " ↓") : ""}
            </button>
          ))}
        </div>

        <div className={styles.list}>
          {sorted.length === 0 && (
            <div className={styles.empty}>
              Sin zonas. Refina la búsqueda para ver resultados.
            </div>
          )}
          {sorted.map((z, i) => {
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
                <div className={styles.body2}>
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
      </div>
    </aside>
  );
}
