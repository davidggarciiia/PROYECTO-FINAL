"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { LineaCercana, TransporteDetalleZona, TransporteTipo } from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./TransportePanel.module.css";

interface Props {
  zonaId: string;
  radioM?: number;
  fallbackLineas?: number;
  fallbackParadas?: number;
}

const RADIO_DEFAULT = 500;
const TIPO_ORDER: TransporteTipo[] = ["metro", "tram", "fgc", "rodalies", "bus"];
const TIPO_LABEL: Record<TransporteTipo, string> = {
  metro: "Metro", tram: "Tram", fgc: "FGC", rodalies: "Rodalies", bus: "Bus",
};
// Fallback de color si el backend no lo manda (colores corporativos TMB).
const TIPO_COLOR_FALLBACK: Record<TransporteTipo, string> = {
  metro: "#D03324", bus: "#E3000F", tram: "#007F3B", fgc: "#9B2743", rodalies: "#9B2743",
};

/** YIQ-based contrast: colores muy claros → texto negro. */
function textColorFor(bg: string): "#fff" | "#111" {
  if (!bg) return "#fff";
  let hex = bg.trim().replace("#", "");
  if (hex.length === 3) hex = hex.split("").map((c) => c + c).join("");
  if (hex.length !== 6) return "#fff";
  const r = parseInt(hex.substring(0, 2), 16);
  const g = parseInt(hex.substring(2, 4), 16);
  const b = parseInt(hex.substring(4, 6), 16);
  if ([r, g, b].some((v) => Number.isNaN(v))) return "#fff";
  return (r * 299 + g * 587 + b * 114) / 1000 >= 175 ? "#111" : "#fff";
}

function colorForLinea(linea: LineaCercana): string {
  if (linea.color && linea.color.trim()) return linea.color;
  return TIPO_COLOR_FALLBACK[linea.tipo as TransporteTipo] ?? "#888";
}

const roundM = (n: number) => `${Math.round(n)} m`;

function LineaPopover({ linea, onClose }: { linea: LineaCercana; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", onKey);
    ref.current?.focus();
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);
  const bg = colorForLinea(linea);
  const fg = textColorFor(bg);
  const topParadas = linea.paradas_cercanas.slice(0, 2);
  return (
    <div ref={ref} className={styles.popover} role="dialog" aria-label={`Detalles línea ${linea.codigo}`} tabIndex={-1}>
      <div className={styles.popoverHead}>
        <span className={styles.popoverChip} style={{ background: bg, color: fg, borderColor: bg }}>{linea.codigo}</span>
        <div className={styles.popoverTitle}>{linea.nombre}</div>
        <button type="button" className={styles.popoverClose} onClick={onClose} aria-label="Cerrar detalle de línea">×</button>
      </div>
      {topParadas.length > 0 ? (
        <ul className={styles.popoverStops}>
          {topParadas.map((p, i) => (
            <li key={`${p.nombre}-${i}`} className={styles.popoverStop}>
              <span className={styles.popoverStopName}>{p.nombre}</span>
              <span className={styles.popoverStopDist}>{roundM(p.distancia_m)}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className={styles.popoverEmpty}>Sin paradas detalladas.</p>
      )}
      <p className={styles.popoverFoot}>Parada más cercana a {roundM(linea.dist_min_m)}.</p>
    </div>
  );
}

export default function TransportePanel({
  zonaId, radioM = RADIO_DEFAULT, fallbackLineas, fallbackParadas,
}: Props) {
  const [data, setData] = useState<TransporteDetalleZona | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [openKey, setOpenKey] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setLoading(true); setError(null); setData(null); setOpenKey(null);
    api.transporte(zonaId, radioM)
      .then((res) => { if (alive) setData(res); })
      .catch((err) => {
        console.error("Error transporte:", err);
        if (alive) setError("No se ha podido cargar el detalle de transporte.");
      })
      .finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [zonaId, radioM]);

  const grupos = useMemo(() => {
    if (!data) return [] as Array<{ tipo: string; lineas: LineaCercana[] }>;
    const buckets = new Map<string, LineaCercana[]>();
    data.lineas.forEach((l) => {
      const key = String(l.tipo);
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key)!.push(l);
    });
    const ordered: Array<{ tipo: string; lineas: LineaCercana[] }> = [];
    TIPO_ORDER.forEach((t) => {
      const arr = buckets.get(t);
      if (arr && arr.length) ordered.push({ tipo: t, lineas: arr });
    });
    // Extras no previstos (defensa contra contratos futuros): al final.
    buckets.forEach((arr, key) => {
      if (!TIPO_ORDER.includes(key as TransporteTipo) && arr.length) {
        ordered.push({ tipo: key, lineas: arr });
      }
    });
    return ordered;
  }, [data]);

  if (loading) {
    return (
      <div className={styles.panel}>
        <div className={styles.loading}>
          <div className="spinner" />
          <span>Cargando líneas de transporte…</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={styles.panel}>
        <div className={styles.errorState}>
          <p>{error}</p>
          {(fallbackLineas != null || fallbackParadas != null) && (
            <p className={styles.errorFallback}>
              {fallbackLineas != null ? `${fallbackLineas} líneas` : "—"}
              {" · "}
              {fallbackParadas != null ? `${fallbackParadas} paradas` : "—"}
              {" en el entorno."}
            </p>
          )}
        </div>
      </div>
    );
  }

  if (!data || data.total_lineas === 0) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>No hay líneas de transporte en {radioM} m.</div>
      </div>
    );
  }

  const tiposPresentes = new Set(grupos.map((g) => g.tipo));
  const soloBus = tiposPresentes.size === 1 && tiposPresentes.has("bus");

  return (
    <div className={styles.panel}>
      <header className={styles.header}>
        <span className={styles.headerKicker}>Transporte cercano</span>
        <p className={styles.headerCounter}>
          <strong>{data.total_lineas}</strong> líneas · <strong>{data.total_paradas}</strong> paradas en {radioM} m
        </p>
      </header>

      {soloBus && (
        <div
          style={{
            padding: "10px 12px",
            margin: "0 0 12px 0",
            borderRadius: 10,
            background: "rgba(245, 158, 11, 0.08)",
            border: "1px solid rgba(245, 158, 11, 0.28)",
            color: "var(--text-muted)",
            fontSize: 12,
            lineHeight: 1.5,
          }}
          role="note"
        >
          Solo detectamos líneas de bus en esta zona. Si esperabas metro / tram / FGC,
          puede ser que el pipeline de TMB no haya cargado aún esos tipos — revísalo
          en <code>pipelines/transporte/transporte.py</code>.
        </div>
      )}

      <div className={styles.groups}>
        {grupos.map(({ tipo, lineas }) => {
          const label = TIPO_LABEL[tipo as TransporteTipo] ?? String(tipo);
          return (
            <section key={tipo} className={styles.group}>
              <div className={styles.groupTitleRow}>
                <h4 className={styles.groupTitle}>{label}</h4>
                <span className={styles.groupCount}>{lineas.length}</span>
              </div>
              <div className={styles.badgeGrid}>
                {lineas.map((linea) => {
                  const key = `${tipo}-${linea.codigo}`;
                  const active = openKey === key;
                  const bg = colorForLinea(linea);
                  const fg = textColorFor(bg);
                  const isBus = linea.tipo === "bus";
                  return (
                    <div key={key} className={styles.badgeWrap}>
                      <button
                        type="button"
                        className={`${styles.badge} ${isBus ? styles.badgeBus : styles.badgeRail} ${active ? styles.badgeActive : ""}`}
                        style={{ background: bg, color: fg, borderColor: bg }}
                        onClick={() => setOpenKey(active ? null : key)}
                        aria-label={`${linea.tipo} línea ${linea.codigo}`}
                        aria-expanded={active}
                        title={linea.nombre}
                      >
                        {linea.codigo}
                      </button>
                      {active && <LineaPopover linea={linea} onClose={() => setOpenKey(null)} />}
                    </div>
                  );
                })}
              </div>
            </section>
          );
        })}
      </div>
    </div>
  );
}
