"use client";

/**
 * /admin/competencia — Panel interno de auditoría de Competencia.
 *
 * Consume GET /api/admin/competencia y muestra:
 *   - Última ejecución del pipeline `competencia` + estado.
 *   - Por barrio: % de zonas con datos recientes (<=7d) en
 *     `competencia_detalle_zona`, HHI medio, cadenas dominantes (HHI>0.25).
 *   - Filas "en rojo" para barrios con <80 % de cobertura reciente.
 *
 * Pensado para el equipo: abrir cada mañana para ver si el scraper ha caído
 * o si un distrito se ha quedado sin datos.
 */

import { useEffect, useState } from "react";
import styles from "./page.module.css";

interface CoberturaBarrio {
  barrio: string;
  distrito: string;
  zonas_totales: number;
  zonas_con_datos_recientes: number;
  pct_recientes: number;
  hhi_medio: number | null;
  cadenas_dominantes: number;
}

interface CompetenciaAdminResponse {
  total_zonas: number;
  total_filas_vigentes: number;
  ultima_ejecucion: string | null;
  ultimo_estado: string | null;
  por_barrio: CoberturaBarrio[];
}

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export default function AdminCompetenciaPage() {
  const [data, setData] = useState<CompetenciaAdminResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`${API_URL}/api/admin/competencia`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <div className={styles.errorBox}>Error: {error}</div>;
  if (!data) return <div className={styles.loading}>Cargando…</div>;

  const barriosEnRojo = data.por_barrio.filter((b) => b.pct_recientes < 80).length;

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <h1>Competencia · Auditoría</h1>
        <div className={styles.meta}>
          <Badge label="Zonas" value={data.total_zonas.toString()} />
          <Badge label="Filas vigentes (7d)" value={data.total_filas_vigentes.toString()} />
          <Badge
            label="Última ejecución"
            value={data.ultima_ejecucion
              ? new Date(data.ultima_ejecucion).toLocaleString("es-ES")
              : "—"}
          />
          <Badge
            label="Estado"
            value={data.ultimo_estado ?? "—"}
            color={data.ultimo_estado === "ok" ? "green" : "amber"}
          />
          <Badge
            label="Barrios en rojo"
            value={barriosEnRojo.toString()}
            color={barriosEnRojo > 0 ? "red" : "green"}
          />
        </div>
      </header>

      <table className={styles.table}>
        <thead>
          <tr>
            <th>Barrio</th>
            <th>Distrito</th>
            <th className={styles.num}>Zonas</th>
            <th className={styles.num}>Recientes (7d)</th>
            <th className={styles.num}>% recientes</th>
            <th className={styles.num}>HHI medio</th>
            <th className={styles.num}>Cadenas ≥0.25</th>
          </tr>
        </thead>
        <tbody>
          {data.por_barrio.map((b) => (
            <tr
              key={`${b.distrito}-${b.barrio}`}
              className={b.pct_recientes < 80 ? styles.rowRed : ""}
            >
              <td>{b.barrio}</td>
              <td className={styles.distrito}>{b.distrito}</td>
              <td className={styles.num}>{b.zonas_totales}</td>
              <td className={styles.num}>{b.zonas_con_datos_recientes}</td>
              <td className={styles.num}>
                <Barra valor={b.pct_recientes} color={b.pct_recientes >= 80 ? "#22c55e" : "#ef4444"} />
                {b.pct_recientes.toFixed(1)}
              </td>
              <td className={styles.num}>
                {b.hhi_medio != null ? b.hhi_medio.toFixed(3) : "—"}
              </td>
              <td className={styles.num}>{b.cadenas_dominantes}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <footer className={styles.footer}>
        Un barrio en rojo tiene menos del 80 % de zonas con datos frescos (últimos 7 días).
        Revisa <code>pipelines.comercio.competencia</code> y <code>pipelines.entorno.google_maps</code>.
      </footer>
    </div>
  );
}

function Badge({ label, value, color }: { label: string; value: string; color?: "green" | "amber" | "red" }) {
  return (
    <span className={`${styles.badge} ${color ? styles[`badge_${color}`] : ""}`}>
      <span className={styles.badgeLabel}>{label}</span>
      <span className={styles.badgeValue}>{value}</span>
    </span>
  );
}

function Barra({ valor, color }: { valor: number; color: string }) {
  const w = Math.max(2, Math.min(100, valor));
  return (
    <span className={styles.barra}>
      <span className={styles.barraFill} style={{ width: `${w}%`, background: color }} />
    </span>
  );
}
