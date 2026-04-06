"use client";

import { useState } from "react";
import type { DevData } from "@/lib/types";
import styles from "./DevPanel.module.css";

interface Props {
  data: DevData | null;
  loading: boolean;
}

function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      width="12" height="12" viewBox="0 0 12 12" fill="none"
      className={`${styles.chevron} ${open ? styles.chevronOpen : ""}`}
    >
      <path d="M4.5 2.5L8 6l-3.5 3.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function DataRow({ label, value }: { label: string; value: unknown }) {
  const isNull = value === null || value === undefined;
  let display: string;
  if (isNull) {
    display = "null";
  } else if (typeof value === "number") {
    display = Number.isInteger(value) ? String(value) : value.toFixed(3);
  } else {
    display = String(value);
  }
  return (
    <div className={styles.dataRow}>
      <span className={styles.dataKey}>{label}</span>
      <span className={isNull ? styles.dataValNull : styles.dataVal}>{display}</span>
    </div>
  );
}

function Section({
  title,
  badge,
  defaultOpen = false,
  children,
}: {
  title: string;
  badge?: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className={styles.section}>
      <div className={styles.sectionHeader} onClick={() => setOpen(!open)}>
        <span className={styles.sectionTitle}>{title}</span>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {badge && <span className={styles.sectionBadge}>{badge}</span>}
          <Chevron open={open} />
        </div>
      </div>
      {open && <div className={styles.sectionBody}>{children}</div>}
    </div>
  );
}

function ShapBars({ shap }: { shap: Record<string, number> }) {
  const entries = Object.entries(shap).sort((a, b) => Math.abs(b[1]) - Math.abs(a[1])).slice(0, 15);
  const maxAbs = Math.max(...entries.map(([, v]) => Math.abs(v)), 0.01);
  return (
    <div>
      {entries.map(([key, val]) => {
        const pct = Math.min((Math.abs(val) / maxAbs) * 50, 50);
        return (
          <div key={key} className={styles.shapBar}>
            <span className={styles.shapLabel}>{key}</span>
            <div className={styles.shapBarWrap}>
              {val >= 0 ? (
                <div className={styles.shapFillPos} style={{ width: `${pct}%` }} />
              ) : (
                <div className={styles.shapFillNeg} style={{ width: `${pct}%` }} />
              )}
            </div>
            <span className={`${styles.shapVal} ${val >= 0 ? styles.shapPos : styles.shapNeg}`}>
              {val >= 0 ? "+" : ""}{val.toFixed(3)}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export default function DevPanel({ data, loading }: Props) {
  if (loading) {
    return (
      <div className={styles.devPanel}>
        <p style={{ color: "rgba(255,255,255,0.3)" }}>Cargando datos dev...</p>
      </div>
    );
  }

  if (!data) {
    return (
      <div className={styles.devPanel}>
        <p className={styles.emptyNote}>
          Dev data no disponible. Activa el modo dev para ver los datos internos del modelo.
        </p>
      </div>
    );
  }

  const flujo = data.flujo_fuentes;
  const trans = data.transporte;
  const comp = data.competencia_raw;
  const demo = data.demografia;
  const seg = data.seguridad;
  const ent = data.entorno;
  const tur = data.turismo;
  const ml = data.ml;

  return (
    <div className={styles.devPanel}>
      {/* Flujo Peatonal */}
      <Section title="Flujo Peatonal" badge="4 fuentes">
        <div className={styles.dataGrid}>
          <DataRow label="popular_times_score" value={flujo?.popular_times_score} />
          <DataRow label="vcity_flujo_peatonal" value={flujo?.vcity_flujo_peatonal} />
          <DataRow label="vianants_intensitat" value={flujo?.vianants_intensitat} />
          <DataRow label="ratio_locales_comerciales" value={flujo?.ratio_locales_comerciales} />
        </div>
      </Section>

      {/* Transporte */}
      <Section title="Transporte">
        <div className={styles.dataGrid}>
          <DataRow label="num_lineas" value={trans?.num_lineas} />
          <DataRow label="num_paradas_500m" value={trans?.num_paradas_500m} />
        </div>
      </Section>

      {/* Competencia */}
      <Section title="Competencia" badge={`${comp?.competidores?.length ?? 0} negocios`}>
        {comp?.competidores && comp.competidores.length > 0 ? (
          <table className={styles.compTable}>
            <thead>
              <tr>
                <th>Nombre</th>
                <th>Sector</th>
                <th>Rating</th>
                <th>Dist</th>
                <th>Tipo</th>
              </tr>
            </thead>
            <tbody>
              {comp.competidores.slice(0, 20).map((c, i) => (
                <tr key={i}>
                  <td>{c.nombre ?? "?"}</td>
                  <td>{c.sector ?? "-"}</td>
                  <td>{c.rating != null ? c.rating.toFixed(1) : "-"}</td>
                  <td>{c.distancia_m != null ? `${Math.round(c.distancia_m)}m` : "-"}</td>
                  <td>
                    {c.es_competencia_directa && <span className={styles.badgeDirect}>directo</span>}
                    {c.es_vulnerable && <span className={styles.badgeVulnerable}>vulnerable</span>}
                    {!c.es_competencia_directa && !c.es_vulnerable && <span className={styles.badgeCompl}>compl.</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className={styles.emptyNote}>Sin datos de competencia</p>
        )}
      </Section>

      {/* Demografia */}
      <Section title="Demografia">
        <div className={styles.dataGrid}>
          <DataRow label="poblacion" value={demo?.poblacion} />
          <DataRow label="edad_media" value={demo?.edad_media} />
          <DataRow label="renta_media_hogar" value={demo?.renta_media_hogar} />
          <DataRow label="pct_extranjeros" value={demo?.pct_extranjeros} />
          <DataRow label="densidad_hab_km2" value={demo?.densidad_hab_km2} />
          <DataRow label="pct_poblacio_25_44" value={demo?.pct_poblacio_25_44} />
          <DataRow label="delta_renta_3a" value={demo?.delta_renta_3a} />
          <DataRow label="nivel_estudios_alto_pct" value={demo?.nivel_estudios_alto_pct} />
        </div>
      </Section>

      {/* Seguridad */}
      <Section title="Seguridad" defaultOpen>
        <div className={styles.dataGrid}>
          <DataRow label="incidencias_por_1000hab" value={seg?.incidencias_por_1000hab} />
          <DataRow label="hurtos_por_1000hab" value={seg?.hurtos_por_1000hab} />
          <DataRow label="robatoris_por_1000hab" value={seg?.robatoris_por_1000hab} />
          <DataRow label="danys_por_1000hab" value={seg?.danys_por_1000hab} />
          <DataRow label="incidencias_noche_pct" value={seg?.incidencias_noche_pct} />
          <DataRow label="comisarias_1km" value={seg?.comisarias_1km} />
          <DataRow label="dist_comisaria_m" value={seg?.dist_comisaria_m} />
          <DataRow label="seguridad_barri_score" value={seg?.seguridad_barri_score} />
        </div>
      </Section>

      {/* Entorno Comercial */}
      <Section title="Entorno Comercial" badge="6 sub-scores" defaultOpen>
        <div className={styles.dataGrid}>
          <DataRow label="pct_locales_vacios" value={ent?.pct_locales_vacios} />
          <DataRow label="tasa_rotacion_anual" value={ent?.tasa_rotacion_anual} />
          <DataRow label="licencias_nuevas_1a" value={ent?.licencias_nuevas_1a} />
          <DataRow label="ratio_locales_comerciales" value={ent?.ratio_locales_comerciales} />
          <DataRow label="nivel_ruido_db" value={ent?.nivel_ruido_db} />
          <DataRow label="score_equipamientos" value={ent?.score_equipamientos} />
          <DataRow label="m2_zonas_verdes" value={ent?.m2_zonas_verdes_cercanas} />
          <DataRow label="mercados_municipales_1km" value={ent?.mercados_municipales_1km} />
          <DataRow label="eventos_culturales_500m" value={ent?.eventos_culturales_500m} />
        </div>
      </Section>

      {/* Turismo */}
      <Section title="Turismo">
        <div className={styles.dataGrid}>
          <DataRow label="score_turismo" value={tur?.score_turismo} />
          <DataRow label="airbnb_density_500m" value={tur?.airbnb_density_500m} />
          <DataRow label="booking_hoteles_500m" value={tur?.booking_hoteles_500m} />
        </div>
      </Section>

      {/* ML / SHAP */}
      <Section title="ML / SHAP" badge={ml?.modelo_version ?? "N/A"}>
        <div className={styles.dataGrid}>
          <DataRow label="modelo_version" value={ml?.modelo_version} />
          <DataRow label="prob_supervivencia" value={ml?.probabilidad_supervivencia} />
        </div>
        {ml?.shap_values && Object.keys(ml.shap_values).length > 0 ? (
          <>
            <div style={{ marginTop: 12, marginBottom: 4, color: "rgba(255,255,255,0.4)", fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>
              SHAP Values (top 15)
            </div>
            <ShapBars shap={ml.shap_values} />
          </>
        ) : (
          <p className={styles.emptyNote}>SHAP values no disponibles (scorer manual)</p>
        )}
      </Section>
    </div>
  );
}
