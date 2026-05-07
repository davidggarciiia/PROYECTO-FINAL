"use client";

import type { CompetenciaDetalle, CompetidorDetalle } from "@/lib/types";
import styles from "./CompetenciaExtra.module.css";

interface Props {
  competencia: CompetenciaDetalle | null;
  loading: boolean;
}

/**
 * CompetenciaExtra — bloque que se inyecta dentro de la sección "Competencia"
 * del editorial para mostrar el resultado del análisis LLM relacional:
 * amenazas (competencia directa real), oportunidades (vulnerables) y
 * sinérgicos (complementarios). Reemplaza al antiguo CompetenciaPanel grande
 * que vivía en su propia tab.
 *
 * Si /api/competencia aún está cargando, muestra un placeholder breve.
 * Si no hay datos, no renderiza nada — el CompetenciaDimPanel ya cubre el
 * mínimo (HHI + número directos + minimapa).
 */
export default function CompetenciaExtra({ competencia, loading }: Props) {
  if (loading) {
    return (
      <div className={styles.loading}>
        <span className={styles.loadingDot} />
        Analizando competencia con LLM…
      </div>
    );
  }

  if (!competencia) return null;

  const amenazas = (competencia.amenaza ?? []).slice(0, 6);
  const oportunidades = (competencia.oportunidad ?? []).slice(0, 4);
  const sinergicos = (competencia.sinergicos ?? []).slice(0, 4);

  const hasAlgo = amenazas.length + oportunidades.length + sinergicos.length > 0;
  if (!hasAlgo) return null;

  return (
    <div className={styles.wrap}>
      <div className={styles.kpiRow}>
        <KPI
          label="Amenazas reales"
          value={amenazas.length}
          tone="red"
          hint="competencia directa que comparte público"
        />
        <KPI
          label="Oportunidades"
          value={oportunidades.length}
          tone="yellow"
          hint="competidores vulnerables (rating bajo)"
        />
        <KPI
          label="Sinérgicos"
          value={sinergicos.length}
          tone="green"
          hint="negocios que generan flujo cruzado"
        />
        {competencia.hhi_index != null && (
          <KPI
            label="HHI"
            value={competencia.hhi_index.toFixed(2)}
            tone="accent"
            hint={
              competencia.hhi_index < 0.20
                ? "mercado fragmentado"
                : competencia.hhi_index < 0.40
                  ? "mercado equilibrado"
                  : "mercado concentrado"
            }
          />
        )}
      </div>

      {amenazas.length > 0 && (
        <Lista
          eyebrow="AMENAZAS · COMPETENCIA DIRECTA"
          tone="red"
          items={amenazas}
          mostrarMotivo
        />
      )}

      {oportunidades.length > 0 && (
        <Lista
          eyebrow="OPORTUNIDADES · COMPETIDORES VULNERABLES"
          tone="yellow"
          items={oportunidades}
        />
      )}

      {sinergicos.length > 0 && (
        <Lista
          eyebrow="SINÉRGICOS · FLUJO CRUZADO"
          tone="green"
          items={sinergicos}
        />
      )}
    </div>
  );
}

/* ── KPI mini-card ─────────────────────────────────────────────── */

function KPI({
  label,
  value,
  tone,
  hint,
}: {
  label: string;
  value: number | string;
  tone: "red" | "yellow" | "green" | "accent";
  hint: string;
}) {
  return (
    <div className={`${styles.kpi} ${styles[`kpi_${tone}`]}`}>
      <span className={styles.kpiLabel}>{label}</span>
      <span className={styles.kpiValue}>{value}</span>
      <span className={styles.kpiHint}>{hint}</span>
    </div>
  );
}

/* ── Lista de competidores ─────────────────────────────────────── */

function Lista({
  eyebrow,
  tone,
  items,
  mostrarMotivo = false,
}: {
  eyebrow: string;
  tone: "red" | "yellow" | "green";
  items: CompetidorDetalle[];
  mostrarMotivo?: boolean;
}) {
  return (
    <section className={styles.lista}>
      <div className={`${styles.listaEyebrow} ${styles[`eye_${tone}`]}`}>
        <span className={styles.listaTick}>●</span>
        {eyebrow}
        <span className={styles.listaCount}>{items.length}</span>
      </div>
      <ul className={styles.items}>
        {items.map((c, i) => (
          <li key={c.id ?? `${c.nombre}-${i}`} className={`${styles.item} ${styles[`item_${tone}`]}`}>
            <div className={styles.itemHead}>
              <span className={styles.itemNum}>
                {String(i + 1).padStart(2, "0")}
              </span>
              <div className={styles.itemBody}>
                <div className={styles.itemNombre}>{c.nombre}</div>
                <div className={styles.itemMeta}>
                  {c.subsector && <span>{c.subsector}</span>}
                  {c.distancia_m != null && (
                    <>
                      {c.subsector && <span className={styles.dot}>·</span>}
                      <span>{Math.round(c.distancia_m)} m</span>
                    </>
                  )}
                  {c.rating != null && (
                    <>
                      <span className={styles.dot}>·</span>
                      <span className={styles.rating}>★ {c.rating.toFixed(1)}</span>
                      {c.num_resenas != null && (
                        <span className={styles.dim}>({c.num_resenas})</span>
                      )}
                    </>
                  )}
                </div>
              </div>
              {c.amenaza_score != null && (
                <span
                  className={`${styles.amenazaScore} ${styles[`amenaza_${tone}`]}`}
                  title={`Score de amenaza ${Math.round(c.amenaza_score)}/100`}
                >
                  {Math.round(c.amenaza_score)}
                </span>
              )}
            </div>
            {mostrarMotivo && c.motivo_relacion && (
              <p className={styles.motivo}>{c.motivo_relacion}</p>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}
