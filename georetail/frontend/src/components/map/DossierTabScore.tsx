"use client";

import { Fragment, useState } from "react";
import type {
  ZonaPreview,
  LocalDetalleResponse,
  ScoresDimensiones,
  ExplicacionDimension,
} from "@/lib/types";
import {
  DIMENSIONS,
  type DimensionKey,
  type DimensionMeta,
  type DimensionBreakdownRow,
} from "./dimensions";
import styles from "./Dossier.module.css";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
}

type Band = "hi" | "mid" | "lo" | "na";

function scoreBand(score?: number | null): Band {
  if (score == null) return "na";
  if (score >= 75) return "hi";
  if (score >= 55) return "mid";
  return "lo";
}

function bandLabel(band: Band): string {
  if (band === "hi") return "Punto fuerte";
  if (band === "mid") return "Aceptable";
  if (band === "lo") return "Punto débil";
  return "Sin datos";
}

function clamp(n: number, lo = 0, hi = 100): number {
  return Math.max(lo, Math.min(hi, n));
}

/** Desktop grid has 2 columns (see Dossier.module.css :.dimGrid). */
const GRID_COLS = 2;

/* ──────────────────────────────────────────────────────────────
   DimDrawer — layout espejo del prototype_v1/detail.jsx (133-190).
   Se renderiza full-width debajo de la fila donde esté la celda
   abierta. Consume explicaciones_dimensiones cuando existe.
   ────────────────────────────────────────────────────────────── */
interface DrawerProps {
  dim: DimensionMeta;
  value: number | null | undefined;
  explicacion?: ExplicacionDimension;
  peso?: number;           // 0-1, peso de esta dimensión para el sector
  sectorCodigo?: string;   // sector clasificado ("restauracion", etc.)
}

/**
 * Explica por qué una dimensión tiene el peso que tiene para este sector.
 * No viene del LLM — es heurística estable conocida por el modelo manual_v2.
 */
function razonPeso(
  dimKey: DimensionKey,
  sectorCodigo: string | undefined,
  peso: number,
): string {
  const pct = Math.round(peso * 100);
  const sector = sectorCodigo ?? "tu negocio";
  const altoBajo = peso >= 0.18 ? "alto" : peso >= 0.10 ? "medio" : "bajo";
  const intros: Partial<Record<DimensionKey, string>> = {
    flujo_peatonal: `El flujo peatonal pesa ${pct}% (${altoBajo}) para ${sector} porque el volumen de paso determina cuántos clientes ven el escaparate cada día.`,
    demografia: `La demografía pesa ${pct}% (${altoBajo}) para ${sector} porque la renta, edad y nivel educativo marcan el público objetivo y el ticket medio posible.`,
    competencia: `La competencia pesa ${pct}% (${altoBajo}) para ${sector} — demasiados negocios del mismo tipo cerca reducen tu cuota y presionan precios.`,
    transporte: `El transporte pesa ${pct}% (${altoBajo}) para ${sector} porque amplía el radio de captación más allá del barrio inmediato.`,
    seguridad: `La seguridad pesa ${pct}% (${altoBajo}) para ${sector} — importa más cuanto más dure el servicio en franja tarde-noche.`,
    turismo: `El turismo pesa ${pct}% (${altoBajo}) para ${sector} porque el perfil estacional condiciona ingresos recurrentes vs picos.`,
    dinamismo: `El dinamismo pesa ${pct}% (${altoBajo}) para ${sector} — una zona emergente puede hacer crecer tu local, una en declive lo hunde.`,
    precio_alquiler: `El precio del alquiler pesa ${pct}% (${altoBajo}) para ${sector} como restricción financiera: determina el punto muerto de tu P&L.`,
  };
  return intros[dimKey] ?? `Esta dimensión pesa ${pct}% para ${sector}.`;
}

function DimDrawer({ dim, value, explicacion, peso, sectorCodigo }: DrawerProps) {
  const band = scoreBand(value ?? null);
  const label = bandLabel(band);

  // TODO: reemplazar por desglose real cuando el backend lo devuelva.
  const baseValue = value ?? 60;
  const breakdownRows: Array<DimensionBreakdownRow & { value: number }> =
    dim.breakdown.map((row) => ({
      ...row,
      value: Math.round(clamp(baseValue + row.offset)),
    }));

  // Copy para "Para este local": usamos explicacion del API cuando la hay.
  const interpTitular = explicacion?.titular?.trim();
  const interpSubtitular = explicacion?.explicacion_corta?.trim();
  const bullets: Array<{ kind: "up" | "down" | "fact"; text: string }> = [];
  (explicacion?.porque_sube ?? []).forEach((t) =>
    bullets.push({ kind: "up", text: t }),
  );
  (explicacion?.porque_baja ?? []).forEach((t) =>
    bullets.push({ kind: "down", text: t }),
  );
  (explicacion?.hechos_clave ?? []).forEach((t) =>
    bullets.push({ kind: "fact", text: t }),
  );
  const hasInterp = Boolean(interpTitular || interpSubtitular || bullets.length);

  // Sources: si el API devuelve fuentes, las priorizamos, si no usamos las por defecto.
  const sources =
    explicacion?.fuentes && explicacion.fuentes.length > 0
      ? explicacion.fuentes
      : dim.sources;

  return (
    <div
      className={`${styles.dimDrawerFull} ${styles[`drawer_${band}`]}`}
      role="region"
      aria-label={`Detalle dimensión ${dim.name}`}
    >
      {/* ── Header ─────────────────────────────────────── */}
      <div className={styles.drawerHeader}>
        <div className={styles.drawerHeaderLeft}>
          <div className={styles.drawerTitleRow}>
            <h3 className={styles.drawerTitle}>{dim.name}</h3>
            <span className={`${styles.drawerBand} ${styles[`drawerBand_${band}`]}`}>
              {label}
            </span>
            {peso != null && peso > 0 && (
              <span className={styles.drawerWeightPill} title="Peso de esta dimensión en el score global para tu idea">
                {Math.round(peso * 100)}% peso
              </span>
            )}
          </div>
          <p className={styles.drawerSubtitleNew}>{dim.subtitle}</p>
        </div>
        <div className={styles.drawerHeaderRight}>
          <div className={styles.drawerScoreBig}>
            {value != null ? Math.round(value) : "—"}
            <sup>/100</sup>
          </div>
          <div className={styles.drawerScoreLabel}>{dim.name}</div>
        </div>
      </div>

      {/* ── Body 2 columnas ────────────────────────────── */}
      <div className={styles.drawerBody}>
        {/* Columna izquierda: Qué mide + Para este local + sources */}
        <div className={styles.drawerBodyLeft}>
          <div className={styles.drawerSectionLabel}>Qué mide</div>
          <p className={styles.drawerWhatNew}>{dim.what}</p>

          <div className={styles.drawerSectionLabel}>Para este local</div>
          {hasInterp ? (
            <div className={styles.drawerInterp}>
              {interpTitular && (
                <p className={styles.drawerInterpLead}>
                  <strong>{interpTitular}</strong>
                </p>
              )}
              {interpSubtitular && (
                <p className={styles.drawerInterpText}>{interpSubtitular}</p>
              )}
              {bullets.length > 0 && (
                <ul className={styles.drawerInterpList}>
                  {bullets.map((b, i) => (
                    <li key={i} className={styles[`interpBullet_${b.kind}`]}>
                      <span className={styles.interpBulletIcon} aria-hidden>
                        {b.kind === "up" ? "▲" : b.kind === "down" ? "▼" : "·"}
                      </span>
                      <span>{b.text}</span>
                    </li>
                  ))}
                </ul>
              )}
              {explicacion?.confianza && (
                <div className={styles.drawerConfidence}>
                  Confianza del modelo: <strong>{explicacion.confianza}</strong>
                </div>
              )}
            </div>
          ) : (
            <div className={styles.drawerInterp}>
              <p className={styles.drawerInterpLead}>
                <strong>
                  {band === "hi"
                    ? `${dim.name} es un punto fuerte en esta zona.`
                    : band === "mid"
                    ? `${dim.name} está en un nivel aceptable.`
                    : band === "lo"
                    ? `${dim.name} es un punto débil a vigilar.`
                    : "Sin datos suficientes para evaluar esta dimensión."}
                </strong>
              </p>
              <p className={styles.drawerInterpText}>{dim.what}</p>
              {value != null && (
                <p className={styles.drawerInterpText}>
                  Score obtenido: <strong>{Math.round(value)}/100</strong>. El
                  modelo agrega tus resultados según los pesos específicos de tu
                  idea — revisa la sección <em>Por qué pesa así</em> para ver
                  cuánto influye esta dimensión en el score global.
                </p>
              )}
              <span className={styles.drawerInterpMuted}>
                Interpretación IA específica no disponible — texto genérico.
              </span>
            </div>
          )}

          {/* ── Razonamiento del peso para esta idea ──────────────────── */}
          {peso != null && peso > 0 && (
            <div className={styles.drawerWeightExplain}>
              <div className={styles.drawerSectionLabel}>Por qué pesa así</div>
              <p className={styles.drawerInterpText}>
                {razonPeso(dim.key, sectorCodigo, peso)}
              </p>
            </div>
          )}

          <div className={styles.drawerSources}>
            {sources.map((s) => (
              <span key={s} className={styles.sourceChip}>
                {s}
              </span>
            ))}
          </div>
        </div>

        {/* Columna derecha: Desglose */}
        <div className={styles.drawerBodyRight}>
          <div className={styles.drawerSectionLabel}>Desglose</div>
          <div className={styles.drawerBreakdown}>
            {breakdownRows.map((row, i) => {
              const rowBand = scoreBand(row.value);
              return (
                <div
                  key={i}
                  className={`${styles.breakdownRow} ${styles[`bd_${rowBand}`]}`}
                >
                  <div className={styles.breakdownText}>
                    <div className={styles.breakdownLabel}>{row.label}</div>
                    <div className={styles.breakdownMeta}>{row.meta}</div>
                  </div>
                  <div className={styles.breakdownBar}>
                    <span style={{ width: `${row.value}%` }} />
                  </div>
                  <div className={styles.breakdownValue}>{row.value}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────────────────────
   Tab principal
   ────────────────────────────────────────────────────────────── */
export default function DossierTabScore({ zone, detalle, loading }: Props) {
  const [openKey, setOpenKey] = useState<DimensionKey | null>(null);

  const dims: ScoresDimensiones = detalle?.zona.scores_dimensiones ?? {};
  const prob = detalle?.zona.probabilidad_supervivencia;
  const explicaciones: Record<string, ExplicacionDimension> =
    detalle?.zona.explicaciones_dimensiones ??
    detalle?.zona.analisis_ia?.explicaciones_dimensiones ??
    {};
  const pesos = detalle?.zona.pesos_dimensiones ?? {};
  const sectorCodigo = detalle?.zona.sector_codigo;

  if (loading && !detalle) {
    return (
      <div className={styles.loading}>
        <div className="spinner" />
        <span>Calculando análisis…</span>
      </div>
    );
  }

  const openIdx = openKey
    ? DIMENSIONS.findIndex((d) => d.key === openKey)
    : -1;
  const openRow = openIdx >= 0 ? Math.floor(openIdx / GRID_COLS) : -1;
  const openDim = openIdx >= 0 ? DIMENSIONS[openIdx] : null;

  // Agrupamos las celdas en "filas lógicas" para poder insertar la drawer
  // inmediatamente después de la fila donde está abierta.
  const rows: DimensionMeta[][] = [];
  for (let i = 0; i < DIMENSIONS.length; i += GRID_COLS) {
    rows.push(DIMENSIONS.slice(i, i + GRID_COLS));
  }

  return (
    <div className={styles.scoreTab}>
      {prob != null && (
        <div className={styles.probBanner}>
          <div className={styles.probLabel}>PROBABILIDAD SUPERVIVENCIA 3 AÑOS</div>
          <div className={styles.probValue}>
            {Math.round(prob * 100)}
            <span className={styles.probPct}>%</span>
          </div>
          <div className={styles.probBarTrack}>
            <div
              className={styles.probBarFill}
              style={{ width: `${Math.round(prob * 100)}%` }}
            />
          </div>
        </div>
      )}

      <div className={styles.dimGridStack}>
        {rows.map((rowDims, rowIdx) => (
          <Fragment key={`row-${rowIdx}`}>
            <div className={styles.dimGrid}>
              {rowDims.map((dim) => {
                const val = dims[dim.key];
                const band = scoreBand(val);
                const isOpen = openKey === dim.key;
                return (
                  <button
                    key={dim.key}
                    type="button"
                    className={`${styles.dimCell} ${styles[`dim_${band}`]} ${
                      isOpen ? styles.dimCellOpen : ""
                    }`}
                    onClick={() => setOpenKey(isOpen ? null : dim.key)}
                    aria-expanded={isOpen}
                  >
                    <div className={styles.dimCellHead}>
                      <span className={styles.dimCellName}>{dim.name}</span>
                      <span className={styles.dimCellVal}>
                        {val != null ? Math.round(val) : "—"}
                      </span>
                    </div>
                    <div className={styles.dimCellBarTrack}>
                      <div
                        className={styles.dimCellBarFill}
                        style={{
                          width:
                            val != null
                              ? `${Math.max(0, Math.min(100, val))}%`
                              : "0%",
                        }}
                      />
                    </div>
                    <div className={styles.dimCellFoot}>
                      <span>{dim.short}</span>
                      {pesos[dim.key] != null && pesos[dim.key] > 0 ? (
                        <span className={styles.dimCellWeight}>
                          {Math.round(pesos[dim.key] * 100)}% peso
                        </span>
                      ) : (
                        <span className={styles.dimCellHint}>{dim.hint}</span>
                      )}
                    </div>
                  </button>
                );
              })}
            </div>

            {rowIdx === openRow && openDim && (
              <DimDrawer
                dim={openDim}
                value={dims[openDim.key]}
                explicacion={explicaciones[openDim.key]}
                peso={pesos[openDim.key]}
                sectorCodigo={sectorCodigo}
              />
            )}
          </Fragment>
        ))}
      </div>

      {detalle?.zona.analisis_ia?.resumen_global && (
        <div className={styles.summary}>
          <div className={styles.summaryEyebrow}>
            <span className={styles.tick}>●</span>RESUMEN IA
          </div>
          <p className={styles.summaryText}>{detalle.zona.analisis_ia.resumen_global}</p>
        </div>
      )}
    </div>
  );
}
