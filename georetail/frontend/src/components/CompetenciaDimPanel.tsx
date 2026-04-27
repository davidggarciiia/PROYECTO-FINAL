"use client";

import type { CompetidorCercano } from "@/lib/types";
import styles from "./CompetenciaDimPanel.module.css";

interface CompetenciaDimPanelProps {
  hhi_index?: number | null;
  num_directos?: number | null;
  competidores_cercanos?: CompetidorCercano[];
  score?: number | null;
  explicacion_bullets?: string[];
}

function fmt1(n: number): string {
  return n.toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtInt(n: number): string {
  return n.toLocaleString("es-ES", { maximumFractionDigits: 0 });
}

function truncate(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function hhiColor(hhi: number): string {
  if (hhi < 0.25) return "var(--green)";
  if (hhi < 0.5) return "var(--yellow)";
  return "var(--red)";
}

function hhiLabel(hhi: number): string {
  if (hhi < 0.25) return "Mercado fragmentado";
  if (hhi < 0.5) return "Concentración media";
  return "Mercado concentrado";
}

function competitorMarketLabel(count: number): { text: string; color: string; bg: string; border: string } {
  if (count <= 2) return { text: "Mercado libre", color: "var(--green)", bg: "rgba(16,185,129,0.1)", border: "rgba(16,185,129,0.3)" };
  if (count <= 5) return { text: "Competencia moderada", color: "var(--yellow)", bg: "rgba(245,158,11,0.1)", border: "rgba(245,158,11,0.3)" };
  return { text: "Mercado competido", color: "var(--red)", bg: "rgba(239,68,68,0.1)", border: "rgba(239,68,68,0.3)" };
}

export default function CompetenciaDimPanel({
  hhi_index,
  num_directos,
  competidores_cercanos,
  score,
  explicacion_bullets,
}: CompetenciaDimPanelProps) {
  const hasAny =
    hhi_index != null ||
    num_directos != null ||
    (competidores_cercanos && competidores_cercanos.length > 0) ||
    score != null ||
    (explicacion_bullets && explicacion_bullets.length > 0);

  if (!hasAny) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Sin datos de competencia disponibles para esta zona.
        </div>
      </div>
    );
  }

  const ringPct = hhi_index != null ? Math.min(Math.max(hhi_index * 100, 0), 100) : 0;
  const ringColor = hhi_index != null ? hhiColor(hhi_index) : "var(--surface-3)";

  const directCount = num_directos ?? 0;
  const marketLabel = competitorMarketLabel(directCount);

  const top5 = (competidores_cercanos ?? [])
    .slice()
    .sort((a, b) => (a.distancia_m ?? 9999) - (b.distancia_m ?? 9999))
    .slice(0, 5);

  const maxDist = 500;

  return (
    <div className={styles.panel}>

      {/* ── Market concentration ring ───────────────────────── */}
      {hhi_index != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Concentración de mercado</div>
          <div className={styles.ringRow}>
            <div
              className={styles.ring}
              style={{
                background: `conic-gradient(${ringColor} calc(${ringPct} * 1%), var(--surface-3) 0)`,
              }}
              role="img"
              aria-label={`HHI: ${fmt1(hhi_index)}`}
            >
              <div className={styles.ringInner}>
                <span className={styles.ringValue}>{fmt1(hhi_index)}</span>
                <span className={styles.ringUnit}>HHI</span>
              </div>
            </div>
            <div className={styles.ringMeta}>
              <span className={styles.ringLabel}>Índice HHI</span>
              <span
                className={styles.ringChip}
                style={{
                  background:
                    hhi_index < 0.25
                      ? "rgba(16,185,129,0.12)"
                      : hhi_index < 0.5
                      ? "rgba(245,158,11,0.12)"
                      : "rgba(239,68,68,0.12)",
                  borderColor:
                    hhi_index < 0.25
                      ? "rgba(16,185,129,0.3)"
                      : hhi_index < 0.5
                      ? "rgba(245,158,11,0.3)"
                      : "rgba(239,68,68,0.3)",
                  color: ringColor,
                }}
              >
                {hhiLabel(hhi_index)}
              </span>
              <p className={styles.ringSubtext}>
                {hhi_index < 0.25
                  ? "Muchos actores, nadie domina. Buen escenario para entrar."
                  : hhi_index < 0.5
                  ? "Algunos operadores fuertes. Diferenciación clave."
                  : "Pocos actores dominantes. Barrera de entrada alta."}
              </p>
            </div>
          </div>
        </section>
      )}

      {/* ── Competitor count hero ───────────────────────────── */}
      {num_directos != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Competidores directos cercanos</div>
          <div className={styles.heroRow}>
            <span className={styles.heroNumber}>{fmtInt(directCount)}</span>
            <div className={styles.heroRight}>
              <span
                className={styles.heroChip}
                style={{
                  background: marketLabel.bg,
                  borderColor: marketLabel.border,
                  color: marketLabel.color,
                }}
              >
                {marketLabel.text}
              </span>
              <span className={styles.heroSubtext}>
                {directCount === 0
                  ? "No se detectan competidores directos en el radio."
                  : directCount <= 2
                  ? "Espacio de mercado disponible en la zona."
                  : directCount <= 5
                  ? "Presencia competidora moderada. Diferenciación necesaria."
                  : "Alta densidad competitiva. Analiza puntos de diferenciación."}
              </span>
            </div>
          </div>
        </section>
      )}

      {/* ── Competitor list ─────────────────────────────────── */}
      {top5.length > 0 && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>
            Más cercanos ({top5.length} de {competidores_cercanos?.length ?? 0})
          </div>
          <div className={styles.compList}>
            {top5.map((c, i) => {
              const dist = c.distancia_m ?? 0;
              const distPct = Math.min((dist / maxDist) * 100, 100);
              const isDirect = c.es_competencia_directa;
              const isVulnerable = c.es_vulnerable;
              const badge = c.subsector ?? c.sector ?? "—";

              return (
                <div key={i} className={styles.compRow}>
                  <div className={styles.compRowTop}>
                    <span className={styles.compName} title={c.nombre}>
                      {truncate(c.nombre, 28)}
                    </span>
                    <div className={styles.compBadges}>
                      {isDirect && (
                        <span className={styles.badgeDirect}>Directo</span>
                      )}
                      {isVulnerable && (
                        <span className={styles.badgeVuln}>Vulnerable</span>
                      )}
                      {c.es_complementario && (
                        <span className={styles.badgeComp}>Sinergia</span>
                      )}
                    </div>
                  </div>
                  <div className={styles.compRowMid}>
                    <span className={styles.compSector}>{truncate(badge, 22)}</span>
                    {c.rating != null && (
                      <span className={styles.compRating}>
                        {c.rating.toFixed(1)} ★
                      </span>
                    )}
                  </div>
                  <div className={styles.compDistRow}>
                    <div className={styles.compDistTrack} role="presentation">
                      <div
                        className={styles.compDistFill}
                        style={{ width: `${distPct}%` }}
                      />
                    </div>
                    <span className={styles.compDistLabel}>
                      {dist < 1000
                        ? `${fmtInt(dist)} m`
                        : `${(dist / 1000).toFixed(1)} km`}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {/* ── Score ring ──────────────────────────────────────── */}
      {score != null && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Score competencia</div>
          <div className={styles.scoreRow}>
            <div
              className={styles.scoreRing}
              style={{
                background: `conic-gradient(var(--accent-light) calc(${score} * 1%), var(--surface-3) 0)`,
              }}
              role="img"
              aria-label={`Score competencia: ${Math.round(score)} de 100`}
            >
              <div className={styles.scoreRingInner}>
                <span className={styles.scoreNumber}>{Math.round(score)}</span>
              </div>
            </div>
            <span className={styles.scoreLabel}>/ 100</span>
          </div>
        </section>
      )}

      {/* ── Bullets ─────────────────────────────────────────── */}
      {explicacion_bullets && explicacion_bullets.length > 0 && (
        <section className={styles.section}>
          <div className={styles.sectionKicker}>Hechos clave</div>
          <div className={styles.bulletRow}>
            {explicacion_bullets.map((b, i) => (
              <span key={i} className={styles.bulletPill}>
                {b}
              </span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
