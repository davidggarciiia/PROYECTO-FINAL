"use client";

import type { ReactNode } from "react";
import type { PerfilRefinado, ZonaDetalle } from "@/lib/types";
import { DIMENSIONS } from "./dimensions";
import styles from "./DossierTabPerfil.module.css";

interface Props {
  zona?: ZonaDetalle | null;
}

// Etiquetas legibles ES para los enums del backend.
const MODELO_SERVICIO_LABEL: Record<string, string> = {
  take_away: "Take away",
  mesas: "Mesas",
  mixto: "Mixto",
  delivery_only: "Solo delivery",
};
const ESCALA_LABEL: Record<string, string> = {
  solo: "Emprendedor solo",
  micro: "Micro (2-5)",
  "pequeña": "Pequeña (6-15)",
  mediana: "Mediana (15+)",
};
const NIVEL_SOCIO_LABEL: Record<string, string> = {
  bajo: "Renta baja",
  medio: "Renta media",
  "medio-alto": "Renta media-alta",
  alto: "Renta alta",
};
const DENSIDAD_LABEL: Record<string, string> = {
  baja: "Densidad baja",
  media: "Densidad media",
  alta: "Densidad alta",
};

function capitalize(value: string): string {
  if (!value) return value;
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function ticketStr(tier: number | null | undefined): string | null {
  if (tier == null) return null;
  if (tier < 1 || tier > 5) return null;
  return "€".repeat(Math.round(tier));
}

function isEmptyPerfil(perfil: PerfilRefinado | null | undefined): boolean {
  if (!perfil) return true;
  const { publico_objetivo, propuesta_valor, operacion, ubicacion_ideal, nuances_detected } = perfil;
  const hasPublico =
    Boolean(publico_objetivo?.edad_rango) ||
    Boolean(publico_objetivo?.nivel_socioeconomico) ||
    (publico_objetivo?.estilo_vida ?? []).length > 0 ||
    (publico_objetivo?.horarios_pico ?? []).length > 0;
  const hasPropuesta =
    Boolean(propuesta_valor?.especializacion) ||
    (propuesta_valor?.diferenciadores ?? []).length > 0 ||
    propuesta_valor?.calidad_percibida_0_5 != null;
  const hasOperacion =
    Boolean(operacion?.modelo_servicio) ||
    operacion?.ticket_tier_p1_p5 != null ||
    Boolean(operacion?.escala_operativa) ||
    (operacion?.horarios_apertura ?? []).length > 0;
  const hasUbicacion =
    Boolean(ubicacion_ideal?.tipo_calle) ||
    Boolean(ubicacion_ideal?.densidad_preferida) ||
    Boolean(ubicacion_ideal?.flujo_tipo);
  const hasNuances = (nuances_detected ?? []).length > 0;
  return !(hasPublico || hasPropuesta || hasOperacion || hasUbicacion || hasNuances);
}

function Chips({ items, variant }: { items: string[]; variant?: "accent" | "neutral" }) {
  if (!items || items.length === 0) return <span className={styles.muted}>—</span>;
  return (
    <div className={styles.chipRow}>
      {items.map((t) => (
        <span
          key={t}
          className={`${styles.chip} ${variant === "accent" ? styles.chipAccent : ""}`}
        >
          {t}
        </span>
      ))}
    </div>
  );
}

function Row({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className={styles.row}>
      <div className={styles.rowLabel}>{label}</div>
      <div className={styles.rowValue}>
        {value == null || value === "" ? <span className={styles.muted}>—</span> : value}
      </div>
    </div>
  );
}

/** Empty state — se muestra cuando no hay perfil_refinado aun. */
function EmptyState() {
  return (
    <div className={styles.emptyWrap}>
      <div className={styles.emptyCard}>
        <div className={styles.emptyEyebrow}>
          <span className={styles.emptyTick}>●</span>
          PERFIL REFINADO
        </div>
        <h3 className={styles.emptyTitle}>
          Aun no hemos refinado tu descripcion
        </h3>
        <p className={styles.emptyBody}>
          El motor todavia no ha generado un perfil rico para tu idea. Afina la
          descripcion desde la barra de busqueda para capturar matices como
          publico objetivo, ticket tier, horarios y diferenciacion.
        </p>
      </div>
    </div>
  );
}

/**
 * Compara pesos default del sector (pesos_dimensiones) con los pesos
 * modulados por el perfil refinado (pesos_modulados). Renderiza una
 * barra por dimension + flecha de diff.
 */
function PesosComparativa({
  pesosBase,
  pesosMod,
}: {
  pesosBase: Record<string, number>;
  pesosMod: Record<string, number>;
}) {
  const dims = DIMENSIONS.filter(
    (d) => pesosBase[d.key] != null || pesosMod[d.key] != null,
  );
  if (dims.length === 0) {
    return (
      <div className={styles.pesosEmpty}>
        Aun no hay comparacion de pesos disponible para esta idea.
      </div>
    );
  }
  return (
    <div className={styles.pesosList}>
      {dims.map((d) => {
        const base = pesosBase[d.key] ?? 0;
        const mod = pesosMod[d.key] ?? base;
        const basePct = Math.round(base * 100);
        const modPct = Math.round(mod * 100);
        const diff = modPct - basePct;
        const arrow = diff > 0 ? "↑" : diff < 0 ? "↓" : "·";
        const arrowClass =
          diff > 0 ? styles.diffUp : diff < 0 ? styles.diffDown : styles.diffFlat;
        return (
          <div key={d.key} className={styles.pesoRow}>
            <div className={styles.pesoLabel}>{d.name}</div>
            <div className={styles.pesoBars}>
              <div className={styles.pesoBarGroup} title={`Peso sector base: ${basePct}%`}>
                <div className={styles.pesoBarTrack}>
                  <div
                    className={`${styles.pesoBarFill} ${styles.pesoBarBase}`}
                    style={{ width: `${Math.min(100, basePct)}%` }}
                  />
                </div>
                <span className={styles.pesoBarValue}>{basePct}%</span>
              </div>
              <div className={styles.pesoBarGroup} title={`Peso modulado para tu idea: ${modPct}%`}>
                <div className={styles.pesoBarTrack}>
                  <div
                    className={`${styles.pesoBarFill} ${styles.pesoBarMod}`}
                    style={{ width: `${Math.min(100, modPct)}%` }}
                  />
                </div>
                <span className={styles.pesoBarValue}>{modPct}%</span>
              </div>
            </div>
            <div className={`${styles.pesoDiff} ${arrowClass}`}>
              <span className={styles.pesoArrow}>{arrow}</span>
              <span className={styles.pesoDiffNum}>
                {diff > 0 ? `+${diff}` : diff}
              </span>
            </div>
          </div>
        );
      })}
      <div className={styles.pesosLegend}>
        <span className={`${styles.pesosLegendDot} ${styles.pesoBarBase}`} />
        <span>Peso base del sector</span>
        <span className={`${styles.pesosLegendDot} ${styles.pesoBarMod}`} />
        <span>Peso modulado por tu perfil</span>
      </div>
    </div>
  );
}

export default function DossierTabPerfil({ zona }: Props) {
  const perfil = zona?.perfil_refinado ?? null;
  const pesosBase = zona?.pesos_dimensiones ?? {};
  const pesosMod = zona?.pesos_modulados ?? {};
  const hasPesosMod =
    Object.keys(pesosMod).length > 0 && Object.keys(pesosBase).length > 0;

  if (isEmptyPerfil(perfil)) {
    return <EmptyState />;
  }

  // Safe narrowing: perfil es no-null aqui.
  const p = perfil as PerfilRefinado;
  const po = p.publico_objetivo ?? { estilo_vida: [], horarios_pico: [] };
  const pv = p.propuesta_valor ?? { diferenciadores: [] };
  const op = p.operacion ?? { horarios_apertura: [] };
  const ui = p.ubicacion_ideal ?? {};
  const nuances = p.nuances_detected ?? [];

  const ticket = ticketStr(op.ticket_tier_p1_p5 ?? null);

  return (
    <div className={styles.tab}>
      {/* Banner con signal preservation score */}
      {typeof p.signal_preservation_score === "number" && (
        <div className={styles.preservBanner}>
          <div className={styles.preservLabel}>PRESERVACION DE SEnAL</div>
          <div className={styles.preservBody}>
            <div className={styles.preservValue}>
              {Math.round(p.signal_preservation_score)}
              <span className={styles.preservPct}>/100</span>
            </div>
            <p className={styles.preservText}>
              {p.signal_preservation_score >= 80
                ? "El motor ha capturado la mayoria de matices de tu idea."
                : p.signal_preservation_score >= 60
                ? "Quedan matices por precisar — puedes refinar abajo."
                : "Varios matices se han perdido — refina tu descripcion."}
            </p>
          </div>
        </div>
      )}

      {/* 1 — Publico objetivo */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>Publico objetivo</h3>
        <div className={styles.rows}>
          <Row label="Edad tipica" value={po.edad_rango ?? null} />
          <Row
            label="Nivel socioeconomico"
            value={
              po.nivel_socioeconomico
                ? NIVEL_SOCIO_LABEL[po.nivel_socioeconomico] ?? po.nivel_socioeconomico
                : null
            }
          />
          <Row
            label="Estilo de vida"
            value={<Chips items={po.estilo_vida ?? []} variant="accent" />}
          />
          <Row
            label="Horarios pico"
            value={
              po.horarios_pico && po.horarios_pico.length > 0
                ? po.horarios_pico.join(" · ")
                : null
            }
          />
        </div>
      </section>

      {/* 2 — Propuesta de valor */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>Propuesta de valor</h3>
        <div className={styles.rows}>
          <Row
            label="Especializacion"
            value={pv.especializacion ? <p className={styles.prose}>{pv.especializacion}</p> : null}
          />
          <Row label="Diferenciadores" value={<Chips items={pv.diferenciadores ?? []} />} />
          <Row
            label="Calidad percibida"
            value={
              pv.calidad_percibida_0_5 != null
                ? `${pv.calidad_percibida_0_5.toFixed(1)} / 5`
                : null
            }
          />
        </div>
      </section>

      {/* 3 — Operacion */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>Operacion</h3>
        <div className={styles.rows}>
          <Row
            label="Modelo de servicio"
            value={
              op.modelo_servicio
                ? MODELO_SERVICIO_LABEL[op.modelo_servicio] ?? capitalize(op.modelo_servicio)
                : null
            }
          />
          <Row
            label="Ticket tier"
            value={
              ticket ? (
                <span className={styles.ticket}>
                  <span className={styles.ticketGlyph}>{ticket}</span>
                  <span className={styles.ticketTier}>
                    tier {op.ticket_tier_p1_p5} / 5
                  </span>
                </span>
              ) : null
            }
          />
          <Row
            label="Escala operativa"
            value={
              op.escala_operativa
                ? ESCALA_LABEL[op.escala_operativa] ?? capitalize(op.escala_operativa)
                : null
            }
          />
          <Row
            label="Horarios de apertura"
            value={
              op.horarios_apertura && op.horarios_apertura.length > 0 ? (
                <Chips items={op.horarios_apertura} />
              ) : null
            }
          />
        </div>
      </section>

      {/* 4 — Ubicacion ideal */}
      <section className={styles.section}>
        <h3 className={styles.sectionTitle}>Ubicacion ideal</h3>
        <div className={styles.triad}>
          <div className={styles.triadCell}>
            <div className={styles.triadLabel}>Tipo de calle</div>
            <div className={styles.triadValue}>{ui.tipo_calle ?? "—"}</div>
          </div>
          <div className={styles.triadCell}>
            <div className={styles.triadLabel}>Flujo</div>
            <div className={styles.triadValue}>{ui.flujo_tipo ?? "—"}</div>
          </div>
          <div className={styles.triadCell}>
            <div className={styles.triadLabel}>Densidad</div>
            <div className={styles.triadValue}>
              {ui.densidad_preferida
                ? DENSIDAD_LABEL[ui.densidad_preferida] ?? ui.densidad_preferida
                : "—"}
            </div>
          </div>
        </div>
      </section>

      {/* 5 — Nuances detectadas */}
      {nuances.length > 0 && (
        <section className={styles.section}>
          <h3 className={styles.sectionTitle}>Matices detectados</h3>
          <p className={styles.sectionHelp}>
            Senales sueltas que el motor ha identificado en tu descripcion — vocabulario abierto, no taxonomia cerrada.
          </p>
          <div className={styles.chipRow}>
            {nuances.map((n) => (
              <span key={n} className={`${styles.chip} ${styles.chipAccent}`}>
                {n}
              </span>
            ))}
          </div>
        </section>
      )}

      {/* 6 — Como afecta a tu score */}
      {hasPesosMod && (
        <section className={styles.section}>
          <h3 className={styles.sectionTitle}>Como afecta a tu score</h3>
          <h4 className={styles.sectionSubtitle}>
            Pesos del modelo — base del sector vs. modulados por tu perfil
          </h4>
          <PesosComparativa pesosBase={pesosBase} pesosMod={pesosMod} />
        </section>
      )}

    </div>
  );
}
