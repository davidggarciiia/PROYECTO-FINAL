"use client";

import React, { useState, useCallback, useRef } from "react";
import {
  AreaChart, Area,
  BarChart, Bar, Cell,
  ComposedChart,
  LineChart, Line,
  XAxis, YAxis, CartesianGrid,
  Tooltip as ReTooltip,
  ReferenceLine, Legend,
  ResponsiveContainer,
} from "recharts";
import type {
  FinancieroResponse, DecisionBlock, EstructuraCostes,
  BreakEvenInfo, MetricasClave, Riesgo, Insight,
  CorreccionAplicada, CapacityModelInfo, BusinessContext,
} from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./FinancialPanel.module.css";

interface Props {
  financiero: FinancieroResponse | null;
  loading: boolean;
  zonaId: string;
  sessionId: string;
  onUpdate: (data: FinancieroResponse) => void;
}

// ─── Colores constantes ───────────────────────────────────────────────────────
const C = {
  conservador: "#6366F1",
  base:        "#F59E0B",
  optimista:   "#10B981",
  alquiler:    "#EF4444",
  personal:    "#F59E0B",
  variable:    "#8B5CF6",
  otros:       "#64748B",
  beneficio:   "#10B981",
  perdida:     "#EF4444",
  green:       "#10B981",
  yellow:      "#F59E0B",
  red:         "#EF4444",
  indigo:      "#6366F1",
  text:        "#E2E8F0",
  muted:       "#94A3B8",
  subtle:      "#475569",
  border:      "rgba(255,255,255,0.07)",
  surface:     "rgba(255,255,255,0.03)",
  surface2:    "rgba(255,255,255,0.05)",
};

const fmt  = (n: number) => Math.round(n).toLocaleString("es-ES");
const fmtK = (n: number) => `${Math.round(n / 1000)}k`;

// ─── Debounce ─────────────────────────────────────────────────────────────────
function useDebounce(fn: (...args: unknown[]) => void, delay: number) {
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  return useCallback((...args: unknown[]) => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => fn(...args), delay);
  }, [fn, delay]);
}

// ─── Tooltip personalizado ────────────────────────────────────────────────────
interface TooltipEntry { name: string; value: number; color: string; dataKey: string }
interface TooltipBaseProps { active?: boolean; payload?: TooltipEntry[]; label?: string | number }

function TooltipBase({ active, payload, label }: TooltipBaseProps) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#0D1220", border: `1px solid ${C.border}`,
      borderRadius: 8, padding: "8px 12px", fontSize: 12,
    }}>
      {label != null && <div style={{ color: C.muted, marginBottom: 4 }}>{label}</div>}
      {payload.map((p) => (
        <div key={p.dataKey} style={{ color: p.color, fontWeight: 600 }}>
          {p.name}: {fmt(p.value)} €
        </div>
      ))}
    </div>
  );
}

// ─── Componentes pequeños ──────────────────────────────────────────────────────

function SectionTitle({ title, badge }: { title: string; badge?: React.ReactNode }) {
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
      <h3 style={{
        fontSize: 10, fontWeight: 700, textTransform: "uppercase",
        letterSpacing: "0.08em", color: C.muted, margin: 0,
      }}>{title}</h3>
      {badge}
    </div>
  );
}

function ChartExplanation({ children }: { children: React.ReactNode }) {
  return (
    <p style={{
      fontSize: 11, color: C.subtle, lineHeight: 1.55,
      margin: "8px 0 0", borderTop: `1px solid ${C.border}`,
      paddingTop: 8,
    }}>{children}</p>
  );
}

function StatCard({
  label, value, sub, color, icon,
}: { label: string; value: string; sub?: string; color?: string; icon?: string }) {
  return (
    <div style={{
      background: C.surface, border: `1px solid ${C.border}`,
      borderRadius: 10, padding: "12px 14px",
      display: "flex", flexDirection: "column", gap: 4,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        {icon && <span style={{ fontSize: 14 }}>{icon}</span>}
        <span style={{
          fontSize: 18, fontWeight: 800, color: color ?? C.text,
          fontVariantNumeric: "tabular-nums", lineHeight: 1,
        }}>{value}</span>
      </div>
      <span style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: "0.05em" }}>{label}</span>
      {sub && <span style={{ fontSize: 10, color: C.subtle }}>{sub}</span>}
    </div>
  );
}

// ─── BLOQUE 1: Decisión rápida ────────────────────────────────────────────────

const DECISION_CONFIG = {
  si:     { label: "ABRIR", bg: "rgba(16,185,129,0.12)", border: "rgba(16,185,129,0.3)", color: C.green,  emoji: "✓" },
  riesgo: { label: "CON RIESGO", bg: "rgba(245,158,11,0.12)", border: "rgba(245,158,11,0.3)", color: C.yellow, emoji: "!" },
  no:     { label: "NO ABRIR", bg: "rgba(239,68,68,0.12)",  border: "rgba(239,68,68,0.3)",  color: C.red,   emoji: "✕" },
};

function BloqueDecision({ d }: { d: DecisionBlock }) {
  const cfg = DECISION_CONFIG[d.recomendacion];
  return (
    <section className={styles.section}>
      <SectionTitle title="Decisión rápida" />
      <div style={{
        background: cfg.bg, border: `1px solid ${cfg.border}`,
        borderRadius: 12, padding: "14px 16px", marginBottom: 14,
        display: "flex", alignItems: "center", gap: 14,
      }}>
        <div style={{
          width: 44, height: 44, borderRadius: 10, flexShrink: 0,
          background: `${cfg.color}22`, border: `2px solid ${cfg.color}55`,
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 22, fontWeight: 900, color: cfg.color,
        }}>{cfg.emoji}</div>
        <div>
          <div style={{ fontSize: 16, fontWeight: 800, color: cfg.color, letterSpacing: "0.04em" }}>
            {cfg.label}
          </div>
          <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
            Basado en ROI, payback y ratio alquiler/ventas
          </div>
        </div>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <StatCard
          label="Beneficio mensual"
          value={`${d.beneficio_mensual >= 0 ? "+" : ""}${fmt(d.beneficio_mensual)} €`}
          sub="en régimen estable (escenario base)"
          color={d.beneficio_mensual >= 0 ? C.green : C.red}
        />
        <StatCard
          label="Payback"
          value={d.payback >= 999 ? "+36m" : `${d.payback} meses`}
          sub="recuperación de inversión"
          color={d.payback <= 18 ? C.green : d.payback <= 30 ? C.yellow : C.red}
        />
        <StatCard
          label="Capital necesario"
          value={`${fmt(d.capital_necesario)} €`}
          sub="inversión inicial total"
        />
        <StatCard
          label="Gap de capital"
          value={d.gap_capital > 0 ? `${fmt(d.gap_capital)} €` : "Cubierto"}
          sub={d.gap_capital > 0 ? "financiación necesaria" : "capital suficiente"}
          color={d.gap_capital > 0 ? C.yellow : C.green}
        />
      </div>
    </section>
  );
}

// ─── BLOQUE: Flags de validación del modelo ──────────────────────────────────

function BloqueValidationFlags({ flags }: { flags: string[] }) {
  if (!flags.length) return null;
  return (
    <section className={styles.section}>
      <SectionTitle title="Avisos del modelo" />
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        {flags.map((flag, i) => (
          <div key={i} style={{
            display: "flex", alignItems: "flex-start", gap: 8, padding: "8px 10px",
            borderRadius: 8, fontSize: 11, lineHeight: 1.55,
            background: "rgba(245,158,11,0.06)", border: "1px solid rgba(245,158,11,0.2)",
          }}>
            <span style={{ color: C.yellow, flexShrink: 0, marginTop: 1 }}>!</span>
            <span style={{ color: C.muted }}>{flag}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

// ─── BLOQUE: Correcciones automáticas ────────────────────────────────────────

const BUSINESS_MODEL_LABELS: Record<string, { label: string; color: string }> = {
  retail_walkin:      { label: "Retail / Paso",    color: C.indigo },
  restaurant:         { label: "Restauración",     color: C.yellow },
  appointment_based:  { label: "Negocio de cita",  color: C.green  },
  hybrid:             { label: "Híbrido",           color: C.muted  },
};

function BloqueCorrecciones({ correcciones }: { correcciones: CorreccionAplicada[] }) {
  if (!correcciones.length) return null;
  return (
    <section className={styles.section}>
      <SectionTitle title="Ajustes automáticos aplicados" />
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {correcciones.map((c, i) => (
          <div key={i} style={{
            background: "rgba(245,158,11,0.07)", border: "1px solid rgba(245,158,11,0.25)",
            borderRadius: 10, padding: "10px 12px",
          }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.yellow, marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.04em" }}>
              Corrección: {c.parametro.replace(/_/g, " ")}
            </div>
            <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.55 }}>{c.motivo}</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
              <span style={{ fontSize: 10, color: C.subtle }}>
                Estimado: <strong style={{ color: C.text }}>{Math.round(c.valor_original)}</strong>
              </span>
              <span style={{ fontSize: 10, color: C.subtle }}>→</span>
              <span style={{ fontSize: 10, color: C.subtle }}>
                Corregido: <strong style={{ color: C.yellow }}>{Math.round(c.valor_corregido)}</strong>
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

// ─── BLOQUE: Modelo de capacidad (negocios de cita) ──────────────────────────

function BloqueCapacidad({ cm }: { cm: CapacityModelInfo }) {
  return (
    <div style={{
      marginTop: 12, padding: "10px 12px", borderRadius: 8,
      background: "rgba(16,185,129,0.06)", border: "1px solid rgba(16,185,129,0.2)",
    }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: C.green, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
        Modelo de capacidad
      </div>
      <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.6 }}>
        {cm.descripcion}
        {" — "}
        <strong style={{ color: C.text }}>máx. {Math.round(cm.max_clients_day)} clientes/día</strong>
      </div>
    </div>
  );
}

// ─── BLOQUE 2: Economía base ──────────────────────────────────────────────────

function BloqueEconomia({ f }: { f: FinancieroResponse }) {
  const eb = f.economia_base;
  const md = f.modelo_demanda;
  if (!eb) return null;

  const conversionPct    = Math.round(eb.conversion_pct * 100);
  const modelType        = f.business_model_type ?? "retail_walkin";
  const modelLabel       = BUSINESS_MODEL_LABELS[modelType] ?? BUSINESS_MODEL_LABELS.retail_walkin;
  const isAppointment    = modelType === "appointment_based";
  const ocupacionPct     = Math.round((eb.ocupacion_efectiva ?? f.ocupacion_efectiva ?? 0) * 100);
  const ocupacionColor   = ocupacionPct > 85 ? C.red : ocupacionPct > 65 ? C.yellow : C.green;

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Economía base"
        badge={
          <span style={{
            fontSize: 10, fontWeight: 700, color: modelLabel.color,
            background: `${modelLabel.color}18`, border: `1px solid ${modelLabel.color}40`,
            borderRadius: 100, padding: "2px 8px",
          }}>{modelLabel.label}</span>
        }
      />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <StatCard label="Ingresos / mes" value={`${fmt(eb.ingresos_mensuales)} €`} sub="escenario base, régimen estable (80% ocu.)" />
        <StatCard label="Clientes / día"  value={`${Math.round(eb.clientes_dia)}`} sub="estimación media" />
        <StatCard label="Ticket medio"    value={`${Math.round(eb.ticket_medio)} €`} sub="mediana competidores" />
        {!isAppointment && (
          <StatCard
            label="Tasa captación"
            value={`${conversionPct}%`}
            sub="clientes / potencial zona (máx. 15%)"
            color={conversionPct >= 15 ? C.yellow : C.green}
          />
        )}
        {ocupacionPct > 0 && (
          <StatCard
            label="Ocupación efectiva"
            value={`${ocupacionPct}%`}
            sub="régimen estable (techo: 80%)"
            color={ocupacionColor}
          />
        )}
      </div>

      {/* Modelo de capacidad (cita) */}
      {f.capacity_model && <BloqueCapacidad cm={f.capacity_model} />}

      {/* Modelo de demanda (paso) */}
      {md && !isAppointment && (
        <div style={{
          marginTop: 12, padding: "10px 12px", borderRadius: 8,
          background: "rgba(99,102,241,0.06)", border: "1px solid rgba(99,102,241,0.15)",
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: C.indigo, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
            Modelo de demanda
          </div>
          <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.6 }}>
            <strong style={{ color: C.text }}>{fmt(md.flujo_peatonal_dia)}</strong> peatones/día →{" "}
            <strong style={{ color: C.text }}>{fmt(md.max_potential_customers)}</strong> potenciales (15%) →{" "}
            <strong style={{ color: C.text }}>{Math.round(eb.clientes_dia)} clientes ({conversionPct}% captación)</strong>
          </div>
        </div>
      )}
    </section>
  );
}

// ─── BLOQUE 3: Estructura de costes ─── GRÁFICO 1 ────────────────────────────

const LABELS_COSTES: Record<string, string> = {
  alquiler: "Alquiler",
  personal: "Personal",
  variable: "Variable (COGS)",
  otros:    "Otros fijos",
  beneficio:"Beneficio",
  perdida:  "Pérdida",
};

function BloqueCostes({ ec }: { ec: EstructuraCostes }) {
  const hayBeneficio = ec.beneficio > 0;
  const hayPerdida   = ec.perdida > 0;

  const barData = [
    { name: "Mes estable", alquiler: ec.alquiler, personal: ec.personal, variable: ec.variable, otros: ec.otros,
      ...(hayBeneficio ? { beneficio: ec.beneficio } : { perdida: ec.perdida }) },
  ];

  const total = ec.alquiler + ec.personal + ec.variable + ec.otros;
  const pctBeneficio = ec.ingresos_totales > 0
    ? Math.round((ec.beneficio / ec.ingresos_totales) * 100)
    : 0;

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Estructura de costes"
        badge={
          <span style={{
            fontSize: 10, fontWeight: 700,
            color: hayBeneficio ? C.green : C.red,
            background: hayBeneficio ? "rgba(16,185,129,0.1)" : "rgba(239,68,68,0.1)",
            border: `1px solid ${hayBeneficio ? "rgba(16,185,129,0.25)" : "rgba(239,68,68,0.25)"}`,
            borderRadius: 100, padding: "2px 8px",
          }}>
            {hayBeneficio ? `+${pctBeneficio}% margen` : "Sin beneficio"}
          </span>
        }
      />

      {/* Leyenda rápida */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: "6px 12px", marginBottom: 12 }}>
        {(["alquiler","personal","variable","otros"] as const).map(k => (
          <div key={k} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11 }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: C[k], display: "inline-block" }} />
            <span style={{ color: C.muted }}>{LABELS_COSTES[k]}</span>
            <span style={{ color: C.text, fontWeight: 600 }}>{fmt(ec[k])} €</span>
          </div>
        ))}
        {hayBeneficio && (
          <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11 }}>
            <span style={{ width: 8, height: 8, borderRadius: 2, background: C.beneficio, display: "inline-block" }} />
            <span style={{ color: C.muted }}>Beneficio</span>
            <span style={{ color: C.green, fontWeight: 600 }}>+{fmt(ec.beneficio)} €</span>
          </div>
        )}
      </div>

      {/* Gráfico stacked bar horizontal */}
      <div style={{ height: 80 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={barData} layout="vertical" margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
            <XAxis type="number" hide tick={false} axisLine={false} />
            <YAxis type="category" dataKey="name" hide />
            <ReTooltip
              content={<TooltipBase />}
              cursor={{ fill: "rgba(255,255,255,0.03)" }}
            />
            <Bar dataKey="alquiler" name="Alquiler"  stackId="a" fill={C.alquiler} radius={[0,0,0,0]} />
            <Bar dataKey="personal" name="Personal"  stackId="a" fill={C.personal} />
            <Bar dataKey="variable" name="Variable"  stackId="a" fill={C.variable} />
            <Bar dataKey="otros"    name="Otros"     stackId="a" fill={C.otros} />
            {hayBeneficio && <Bar dataKey="beneficio" name="Beneficio" stackId="a" fill={C.beneficio} radius={[0,4,4,0]} />}
            {hayPerdida   && <Bar dataKey="perdida"   name="Pérdida"   stackId="a" fill={C.perdida}   radius={[0,4,4,0]} />}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <ChartExplanation>
        Total costes fijos: <strong>{fmt(total)} €/mes</strong>.{" "}
        Ingresos base: <strong>{fmt(ec.ingresos_totales)} €/mes</strong>.{" "}
        {hayBeneficio
          ? `El negocio genera ${fmt(ec.beneficio)} € de beneficio en régimen estable.`
          : `Déficit de ${fmt(ec.perdida)} € — aumenta clientes o reduce costes.`}
      </ChartExplanation>
    </section>
  );
}

// ─── BLOQUE 4: Break-even ─── GRÁFICO 2 ──────────────────────────────────────

function BloqueBreakEven({ be, alquilerPct }: { be: BreakEvenInfo; alquilerPct: number }) {
  const margen = be.margen_sobre_be_pct;
  const suficiente = be.clientes_base >= be.clientes_be;

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Punto de equilibrio"
        badge={
          <span style={{
            fontSize: 10, fontWeight: 700,
            color: suficiente ? C.green : C.red,
            background: suficiente ? "rgba(16,185,129,0.1)" : "rgba(239,68,68,0.1)",
            border: `1px solid ${suficiente ? "rgba(16,185,129,0.25)" : "rgba(239,68,68,0.25)"}`,
            borderRadius: 100, padding: "2px 8px",
          }}>
            {suficiente ? `+${Math.round(margen)}% sobre mínimo` : `Déficit ${Math.abs(Math.round(margen))}%`}
          </span>
        }
      />

      <div style={{ display: "flex", gap: 16, marginBottom: 14, alignItems: "stretch" }}>
        <div style={{
          flex: 1, background: C.surface, border: `1px solid ${C.border}`,
          borderRadius: 10, padding: "12px 14px", textAlign: "center",
        }}>
          <div style={{ fontSize: 28, fontWeight: 800, color: C.yellow, lineHeight: 1 }}>
            {be.clientes_be}
          </div>
          <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: "0.05em", marginTop: 4 }}>
            clientes/día mínimos
          </div>
        </div>
        <div style={{
          flex: 1, background: C.surface, border: `1px solid ${C.border}`,
          borderRadius: 10, padding: "12px 14px", textAlign: "center",
        }}>
          <div style={{ fontSize: 28, fontWeight: 800, color: suficiente ? C.green : C.red, lineHeight: 1 }}>
            {Math.round(be.clientes_base)}
          </div>
          <div style={{ fontSize: 10, color: C.muted, textTransform: "uppercase", letterSpacing: "0.05em", marginTop: 4 }}>
            clientes/día estimados
          </div>
        </div>
      </div>

      {/* Curva break-even */}
      <div style={{ height: 150 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={be.chart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis
              dataKey="clientes" type="number"
              tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false}
              label={{ value: "clientes/día", position: "insideBottomRight", offset: -4, fill: C.subtle, fontSize: 10 }}
            />
            <YAxis
              tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false}
              tickFormatter={fmtK} width={34}
            />
            <ReTooltip content={<TooltipBase />} />
            <ReferenceLine
              x={be.clientes_be} stroke={C.yellow}
              strokeDasharray="5 3" label={{ value: `BE: ${be.clientes_be}`, fill: C.yellow, fontSize: 10, position: "top" }}
            />
            <ReferenceLine
              x={be.clientes_base} stroke={C.base}
              strokeDasharray="5 3" label={{ value: `Base: ${Math.round(be.clientes_base)}`, fill: C.base, fontSize: 10, position: "top" }}
            />
            <Line type="monotone" dataKey="ingresos"      name="Ingresos"       stroke={C.green}     strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="costes_totales" name="Costes totales" stroke={C.red}       strokeWidth={2} dot={false} strokeDasharray="4 2" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <ChartExplanation>
        Con <strong>{be.clientes_be} clientes/día</strong> cubres todos los costes.
        Tu estimación base de <strong>{Math.round(be.clientes_base)} clientes/día</strong>{" "}
        {suficiente
          ? `supera el mínimo en un ${Math.round(margen)}% — hay margen de seguridad.`
          : `está por debajo del mínimo — necesitas más clientes o reducir costes fijos.`}
        {alquilerPct > 0.15 && (
          <> El alquiler ({Math.round(alquilerPct * 100)}% s/ventas) es el principal factor que eleva el punto de equilibrio.</>
        )}
      </ChartExplanation>
    </section>
  );
}

// ─── BLOQUE 5: Escenarios ─── GRÁFICOS 3, 4, 5 ───────────────────────────────

function BloqueEscenarios({ f }: { f: FinancieroResponse }) {
  const [activeChart, setActiveChart] = useState<"caja" | "mensual" | "comparativa">("caja");

  const proyeccion = f.proyeccion;

  // Datos caja acumulada — cada 3 meses
  const cajaData = proyeccion
    .filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({
      mes: `M${m.mes}`,
      Conservador: m.acumulado_conservador,
      Base:        m.acumulado_base,
      Optimista:   m.acumulado_optimista,
    }));

  // Datos evolución mensual EBITDA (base) — todos los meses
  // Con bandas de volatilidad ±15% derivadas de ingresos_base_low/high
  const costeMercPct = ((f.parametros as Record<string, unknown>).coste_mercancia_pct as { valor_usado?: number })?.valor_usado ?? 0.40;
  const margenUnit   = 1 - costeMercPct;
  const mensualData = proyeccion.map(m => {
    const cfMes = m.costes_fijos ?? 0;
    const low   = m.ingresos_base_low  != null ? Math.round(m.ingresos_base_low  * margenUnit) - cfMes : null;
    const high  = m.ingresos_base_high != null ? Math.round(m.ingresos_base_high * margenUnit) - cfMes : null;
    return { mes: m.mes, ebitda: m.ebitda_base, ebitda_low: low, ebitda_high: high };
  });

  // Datos comparativa escenarios (ingresos año 1 / año 2 / año 3)
  const comparativaData = [
    {
      periodo: "Año 1",
      Conservador: Math.round(proyeccion.slice(0, 12).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(proyeccion.slice(0, 12).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(proyeccion.slice(0, 12).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000),
    },
    {
      periodo: "Año 2",
      Conservador: Math.round(proyeccion.slice(12, 24).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(proyeccion.slice(12, 24).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(proyeccion.slice(12, 24).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000),
    },
    {
      periodo: "Año 3",
      Conservador: Math.round(proyeccion.slice(24, 36).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(proyeccion.slice(24, 36).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(proyeccion.slice(24, 36).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000),
    },
  ];

  const tabs = [
    { id: "caja" as const, label: "Caja acumulada" },
    { id: "mensual" as const, label: "EBITDA mensual" },
    { id: "comparativa" as const, label: "Comparativa" },
  ];

  return (
    <section className={styles.section}>
      <SectionTitle title="Escenarios — 36 meses" />

      {/* Tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 14 }}>
        {tabs.map(t => (
          <button
            key={t.id}
            onClick={() => setActiveChart(t.id)}
            style={{
              all: "unset", cursor: "pointer",
              fontSize: 11, fontWeight: 600, padding: "5px 10px",
              borderRadius: 6, transition: "all 0.15s",
              background: activeChart === t.id ? "rgba(255,255,255,0.1)" : "transparent",
              color: activeChart === t.id ? C.text : C.muted,
              border: `1px solid ${activeChart === t.id ? "rgba(255,255,255,0.15)" : "transparent"}`,
            }}
          >{t.label}</button>
        ))}
      </div>

      {/* GRÁFICO 3: Caja acumulada */}
      {activeChart === "caja" && (
        <>
          <div style={{ height: 180 }}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={cajaData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <defs>
                  {[["gOpt", C.optimista], ["gBase", C.base], ["gCons", C.conservador]].map(([id, color]) => (
                    <linearGradient key={id} id={id} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={color} stopOpacity={0.2} />
                      <stop offset="95%" stopColor={color} stopOpacity={0} />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
                <ReferenceLine y={0} stroke="rgba(255,255,255,0.12)" strokeDasharray="4 4" />
                <ReTooltip
                  contentStyle={{ background: "#0D1220", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }}
                  formatter={(v: number, name: string) => [`${fmt(v)} €`, name]}
                />
                <Area type="monotone" dataKey="Optimista"   stroke={C.optimista}   fill="url(#gOpt)"  strokeWidth={2} dot={false} />
                <Area type="monotone" dataKey="Base"        stroke={C.base}        fill="url(#gBase)" strokeWidth={2} dot={false} />
                <Area type="monotone" dataKey="Conservador" stroke={C.conservador} fill="url(#gCons)" strokeWidth={2} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <LegendRow items={[
            { color: C.optimista,   label: `Optimista (${f.payback_meses_optimista >= 999 ? ">36m" : f.payback_meses_optimista + "m"})` },
            { color: C.base,        label: `Base (${f.payback_meses_base >= 999 ? ">36m" : f.payback_meses_base + "m"})` },
            { color: C.conservador, label: `Conservador (${f.payback_meses_conservador >= 999 ? ">36m" : f.payback_meses_conservador + "m"})` },
          ]} />
          <ChartExplanation>
            Caja acumulada desde el día 1 (negativa = inversión no recuperada).
            La línea cruza cero en el mes de payback. La curva base cruza en el mes {f.payback_meses_base >= 999 ? "nunca (ajusta parámetros)" : f.payback_meses_base}.
          </ChartExplanation>
        </>
      )}

      {/* GRÁFICO 4: EBITDA mensual */}
      {activeChart === "mensual" && (
        <>
          <div style={{ height: 180 }}>
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={mensualData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis
                  dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }}
                  axisLine={false} tickLine={false}
                  tickFormatter={v => (v % 6 === 0 || v === 1) ? `M${v}` : ""}
                />
                <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
                <ReferenceLine y={0} stroke="rgba(255,255,255,0.18)" />
                <ReTooltip
                  contentStyle={{ background: "#0D1220", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }}
                  formatter={(v: number) => [`${fmt(v)} €`, "EBITDA base"]}
                  labelFormatter={v => `Mes ${v}`}
                />
                <Bar dataKey="ebitda" name="EBITDA base" radius={[2, 2, 0, 0]}>
                  {mensualData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.ebitda >= 0 ? C.green : C.red} fillOpacity={0.8} />
                  ))}
                </Bar>
                <Line type="monotone" dataKey="ebitda_high" name="Variabilidad +" stroke="rgba(245,158,11,0.5)" strokeDasharray="3 2" dot={false} strokeWidth={1} />
                <Line type="monotone" dataKey="ebitda_low"  name="Variabilidad −" stroke="rgba(245,158,11,0.5)" strokeDasharray="3 2" dot={false} strokeWidth={1} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
          <ChartExplanation>
            EBITDA mensual escenario base — verde = beneficio, rojo = pérdida.
            Las líneas discontinuas muestran la banda de variabilidad ±15%.
            La rampa alcanza el máximo en el mes 12 (ocupación techo: 80%).
          </ChartExplanation>
        </>
      )}

      {/* GRÁFICO 5: Comparativa escenarios */}
      {activeChart === "comparativa" && (
        <>
          <div style={{ height: 180 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={comparativaData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis dataKey="periodo" tick={{ fill: C.subtle, fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={v => `${v}k`} width={34} />
                <ReTooltip
                  contentStyle={{ background: "#0D1220", border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 12 }}
                  formatter={(v: number, name: string) => [`${fmt(v)}k €`, name]}
                />
                <Bar dataKey="Conservador" fill={C.conservador} radius={[3, 3, 0, 0]} maxBarSize={28} />
                <Bar dataKey="Base"        fill={C.base}        radius={[3, 3, 0, 0]} maxBarSize={28} />
                <Bar dataKey="Optimista"   fill={C.optimista}   radius={[3, 3, 0, 0]} maxBarSize={28} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <LegendRow items={[
            { color: C.conservador, label: "Conservador" },
            { color: C.base,        label: "Base" },
            { color: C.optimista,   label: "Optimista" },
          ]} />
          <ChartExplanation>
            Ingresos anuales por escenario (en miles €). El año 1 siempre es inferior por la curva de arranque.
            A partir del año 2 el negocio opera a plena capacidad.
          </ChartExplanation>
        </>
      )}
    </section>
  );
}

function LegendRow({ items }: { items: { color: string; label: string }[] }) {
  return (
    <div style={{ display: "flex", gap: 14, justifyContent: "center", marginTop: 8, flexWrap: "wrap" }}>
      {items.map(({ color, label }) => (
        <span key={label} style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: C.muted }}>
          <span style={{ width: 20, height: 3, borderRadius: 2, background: color, display: "inline-block" }} />
          {label}
        </span>
      ))}
    </div>
  );
}

// ─── BLOQUE 6: Métricas clave ─────────────────────────────────────────────────

function BloqueMetricas({ m }: { m: MetricasClave }) {
  const colorRoi = (v: number) => v >= 0.3 ? C.green : v >= 0 ? C.yellow : C.red;

  return (
    <section className={styles.section}>
      <SectionTitle title="Métricas clave" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <StatCard
          label="ROI 3 años (base)"
          value={`${Math.round(m.roi_base * 100)}%`}
          sub={`Cons: ${Math.round(m.roi_conservador * 100)}% · Opt: ${Math.round(m.roi_optimista * 100)}%`}
          color={colorRoi(m.roi_base)}
        />
        <StatCard
          label="Margen bruto"
          value={`${Math.round(m.margen_bruto_pct * 100)}%`}
          sub="ingresos - coste mercancía"
          color={m.margen_bruto_pct >= 0.45 ? C.green : m.margen_bruto_pct >= 0.30 ? C.yellow : C.red}
        />
        <StatCard
          label="Payback"
          value={m.payback_meses >= 999 ? ">36m" : `${m.payback_meses}m`}
          sub="recuperación inversión inicial"
          color={m.payback_meses <= 18 ? C.green : m.payback_meses <= 30 ? C.yellow : C.red}
        />
        <StatCard
          label="Mes caja positiva"
          value={m.mes_caja_positiva >= 999 ? ">36m" : `Mes ${m.mes_caja_positiva}`}
          sub="primer mes con caja acumulada ≥ 0"
          color={m.mes_caja_positiva <= 24 ? C.green : C.yellow}
        />
      </div>
    </section>
  );
}

// ─── BLOQUE 7: Riesgos ────────────────────────────────────────────────────────

function BloqueRiesgos({ riesgos }: { riesgos: Riesgo[] }) {
  if (riesgos.length === 0) {
    return (
      <section className={styles.section}>
        <SectionTitle title="Riesgos" />
        <div style={{
          padding: "12px 14px", borderRadius: 10,
          background: "rgba(99,102,241,0.06)", border: "1px solid rgba(99,102,241,0.2)",
          fontSize: 12, color: C.muted,
        }}>
          Análisis de riesgos no disponible para este perfil.
        </div>
      </section>
    );
  }

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Riesgos"
        badge={
          <span style={{
            fontSize: 10, fontWeight: 700, color: C.red,
            background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.25)",
            borderRadius: 100, padding: "2px 8px",
          }}>{riesgos.length} detectado{riesgos.length > 1 ? "s" : ""}</span>
        }
      />
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {riesgos.map((r, i) => (
          <div key={i} style={{
            display: "flex", alignItems: "flex-start", gap: 10, padding: "10px 12px",
            borderRadius: 8, fontSize: 12, lineHeight: 1.55,
            background: r.tipo === "bloqueo" ? "rgba(239,68,68,0.06)" : "rgba(245,158,11,0.06)",
            border: `1px solid ${r.tipo === "bloqueo" ? "rgba(239,68,68,0.2)" : "rgba(245,158,11,0.2)"}`,
          }}>
            <span style={{
              fontSize: 9, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: "0.06em", flexShrink: 0, marginTop: 1,
              color: r.tipo === "bloqueo" ? C.red : C.yellow,
              background: r.tipo === "bloqueo" ? "rgba(239,68,68,0.15)" : "rgba(245,158,11,0.15)",
              border: `1px solid ${r.tipo === "bloqueo" ? "rgba(239,68,68,0.3)" : "rgba(245,158,11,0.3)"}`,
              borderRadius: 4, padding: "2px 5px",
            }}>
              {r.tipo === "bloqueo" ? "Bloqueo" : "Aviso"}
            </span>
            <span style={{ color: C.muted }}>{r.mensaje}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

// ─── BLOQUE 8: Insights ───────────────────────────────────────────────────────

function BloqueInsights({ insights }: { insights: Insight[] }) {
  if (insights.length === 0) return null;

  return (
    <section className={styles.section}>
      <SectionTitle title="Insights" />
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {insights.map((ins, i) => {
          const isRisk = ins.type === "risk";
          const color  = isRisk ? C.red : C.green;
          const bg     = isRisk ? "rgba(239,68,68,0.05)" : "rgba(16,185,129,0.05)";
          const border = isRisk ? "rgba(239,68,68,0.18)" : "rgba(16,185,129,0.18)";

          return (
            <div key={i} style={{
              background: bg, border: `1px solid ${border}`,
              borderRadius: 10, padding: "12px 14px",
              borderLeft: `3px solid ${color}`,
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 6 }}>
                <span style={{
                  fontSize: 9, fontWeight: 700, textTransform: "uppercase",
                  letterSpacing: "0.06em", color,
                  background: isRisk ? "rgba(239,68,68,0.12)" : "rgba(16,185,129,0.12)",
                  border: `1px solid ${border}`, borderRadius: 4, padding: "2px 6px",
                }}>
                  {isRisk ? "Riesgo" : "Oportunidad"}
                </span>
              </div>
              <p style={{ margin: "0 0 6px", fontSize: 12, color: C.text, fontWeight: 500, lineHeight: 1.5 }}>
                {ins.message}
              </p>
              <p style={{ margin: 0, fontSize: 11, color: C.muted, lineHeight: 1.55 }}>
                <strong style={{ color: C.subtle }}>Sugerencia:</strong> {ins.suggestion}
              </p>
            </div>
          );
        })}
      </div>
    </section>
  );
}

// ─── Sliders (grupo) ──────────────────────────────────────────────────────────

const LABELS_DESGLOSE: Record<string, string> = {
  reforma_local: "Reforma del local",
  equipamiento: "Equipamiento",
  deposito_fianza: "Depósito fianza (Art.36 LAU)",
  otros_iniciales: "Licencias y gestoría",
};

function ParamGroup({ color, icon, label, children }: {
  color: string; icon: string; label: string; children: React.ReactNode;
}) {
  const [open, setOpen] = useState(true);
  return (
    <div style={{
      borderRadius: 10, border: `1px solid ${C.border}`,
      background: C.surface, overflow: "hidden",
    }}>
      <button onClick={() => setOpen(o => !o)} style={{
        all: "unset", cursor: "pointer", display: "flex", alignItems: "center",
        gap: 8, width: "100%", padding: "9px 12px", boxSizing: "border-box",
        borderBottom: open ? `1px solid ${C.border}` : "none",
      }}>
        <span style={{
          width: 22, height: 22, borderRadius: 6, display: "flex", alignItems: "center",
          justifyContent: "center", fontSize: 11, fontWeight: 700, flexShrink: 0,
          background: `${color}22`, color, border: `1px solid ${color}44`,
        }}>{icon}</span>
        <span style={{
          fontSize: 11, fontWeight: 600, letterSpacing: "0.06em",
          textTransform: "uppercase", color: C.muted, flex: 1,
        }}>{label}</span>
        <svg width="10" height="10" viewBox="0 0 10 10" style={{
          color: C.subtle, transform: open ? "rotate(180deg)" : "none", transition: "transform 0.2s",
        }}>
          <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinecap="round" />
        </svg>
      </button>
      {open && (
        <div style={{ padding: "10px 12px", display: "flex", flexDirection: "column", gap: 14 }}>
          {children}
        </div>
      )}
    </div>
  );
}

function SliderParam({ label, unit, value, min, max, fuente, color, onChange }: {
  label: string; unit: string; value: number; min: number; max: number;
  fuente: string; color: string; onChange: (v: number) => void;
}) {
  const pct = Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
        <span style={{ fontSize: 12, color: C.muted, fontWeight: 500, lineHeight: 1 }}>{label}</span>
        <span style={{
          fontSize: 12, fontWeight: 700, fontVariantNumeric: "tabular-nums",
          color, background: `${color}18`, border: `1px solid ${color}33`,
          borderRadius: 5, padding: "2px 8px", letterSpacing: "0.02em",
          lineHeight: 1.6, minWidth: 60, textAlign: "right",
        }}>
          {Math.round(value).toLocaleString("es-ES")}{unit && `\u00A0${unit.trim()}`}
        </span>
      </div>
      <div style={{ position: "relative", height: 20, display: "flex", alignItems: "center" }}>
        <div style={{
          position: "absolute", left: 0, right: 0, height: 4,
          borderRadius: 99, background: "rgba(255,255,255,0.06)",
        }} />
        <div style={{
          position: "absolute", left: 0, width: `${pct}%`, height: 4,
          borderRadius: 99, background: `linear-gradient(90deg, ${color}88, ${color})`,
          boxShadow: `0 0 8px ${color}55`, transition: "width 0.05s",
        }} />
        <div style={{
          position: "absolute", left: `calc(${pct}% - 7px)`,
          width: 14, height: 14, borderRadius: "50%", background: color,
          boxShadow: `0 0 0 3px ${color}33, 0 0 10px ${color}88`,
          border: "2px solid rgba(13,18,32,0.8)", transition: "left 0.05s",
          pointerEvents: "none",
        }} />
        <input
          type="range" min={min} max={max}
          step={Math.max(1, Math.round((max - min) / 100))}
          value={value} onChange={e => onChange(Number(e.target.value))}
          style={{
            position: "absolute", inset: 0, width: "100%", height: "100%",
            opacity: 0, cursor: "pointer", margin: 0,
          }}
        />
      </div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ fontSize: 10, color: C.subtle, fontVariantNumeric: "tabular-nums" }}>
          {min.toLocaleString("es-ES")}
        </span>
        <span style={{ fontSize: 9.5, color: C.subtle, fontStyle: "italic", maxWidth: "55%", textAlign: "center",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {fuente}
        </span>
        <span style={{ fontSize: 10, color: C.subtle, fontVariantNumeric: "tabular-nums" }}>
          {max.toLocaleString("es-ES")}
        </span>
      </div>
    </div>
  );
}

// ─── Componente principal ─────────────────────────────────────────────────────

export default function FinancialPanel({ financiero, loading, zonaId, sessionId, onUpdate }: Props) {
  const [overrides, setOverrides] = useState<Record<string, number>>({});
  const [modoConfig, setModoConfig] = useState<"auto" | "manual">("auto");
  const [businessContext, setBusinessContext] = useState<BusinessContext>({ tipo: "nuevo" });
  const slidersRef = useRef<HTMLDivElement>(null);

  const refetch = useCallback(async (
    newOverrides: Record<string, number>,
    bc?: BusinessContext,
  ) => {
    try {
      const data = await api.financiero(zonaId, sessionId, newOverrides, bc ?? businessContext);
      onUpdate(data);
    } catch (e) {
      console.error("Error recalculando:", e);
    }
  }, [zonaId, sessionId, onUpdate, businessContext]);

  const debouncedRefetch = useDebounce(refetch as (...args: unknown[]) => void, 300);

  const handleOverride = (key: string, value: number) => {
    const next = { ...overrides, [key]: value };
    setOverrides(next);
    debouncedRefetch(next);
  };

  const handleBusinessContext = (next: BusinessContext) => {
    setBusinessContext(next);
    refetch(overrides, next);
  };

  const scrollToSliders = () => {
    slidersRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  if (loading) {
    return (
      <div className={styles.loadingState}>
        <div className="spinner" />
        <span>Calculando análisis financiero...</span>
      </div>
    );
  }

  if (!financiero) {
    return (
      <div className={styles.emptyState}>
        <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
          <rect x="4" y="20" width="5" height="8" rx="2" fill="var(--accent)" opacity="0.4" />
          <rect x="13.5" y="12" width="5" height="16" rx="2" fill="var(--accent)" opacity="0.6" />
          <rect x="23" y="6" width="5" height="22" rx="2" fill="var(--accent)" />
        </svg>
        <span>Datos financieros no disponibles</span>
      </div>
    );
  }

  const f = financiero;
  const params = f.parametros ?? {};

  return (
    <div className={styles.container}>

      {/* ── Selector Auto / Manual ── */}
      <section className={styles.section} style={{ paddingBottom: 10 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, flexWrap: "wrap" }}>
          <div style={{ display: "flex", gap: 4 }}>
            {(["auto", "manual"] as const).map(m => (
              <button key={m} onClick={() => setModoConfig(m)} style={{
                all: "unset", cursor: "pointer", fontSize: 11, fontWeight: 700,
                letterSpacing: "0.06em", textTransform: "uppercase",
                padding: "4px 12px", borderRadius: 6,
                background: modoConfig === m ? "rgba(99,102,241,0.18)" : C.surface,
                border: `1px solid ${modoConfig === m ? "rgba(99,102,241,0.5)" : C.border}`,
                color: modoConfig === m ? C.indigo : C.muted,
                transition: "all 0.15s",
              }}>{m}</button>
            ))}
          </div>
          <button onClick={scrollToSliders} style={{
            all: "unset", cursor: "pointer", fontSize: 11, fontWeight: 600,
            color: C.indigo, background: "rgba(99,102,241,0.08)",
            border: "1px solid rgba(99,102,241,0.25)", borderRadius: 6,
            padding: "4px 12px", letterSpacing: "0.04em",
          }}>Editar supuestos ↓</button>
        </div>

        {/* Contexto negocio (modo manual) */}
        {modoConfig === "manual" && (
          <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Contexto del negocio
            </div>
            <div style={{ display: "flex", gap: 6 }}>
              {(["nuevo", "traspaso"] as const).map(t => (
                <button key={t} onClick={() => handleBusinessContext({ ...businessContext, tipo: t })} style={{
                  all: "unset", cursor: "pointer", fontSize: 11, fontWeight: 600,
                  padding: "4px 12px", borderRadius: 6, textTransform: "capitalize",
                  background: businessContext.tipo === t ? "rgba(16,185,129,0.15)" : C.surface,
                  border: `1px solid ${businessContext.tipo === t ? "rgba(16,185,129,0.4)" : C.border}`,
                  color: businessContext.tipo === t ? C.green : C.muted,
                }}>{t}</button>
              ))}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6 }}>
              <div>
                <div style={{ fontSize: 10, color: C.muted, marginBottom: 3 }}>Capital inicial (€)</div>
                <input type="number" min={0} placeholder="ej. 50000"
                  value={businessContext.capital_inicial ?? ""}
                  onChange={e => {
                    const val = e.target.value ? Number(e.target.value) : undefined;
                    handleBusinessContext({ ...businessContext, capital_inicial: val });
                  }}
                  style={{
                    background: C.surface, border: `1px solid ${C.border}`, borderRadius: 6,
                    color: C.text, fontSize: 12, padding: "5px 8px", width: "100%", boxSizing: "border-box",
                  }}
                />
              </div>
              <div>
                <div style={{ fontSize: 10, color: C.muted, marginBottom: 3 }}>Cap. operativa (clientes/día)</div>
                <input type="number" min={1} placeholder="ej. 8"
                  value={businessContext.capacidad_operativa ?? ""}
                  onChange={e => {
                    const val = e.target.value ? Number(e.target.value) : undefined;
                    handleBusinessContext({ ...businessContext, capacidad_operativa: val });
                  }}
                  style={{
                    background: C.surface, border: `1px solid ${C.border}`, borderRadius: 6,
                    color: C.text, fontSize: 12, padding: "5px 8px", width: "100%", boxSizing: "border-box",
                  }}
                />
              </div>
            </div>
          </div>
        )}
      </section>

      {/* ── BLOQUE 1: Decisión rápida ── */}
      {f.decision && <BloqueDecision d={f.decision} />}

      {/* ── Avisos del modelo (validation_flags) ── */}
      <BloqueValidationFlags flags={f.validation_flags ?? []} />

      {/* ── Correcciones automáticas (si las hay) ── */}
      <BloqueCorrecciones correcciones={f.correcciones_aplicadas ?? []} />

      {/* ── Alerta alquiler ── */}
      {f.alerta_alquiler && (
        <div className={styles.rentAlert}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" style={{ flexShrink: 0 }}>
            <path d="M7 1.5l5.5 9.5H1.5L7 1.5z" stroke="var(--yellow)" strokeWidth="1.3" />
            <path d="M7 5.5v2.5M7 9.5h.01" stroke="var(--yellow)" strokeWidth="1.3" strokeLinecap="round" />
          </svg>
          El alquiler representa el <strong>{Math.round(f.alquiler_sobre_ventas_pct * 100)}%</strong> de las ventas — por encima del 15% recomendado
        </div>
      )}

      {/* ── BLOQUE 2: Economía base ── */}
      <BloqueEconomia f={f} />

      {/* ── BLOQUE 3: Estructura de costes ── */}
      {f.estructura_costes && <BloqueCostes ec={f.estructura_costes} />}

      {/* ── BLOQUE 4: Break-even ── */}
      {f.break_even && <BloqueBreakEven be={f.break_even} alquilerPct={f.alquiler_sobre_ventas_pct} />}

      {/* ── BLOQUE 5: Escenarios ── */}
      <BloqueEscenarios f={f} />

      {/* ── BLOQUE 6: Métricas clave ── */}
      {f.metricas_clave && <BloqueMetricas m={f.metricas_clave} />}

      {/* ── BLOQUE 7: Riesgos ── */}
      <BloqueRiesgos riesgos={f.riesgos ?? []} />

      {/* ── BLOQUE 8: Insights ── */}
      <BloqueInsights insights={f.insights ?? []} />

      {/* ── Desglose inversión inicial ── */}
      {f.desglose_inversion && (
        <section className={styles.section}>
          <div className={styles.sectionHeader}>
            <h3 className={styles.sectionTitle}>Desglose inversión inicial</h3>
            <span className={styles.totalBadge}>
              {fmt(f.inversion_total)} €
            </span>
          </div>
          <div className={styles.desgloseGrid}>
            {Object.entries(f.desglose_inversion)
              .filter(([, val]) => val > 0)
              .map(([key, val]) => {
                const pct = val / f.inversion_total * 100;
                return (
                  <div key={key} className={styles.desgloseItem}>
                    <div className={styles.desgloseTop}>
                      <span className={styles.desgloseLabel}>{LABELS_DESGLOSE[key] ?? key}</span>
                      <span className={styles.desgloseValue}>{fmt(val)} €</span>
                    </div>
                    <div className={styles.desgloseBar}>
                      <div className={styles.desgloseBarFill} style={{ width: `${pct}%` }} />
                    </div>
                    <span className={styles.desglosePct}>{pct.toFixed(0)}%</span>
                  </div>
                );
              })}
          </div>
          {(params as Record<string, unknown>).num_empleados != null && (
            <div className={styles.empleadosBox}>
              {String((params as Record<string, unknown>).num_empleados)} empleado{Number((params as Record<string, unknown>).num_empleados) > 1 ? "s" : ""}
            </div>
          )}
        </section>
      )}

      {/* ── Ajustar parámetros (sliders) ── */}
      <section ref={slidersRef} className={styles.section}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
          <h3 className={styles.sectionTitle} style={{ margin: 0 }}>Ajustar parámetros</h3>
          <span style={{
            fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase",
            color: C.muted, background: C.surface2, border: `1px solid ${C.border}`,
            borderRadius: 4, padding: "2px 7px",
          }}>interactivo</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <ParamGroup color={C.green} icon="↑" label="Ingresos">
            {params.ticket_medio && (
              <SliderParam color={C.green} label="Ticket medio" unit="€"
                value={overrides.ticket_medio ?? (params.ticket_medio as { valor_usado: number }).valor_usado}
                min={(params.ticket_medio as { rango_min: number }).rango_min}
                max={(params.ticket_medio as { rango_max: number }).rango_max}
                fuente={(params.ticket_medio as { fuente: string }).fuente}
                onChange={v => handleOverride("ticket_medio", v)} />
            )}
            {params.clients_per_day && (
              <SliderParam color={C.green} label="Clientes / día (base)" unit=""
                value={overrides.clients_per_day ?? (params.clients_per_day as { valor_usado: number }).valor_usado}
                min={(params.clients_per_day as { rango_min: number }).rango_min}
                max={Math.max(
                  (params.clients_per_day as { rango_max: number }).rango_max,
                  (f.parametros as { max_capacity?: number }).max_capacity ?? (params.clients_per_day as { rango_max: number }).rango_max,
                )}
                fuente={(params.clients_per_day as { fuente: string }).fuente}
                onChange={v => handleOverride("clients_per_day", v)} />
            )}
            {params.dias_apertura_mes && (
              <SliderParam color={C.green} label="Días apertura / mes" unit=" días"
                value={overrides.dias_apertura_mes ?? (params.dias_apertura_mes as { valor_usado: number }).valor_usado}
                min={(params.dias_apertura_mes as { rango_min: number }).rango_min}
                max={(params.dias_apertura_mes as { rango_max: number }).rango_max}
                fuente={(params.dias_apertura_mes as { fuente: string }).fuente}
                onChange={v => handleOverride("dias_apertura_mes", v)} />
            )}
          </ParamGroup>

          <ParamGroup color={C.yellow} icon="≡" label="Costes fijos">
            {params.alquiler_mensual && (
              <SliderParam color={C.yellow} label="Alquiler mensual" unit="€"
                value={overrides.alquiler_mensual ?? (params.alquiler_mensual as { valor_usado: number }).valor_usado}
                min={(params.alquiler_mensual as { rango_min: number }).rango_min}
                max={(params.alquiler_mensual as { rango_max: number }).rango_max}
                fuente={(params.alquiler_mensual as { fuente: string }).fuente}
                onChange={v => handleOverride("alquiler_mensual", v)} />
            )}
            {params.salarios_mensual && (
              <SliderParam color={C.yellow} label="Salarios mensuales" unit="€"
                value={overrides.salarios_mensual ?? (params.salarios_mensual as { valor_usado: number }).valor_usado}
                min={(params.salarios_mensual as { rango_min: number }).rango_min}
                max={(params.salarios_mensual as { rango_max: number }).rango_max}
                fuente={(params.salarios_mensual as { fuente: string }).fuente}
                onChange={v => handleOverride("salarios_mensual", v)} />
            )}
            {params.otros_fijos_mensual && (
              <SliderParam color={C.yellow} label="Otros costes fijos" unit="€"
                value={overrides.otros_fijos_mensual ?? (params.otros_fijos_mensual as { valor_usado: number }).valor_usado}
                min={(params.otros_fijos_mensual as { rango_min: number }).rango_min}
                max={(params.otros_fijos_mensual as { rango_max: number }).rango_max}
                fuente={(params.otros_fijos_mensual as { fuente: string }).fuente}
                onChange={v => handleOverride("otros_fijos_mensual", v)} />
            )}
          </ParamGroup>

          <ParamGroup color={C.indigo} icon="%" label="Margen">
            {params.coste_mercancia_pct && (
              <SliderParam color={C.indigo} label="Coste mercancía" unit="%"
                value={Math.round((overrides.coste_mercancia_pct ?? (params.coste_mercancia_pct as { valor_usado: number }).valor_usado) * 100)}
                min={Math.round((params.coste_mercancia_pct as { rango_min: number }).rango_min * 100)}
                max={Math.round((params.coste_mercancia_pct as { rango_max: number }).rango_max * 100)}
                fuente={(params.coste_mercancia_pct as { fuente: string }).fuente}
                onChange={v => handleOverride("coste_mercancia_pct", v / 100)} />
            )}
          </ParamGroup>

          <ParamGroup color="#A78BFA" icon="⬡" label="Inversión inicial">
            {params.reforma_local && (
              <SliderParam color="#A78BFA" label="Reforma" unit=" €"
                value={overrides.reforma_local ?? (params.reforma_local as { valor_usado: number }).valor_usado}
                min={(params.reforma_local as { rango_min: number }).rango_min}
                max={(params.reforma_local as { rango_max: number }).rango_max}
                fuente={(params.reforma_local as { fuente: string }).fuente}
                onChange={v => handleOverride("reforma_local", v)} />
            )}
          </ParamGroup>
        </div>
      </section>
    </div>
  );
}
