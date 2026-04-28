"use client";

import React, { useState, useCallback, useRef, useEffect } from "react";
import {
  AreaChart, Area,
  BarChart, Bar, Cell,
  ComposedChart,
  LineChart, Line,
  XAxis, YAxis, CartesianGrid,
  Tooltip as ReTooltip,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import type {
  FinancieroResponse, DecisionBlock, EstructuraCostes,
  BreakEvenInfo, MetricasClave, Riesgo, Insight,
  CorreccionAplicada, CapacityModelInfo, BusinessContext,
  SensitividadItem,
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
  variable:    "#94A3B8",
  otros:       "#64748B",
  beneficio:   "#10B981",
  perdida:     "#EF4444",
  green:       "#10B981",
  yellow:      "#F59E0B",
  red:         "#EF4444",
  indigo:      "#6366F1",
  text:        "#E2E8F0",
  muted:       "#94A3B8",
  subtle:      "#64748B",
  border:      "rgba(255,255,255,0.06)",
  surface:     "rgba(255,255,255,0.025)",
  surface2:    "rgba(255,255,255,0.04)",
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

// ─── Tooltip de gráficos ──────────────────────────────────────────────────────
interface TooltipEntry { name: string; value: number; color: string; dataKey: string }
interface TooltipBaseProps { active?: boolean; payload?: TooltipEntry[]; label?: string | number }

function TooltipBase({ active, payload, label }: TooltipBaseProps) {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: "#111827", border: `1px solid ${C.border}`,
      borderRadius: 6, padding: "8px 12px", fontSize: 12,
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

// ─── Tooltip informativo (conceptos financieros) ──────────────────────────────
function InfoTooltip({ text }: { text: string }) {
  const [visible, setVisible] = useState(false);
  return (
    <span
      style={{ position: "relative", display: "inline-flex", verticalAlign: "middle" }}
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
    >
      <span style={{
        width: 14, height: 14, borderRadius: "50%",
        border: `1px solid ${C.subtle}`, color: C.subtle,
        fontSize: 9, cursor: "help", display: "inline-flex",
        alignItems: "center", justifyContent: "center", fontWeight: 700,
        marginLeft: 4, flexShrink: 0, userSelect: "none",
      }}>?</span>
      {visible && (
        <span style={{
          position: "absolute", bottom: "calc(100% + 6px)", left: "50%",
          transform: "translateX(-50%)", zIndex: 200,
          background: "#111827", border: `1px solid ${C.border}`,
          borderRadius: 6, padding: "8px 10px", width: 200,
          fontSize: 11, color: C.muted, lineHeight: 1.55,
          boxShadow: "0 4px 16px rgba(0,0,0,0.5)",
          pointerEvents: "none", whiteSpace: "normal",
          display: "block",
        }}>{text}</span>
      )}
    </span>
  );
}

// ─── Info popup por gráfico ───────────────────────────────────────────────────

const CHART_INFOS = {
  ebitdaMensual: "EBITDA mes a mes durante 36 meses. Verde = mes rentable, rojo = pérdida. Los primeros meses son negativos por la curva de arranque (rampa hasta mes 12). Las líneas punteadas muestran la variabilidad ±15%.",
  cajaAcumulada: "Saldo de caja desde el día 1 en tres escenarios. Cuando la curva cruza 0€, recuperas toda la inversión. La diferencia entre curvas refleja el riesgo de cada escenario a lo largo del tiempo.",
  cajaVsEbitda:  "El EBITDA acumulado es el beneficio operativo generado. La Caja real descuenta la inversión inicial. La brecha vertical entre ambas es el capital aún no recuperado.",
  comparativa:   "Ingresos totales por año y escenario (en miles €). El Año 1 es inferior por la rampa de arranque. A partir del Año 2 el negocio opera a plena capacidad (80% ocupación).",
  estres:        "Simula una crisis: solo el 40% de los ingresos esperados con todos los costes fijos intactos. Si la curva no cruza 0€ en 36 meses, necesitas reservas o financiación adicional.",
  costes:        "Costes operativos mensuales en régimen estable por categoría. La más grande es tu palanca de ahorro principal — personal + alquiler suelen ser el 70-80% del total.",
  capacidad:     "Compara la demanda estimada (clientes/día) con la capacidad máxima del local. Si la demanda supera la capacidad, necesitas más aforo, turnos adicionales o personal.",
} as const;

function ChartInfoButton({ infoKey }: { infoKey: keyof typeof CHART_INFOS }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  return (
    <div ref={ref} style={{ position: "relative", display: "inline-flex" }}>
      <button
        onClick={e => { e.stopPropagation(); setOpen(o => !o); }}
        aria-label="Información del gráfico"
        style={{
          all: "unset", cursor: "pointer",
          width: 16, height: 16, borderRadius: "50%",
          background: open ? "rgba(255,255,255,0.08)" : "rgba(255,255,255,0.04)",
          border: `1px solid ${open ? "rgba(255,255,255,0.14)" : "rgba(255,255,255,0.07)"}`,
          color: open ? C.muted : C.subtle,
          fontSize: 9, fontWeight: 700, letterSpacing: 0,
          display: "flex", alignItems: "center", justifyContent: "center",
          transition: "all 0.15s", flexShrink: 0,
        }}
      >i</button>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 8px)", right: 0,
          zIndex: 200, width: 280,
          background: "#111827",
          border: `1px solid ${C.border}`,
          borderRadius: 8, padding: "12px 14px",
          fontSize: 12, color: C.muted, lineHeight: 1.65,
          boxShadow: "0 8px 24px rgba(0,0,0,0.5)",
        }}>
          {CHART_INFOS[infoKey]}
        </div>
      )}
    </div>
  );
}

// ─── Componentes pequeños ─────────────────────────────────────────────────────

function SectionTitle({ title, badge, accent, info }: {
  title: string; badge?: React.ReactNode; accent?: string; info?: keyof typeof CHART_INFOS;
}) {
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
        {accent && (
          <span style={{
            width: 2, height: 12, borderRadius: 1,
            background: accent, flexShrink: 0, opacity: 0.8,
          }} />
        )}
        <h3 style={{
          fontSize: 10, fontWeight: 700, textTransform: "uppercase",
          letterSpacing: "0.08em", color: C.muted, margin: 0,
        }}>{title}</h3>
        {info && <ChartInfoButton infoKey={info} />}
      </div>
      {badge}
    </div>
  );
}

function ChartExplanation({ children }: { children: React.ReactNode }) {
  return (
    <p style={{
      fontSize: 11, color: C.subtle, lineHeight: 1.55,
      margin: "8px 0 0", borderTop: `1px solid ${C.border}`, paddingTop: 8,
    }}>{children}</p>
  );
}

function StatCard({
  label, value, sub, color, tooltip,
}: { label: string; value: string; sub?: string; color?: string; icon?: string; tooltip?: string }) {
  return (
    <div style={{
      padding: "14px 0",
      borderBottom: `1px solid ${C.border}`,
      display: "flex", flexDirection: "column", gap: 3,
    }}>
      <div style={{
        fontSize: 22, fontWeight: 800, color: color ?? C.text,
        fontVariantNumeric: "tabular-nums", lineHeight: 1, letterSpacing: "-0.02em",
      }}>{value}</div>
      <div style={{
        fontSize: 10, color: C.muted, textTransform: "uppercase",
        letterSpacing: "0.06em", display: "flex", alignItems: "center",
      }}>
        {label}
        {tooltip && <InfoTooltip text={tooltip} />}
      </div>
      {sub && <div style={{ fontSize: 10, color: C.subtle, marginTop: 1 }}>{sub}</div>}
    </div>
  );
}

// ─── Score de viabilidad global ──────────────────────────────────────────────

function computeViabilityScore(f: FinancieroResponse): number {
  const m = f.metricas_clave;
  const d = f.decision;
  if (!m || !d) return 50;

  let score = 0;

  // ROI base (0–25 pts)
  const roi = m.roi_base;
  score += roi >= 0.6 ? 25 : roi >= 0.4 ? 18 : roi >= 0.2 ? 10 : roi >= 0 ? 4 : 0;

  // Payback (0–20 pts)
  const pb = m.payback_meses;
  score += pb <= 12 ? 20 : pb <= 18 ? 16 : pb <= 24 ? 10 : pb <= 30 ? 5 : pb < 999 ? 1 : 0;

  // Beneficio mensual (0–20 pts)
  const bm = d.beneficio_mensual;
  score += bm > 4000 ? 20 : bm > 2000 ? 15 : bm > 500 ? 10 : bm > 0 ? 5 : 0;

  // Margen bruto (0–15 pts)
  const mb = m.margen_bruto_pct;
  score += mb >= 0.5 ? 15 : mb >= 0.35 ? 10 : mb >= 0.2 ? 5 : 0;

  // Gap de capital (0–10 pts)
  score += d.gap_capital <= 0 ? 10 : d.gap_capital < 10000 ? 5 : 0;

  // Recomendación directa (0–10 pts)
  score += d.recomendacion === "si" ? 10 : d.recomendacion === "riesgo" ? 5 : 0;

  // Penalizaciones
  if (f.alerta_alquiler) score = Math.max(0, score - 5);

  return Math.min(100, score);
}

// ─── BLOQUE 1: Decisión rápida ────────────────────────────────────────────────

const DECISION_CONFIG = {
  si:     { label: "ABRIR", bg: "rgba(16,185,129,0.12)", border: "rgba(16,185,129,0.3)", color: C.green,  emoji: "✓" },
  riesgo: { label: "CON RIESGO", bg: "rgba(245,158,11,0.12)", border: "rgba(245,158,11,0.3)", color: C.yellow, emoji: "!" },
  no:     { label: "NO ABRIR", bg: "rgba(239,68,68,0.12)",  border: "rgba(239,68,68,0.3)",  color: C.red,   emoji: "✕" },
};

function BloqueDecision({ d, viabilityScore }: { d: DecisionBlock; viabilityScore?: number }) {
  const cfg        = DECISION_CONFIG[d.recomendacion];
  const score      = viabilityScore ?? null;
  const scoreColor = score == null ? C.muted : score >= 70 ? C.green : score >= 45 ? C.yellow : C.red;
  return (
    <section className={styles.section}>
      <div style={{
        display: "flex", alignItems: "baseline", justifyContent: "space-between",
        marginBottom: 16,
      }}>
        <h3 style={{
          fontSize: 10, fontWeight: 700, textTransform: "uppercase",
          letterSpacing: "0.08em", color: C.muted, margin: 0,
        }}>Decisión</h3>
        {score != null && (
          <span style={{ fontSize: 11, color: C.muted, fontVariantNumeric: "tabular-nums" }}>
            Viabilidad{" "}
            <strong style={{ color: scoreColor, fontWeight: 800, fontSize: 13 }}>{score}</strong>
            <span style={{ color: C.subtle }}> / 100</span>
          </span>
        )}
      </div>
      <div style={{
        display: "flex", alignItems: "center", gap: 10,
        paddingBottom: 16, marginBottom: 14,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <span style={{
          width: 7, height: 7, borderRadius: "50%", flexShrink: 0,
          background: cfg.color,
        }} />
        <span style={{ fontSize: 16, fontWeight: 700, color: cfg.color, letterSpacing: "0.02em" }}>
          {cfg.label}
        </span>
        <span style={{ fontSize: 10, color: C.subtle, marginLeft: 2 }}>
          ROI ≥ 40% · Payback ≤ 18m · Alquiler ≤ 15%
        </span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 24px" }}>
        <StatCard
          label="Beneficio mensual"
          value={`${d.beneficio_mensual >= 0 ? "+" : ""}${fmt(d.beneficio_mensual)} €`}
          sub="en régimen estable (escenario base)"
          color={d.beneficio_mensual >= 0 ? C.green : C.red}
          tooltip="Beneficio neto mensual en régimen estable (mes 12, ocupación al 80%). Ingresos menos todos los costes operativos."
        />
        <StatCard
          label="Payback"
          value={d.payback >= 999 ? "+36m" : `${d.payback} meses`}
          sub="recuperación de inversión"
          color={d.payback <= 18 ? C.green : d.payback <= 30 ? C.yellow : C.red}
          tooltip="Payback: meses necesarios para recuperar toda la inversión inicial con los beneficios acumulados. Menos de 18 meses es ideal."
        />
        <StatCard
          label="Capital necesario"
          value={`${fmt(d.capital_necesario)} €`}
          sub="inversión inicial total"
          tooltip="Suma de reforma, equipamiento, depósito de fianza y otros gastos previos a la apertura."
        />
        <StatCard
          label="Gap de capital"
          value={d.gap_capital > 0 ? `${fmt(d.gap_capital)} €` : "Cubierto"}
          sub={d.gap_capital > 0 ? "financiación necesaria" : "capital suficiente"}
          color={d.gap_capital > 0 ? C.yellow : C.green}
          tooltip="Diferencia entre el capital necesario y el capital disponible. Si es positivo, necesitas financiación externa o socios."
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
            display: "flex", alignItems: "flex-start", gap: 8, padding: "8px 0",
            fontSize: 11, lineHeight: 1.55,
            borderBottom: `1px solid ${C.border}`,
          }}>
            <span style={{ color: C.yellow, flexShrink: 0, marginTop: 1, opacity: 0.8 }}>·</span>
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
            padding: "10px 0",
            borderBottom: `1px solid ${C.border}`,
          }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.06em" }}>
              {c.parametro.replace(/_/g, " ")}
            </div>
            <div style={{ fontSize: 11, color: C.subtle, lineHeight: 1.55 }}>{c.motivo}</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 5 }}>
              <span style={{ fontSize: 10, color: C.subtle, fontVariantNumeric: "tabular-nums" }}>
                {Math.round(c.valor_original)} → <strong style={{ color: C.muted }}>{Math.round(c.valor_corregido)}</strong>
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

// ─── BLOQUE: Modelo de capacidad ─────────────────────────────────────────────

function BloqueCapacidad({ cm }: { cm: CapacityModelInfo }) {
  return (
    <div style={{
      marginTop: 12, padding: "10px 0",
      borderTop: `1px solid ${C.border}`,
    }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: C.subtle, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
        Modelo de capacidad
      </div>
      <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.6 }}>
        {cm.descripcion}{" — "}
        <strong style={{ color: C.text }}>máx. {Math.round(cm.max_clients_day)} clientes/día</strong>
      </div>
    </div>
  );
}

// ─── BLOQUE: Configuración editable (sliders + empleados) ────────────────────

type ParamRecord = Record<string, unknown>;

function BloqueConfigEditable({
  f, overrides, onOverride, onBatchOverride, recalculating,
}: {
  f: FinancieroResponse;
  overrides: Record<string, number>;
  onOverride: (key: string, value: number) => void;
  onBatchOverride: (batch: Record<string, number>) => void;
  recalculating: boolean;
}) {
  const params = (f.parametros ?? {}) as ParamRecord;

  const numEmpRaw      = params.num_empleados;
  const numEmpSugerido = typeof numEmpRaw === "number" ? numEmpRaw
    : (numEmpRaw as { valor_usado?: number } | undefined)?.valor_usado ?? null;
  const salariosParam   = params.salarios_mensual as { valor_usado?: number } | undefined;
  const salariosTotales = salariosParam?.valor_usado ?? 0;

  // Current values — overrides take precedence
  const currentEmpleados = overrides.num_empleados != null
    ? Math.max(1, Math.round(overrides.num_empleados))
    : (numEmpSugerido != null ? Math.max(1, Math.round(numEmpSugerido)) : null);
  const currentSalarios = overrides.salarios_mensual ?? salariosTotales;

  // Salary per employee — always derived from current state
  const salarioPorEmpActual = currentEmpleados && currentEmpleados > 0
    ? Math.max(800, Math.round(currentSalarios / currentEmpleados))
    : 1500;

  // Impact vs model
  const salaryDelta   = currentSalarios - salariosTotales;
  const benefitImpact = -salaryDelta;
  const showImpact    = salariosTotales > 0 && Math.abs(benefitImpact) > 50;

  // Staff capacity from backend (max_staff_capacity reflects num_empleados × productividad)
  const maxStaffCapacity = f.max_staff_capacity ?? 0;
  const clientsParam     = params.clients_per_day as { valor_usado?: number } | undefined;
  const currentClients   = overrides.clients_per_day ?? clientsParam?.valor_usado ?? null;

  // Capacity: local (aforo) vs staff
  const eb           = f.economia_base;
  const capModel     = f.capacity_model;
  const ocupEfectiva = eb?.ocupacion_efectiva
    ?? (f as unknown as { ocupacion_efectiva?: number }).ocupacion_efectiva ?? 0.8;
  const maxCapacidad = capModel?.max_clients_day
    ?? (eb ? Math.round(eb.clientes_dia / Math.max(0.1, ocupEfectiva)) : null);
  const exceedsCapacity = maxCapacidad != null && currentClients != null && currentClients > maxCapacidad;

  // Staff-specific warnings
  const staffCapacity = maxStaffCapacity > 0 ? maxStaffCapacity : null;
  const exceedsStaff  = staffCapacity != null && currentClients != null && currentClients > staffCapacity;
  const overStaffed   = staffCapacity != null && currentClients != null && currentEmpleados != null
    && currentClients < staffCapacity * 0.55 && currentEmpleados > 1;

  const handleEmpleadoCount = (n: number) => {
    onBatchOverride({ num_empleados: n, salarios_mensual: Math.round(n * salarioPorEmpActual) });
  };

  const handleSalPorEmp = (sal: number) => {
    const empCount = currentEmpleados ?? 1;
    onBatchOverride({ num_empleados: empCount, salarios_mensual: Math.round(empCount * sal) });
  };

  const btnStyle: React.CSSProperties = {
    all: "unset", cursor: "pointer",
    width: 26, height: 26, borderRadius: 6,
    background: C.surface2, border: `1px solid ${C.border}`,
    color: C.text, fontSize: 16, display: "flex",
    alignItems: "center", justifyContent: "center",
    fontWeight: 700, flexShrink: 0,
  };

  return (
    <section className={styles.section} style={{ borderTop: `1px solid ${C.border}` }}>
      <SectionTitle
        title="Configuración editable"
        badge={
          recalculating ? (
            <span style={{
              fontSize: 10, color: C.muted, display: "flex", alignItems: "center", gap: 5,
            }}>
              <span style={{
                width: 7, height: 7, borderRadius: "50%", flexShrink: 0,
                border: `1.5px solid ${C.muted}`,
                borderTopColor: "transparent",
                display: "inline-block",
                animation: "spin 0.7s linear infinite",
              }} />
              Recalculando...
            </span>
          ) : (
            <span style={{
              fontSize: 10, color: C.subtle,
            }}>ajusta y recalcula</span>
          )
        }
      />

      {/* Plantilla: empleados y salarios */}
      {currentEmpleados != null && (
        <div style={{
          background: C.surface, border: `1px solid ${C.border}`,
          borderRadius: 8, padding: "12px 14px", marginBottom: 12,
        }}>
          <div style={{
            fontSize: 10, fontWeight: 700, textTransform: "uppercase",
            letterSpacing: "0.07em", color: C.muted, marginBottom: 10,
          }}>Plantilla</div>

          {/* Stepper empleados */}
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
            <div>
              <div style={{ fontSize: 12, color: C.muted, fontWeight: 500, display: "flex", alignItems: "center" }}>
                Número de empleados
                <InfoTooltip text="Cuántas personas trabajan en el negocio. Al cambiarlo se recalcula la capacidad de atención, los costes de personal, el beneficio y el ROI." />
              </div>
              {numEmpSugerido != null && (
                <div style={{ fontSize: 10, color: C.subtle, marginTop: 3 }}>
                  Sugerido:{" "}
                  <strong style={{ color: C.muted }}>
                    {Math.round(numEmpSugerido)} empleado{Math.round(numEmpSugerido) !== 1 ? "s" : ""}
                  </strong>
                </div>
              )}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <button style={btnStyle} onClick={() => handleEmpleadoCount(Math.max(1, currentEmpleados - 1))}>−</button>
              <span style={{
                fontSize: 22, fontWeight: 800, color: C.text,
                fontVariantNumeric: "tabular-nums", minWidth: 30, textAlign: "center",
              }}>{currentEmpleados}</span>
              <button style={btnStyle} onClick={() => handleEmpleadoCount(Math.min(20, currentEmpleados + 1))}>+</button>
            </div>
          </div>

          {/* Slider: coste por empleado */}
          <SliderParam
            color={C.yellow}
            label="Coste/empleado (salario bruto + SS)"
            unit="€/mes"
            value={salarioPorEmpActual}
            min={800}
            max={5000}
            fuente="Bruto + ~31% Seg. Social empresa · convenio colectivo"
            onChange={handleSalPorEmp}
          />

          {/* Coste total + impacto */}
          <div style={{
            display: "flex", alignItems: "center", justifyContent: "space-between",
            marginTop: 10, paddingTop: 8, borderTop: `1px solid ${C.border}`,
          }}>
            <span style={{ fontSize: 11, color: C.subtle }}>Coste personal total/mes</span>
            <div style={{ textAlign: "right" }}>
              <span style={{ fontSize: 13, fontWeight: 800, color: C.text }}>
                {fmt(currentSalarios)} €
              </span>
              {showImpact && (
                <div style={{ fontSize: 11, fontWeight: 700, color: benefitImpact > 0 ? C.green : C.red }}>
                  {benefitImpact > 0 ? "+" : "−"}{fmt(Math.abs(benefitImpact))} €{" "}
                  <span style={{ fontWeight: 400, color: C.subtle }}>vs modelo</span>
                </div>
              )}
            </div>
          </div>

          {/* Personal insuficiente para la demanda */}
          {exceedsStaff && currentClients != null && staffCapacity != null && (
            <div style={{
              display: "flex", alignItems: "flex-start", gap: 8,
              padding: "8px 10px", borderRadius: 8, marginTop: 10,
              background: "rgba(239,68,68,0.05)", border: `1px solid ${C.border}`,
              fontSize: 11, color: C.muted, lineHeight: 1.55,
            }}>
              <span style={{ color: C.red, fontWeight: 700, flexShrink: 0 }}>!</span>
              <span>
                Con <strong style={{ color: C.text }}>{currentEmpleados} empleado{currentEmpleados !== 1 ? "s" : ""}</strong>{" "}
                la capacidad es de{" "}
                <strong style={{ color: C.red }}>{Math.round(staffCapacity)} clientes/día</strong>,
                pero tienes <strong style={{ color: C.text }}>{Math.round(currentClients)} estimados</strong>.{" "}
                Añade personal o reduce la demanda objetivo.
              </span>
            </div>
          )}

          {/* Sobredimensión */}
          {overStaffed && !exceedsStaff && currentClients != null && staffCapacity != null && (
            <div style={{
              display: "flex", alignItems: "flex-start", gap: 8,
              padding: "8px 10px", borderRadius: 8, marginTop: 10,
              background: "rgba(245,158,11,0.04)", border: `1px solid ${C.border}`,
              fontSize: 11, color: C.muted, lineHeight: 1.55,
            }}>
              <span style={{ color: C.yellow, fontWeight: 700, flexShrink: 0, fontSize: 10 }}>!</span>
              <span>
                Posible sobredimensión: el equipo puede atender{" "}
                <strong style={{ color: C.text }}>{Math.round(staffCapacity)}</strong> clientes/día
                pero solo esperas <strong style={{ color: C.text }}>{Math.round(currentClients)}</strong>.{" "}
                Los costes de personal son elevados para la demanda estimada.
              </span>
            </div>
          )}
        </div>
      )}

      {/* Validación: clientes > capacidad del local (solo si no hay ya warning de personal) */}
      {exceedsCapacity && !exceedsStaff && (
        <div style={{
          display: "flex", alignItems: "flex-start", gap: 8,
          padding: "8px 12px", borderRadius: 8, marginBottom: 8,
          background: "rgba(239,68,68,0.05)", border: `1px solid ${C.border}`,
          fontSize: 11, color: C.muted, lineHeight: 1.55,
        }}>
          <span style={{ color: C.red, fontWeight: 700, flexShrink: 0 }}>!</span>
          <span>
            <strong style={{ color: C.red }}>{Math.round(currentClients!)} clientes/día</strong>{" "}
            supera la capacidad estimada del local{" "}
            (<strong style={{ color: C.text }}>{Math.round(maxCapacidad!)} máx.</strong>).{" "}
            Considera reducir el aforo o aumentar turnos.
          </span>
        </div>
      )}

      {/* Sliders */}
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <ParamGroup color={C.green} icon="↑" label="Ingresos">
          {params.ticket_medio && (
            <SliderParam color={C.green} label="Ticket medio" unit="€"
              value={overrides.ticket_medio ?? (params.ticket_medio as { valor_usado: number }).valor_usado}
              min={(params.ticket_medio as { rango_min: number }).rango_min}
              max={(params.ticket_medio as { rango_max: number }).rango_max}
              fuente={(params.ticket_medio as { fuente: string }).fuente}
              onChange={v => onOverride("ticket_medio", v)} />
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
              onChange={v => onOverride("clients_per_day", v)} />
          )}
          {params.dias_apertura_mes && (
            <SliderParam color={C.green} label="Días apertura / mes" unit=" días"
              value={overrides.dias_apertura_mes ?? (params.dias_apertura_mes as { valor_usado: number }).valor_usado}
              min={(params.dias_apertura_mes as { rango_min: number }).rango_min}
              max={(params.dias_apertura_mes as { rango_max: number }).rango_max}
              fuente={(params.dias_apertura_mes as { fuente: string }).fuente}
              onChange={v => onOverride("dias_apertura_mes", v)} />
          )}
        </ParamGroup>

        <ParamGroup color={C.yellow} icon="≡" label="Costes fijos">
          {params.alquiler_mensual && (
            <SliderParam color={C.yellow} label="Alquiler mensual" unit="€"
              value={overrides.alquiler_mensual ?? (params.alquiler_mensual as { valor_usado: number }).valor_usado}
              min={(params.alquiler_mensual as { rango_min: number }).rango_min}
              max={(params.alquiler_mensual as { rango_max: number }).rango_max}
              fuente={(params.alquiler_mensual as { fuente: string }).fuente}
              onChange={v => onOverride("alquiler_mensual", v)} />
          )}
          {params.salarios_mensual && (
            <SliderParam color={C.yellow} label="Salarios mensuales" unit="€"
              value={overrides.salarios_mensual ?? (params.salarios_mensual as { valor_usado: number }).valor_usado}
              min={(params.salarios_mensual as { rango_min: number }).rango_min}
              max={(params.salarios_mensual as { rango_max: number }).rango_max}
              fuente={(params.salarios_mensual as { fuente: string }).fuente}
              onChange={v => onOverride("salarios_mensual", v)} />
          )}
          {params.otros_fijos_mensual && (
            <SliderParam color={C.yellow} label="Otros costes fijos" unit="€"
              value={overrides.otros_fijos_mensual ?? (params.otros_fijos_mensual as { valor_usado: number }).valor_usado}
              min={(params.otros_fijos_mensual as { rango_min: number }).rango_min}
              max={(params.otros_fijos_mensual as { rango_max: number }).rango_max}
              fuente={(params.otros_fijos_mensual as { fuente: string }).fuente}
              onChange={v => onOverride("otros_fijos_mensual", v)} />
          )}
        </ParamGroup>

        <ParamGroup color={C.indigo} icon="%" label="Margen">
          {params.coste_mercancia_pct && (
            <SliderParam color={C.indigo} label="Coste mercancía" unit="%"
              value={Math.round((overrides.coste_mercancia_pct ?? (params.coste_mercancia_pct as { valor_usado: number }).valor_usado) * 100)}
              min={Math.round((params.coste_mercancia_pct as { rango_min: number }).rango_min * 100)}
              max={Math.round((params.coste_mercancia_pct as { rango_max: number }).rango_max * 100)}
              fuente={(params.coste_mercancia_pct as { fuente: string }).fuente}
              onChange={v => onOverride("coste_mercancia_pct", v / 100)} />
          )}
        </ParamGroup>

        <ParamGroup color="#A78BFA" icon="⬡" label="Inversión inicial">
          {params.reforma_local && (
            <SliderParam color="#A78BFA" label="Reforma" unit=" €"
              value={overrides.reforma_local ?? (params.reforma_local as { valor_usado: number }).valor_usado}
              min={(params.reforma_local as { rango_min: number }).rango_min}
              max={(params.reforma_local as { rango_max: number }).rango_max}
              fuente={(params.reforma_local as { fuente: string }).fuente}
              onChange={v => onOverride("reforma_local", v)} />
          )}
        </ParamGroup>
      </div>
    </section>
  );
}

// ─── BLOQUE 2: Economía base ──────────────────────────────────────────────────

function BloqueEconomia({ f }: { f: FinancieroResponse }) {
  const eb = f.economia_base;
  const md = f.modelo_demanda;
  if (!eb) return null;

  const conversionPct  = Math.round(eb.conversion_pct * 100);
  const modelType      = f.business_model_type ?? "retail_walkin";
  const modelLabel     = BUSINESS_MODEL_LABELS[modelType] ?? BUSINESS_MODEL_LABELS.retail_walkin;
  const isAppointment  = modelType === "appointment_based";
  const ocupacionPct   = Math.round((eb.ocupacion_efectiva ?? (f as unknown as { ocupacion_efectiva?: number }).ocupacion_efectiva ?? 0) * 100);
  const ocupacionColor = ocupacionPct > 85 ? C.red : ocupacionPct > 65 ? C.yellow : C.green;

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

      {f.capacity_model && <BloqueCapacidad cm={f.capacity_model} />}

      {md && !isAppointment && (
        <div style={{
          marginTop: 12, padding: "10px 0",
          borderTop: `1px solid ${C.border}`,
        }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: C.subtle, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
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

// ─── BLOQUE 3: Estructura de costes ──────────────────────────────────────────

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
      <div style={{ height: 80 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={barData} layout="vertical" margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
            <XAxis type="number" hide tick={false} axisLine={false} />
            <YAxis type="category" dataKey="name" hide />
            <ReTooltip content={<TooltipBase />} cursor={{ fill: "rgba(255,255,255,0.03)" }} />
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

// ─── BLOQUE 4: Break-even ─────────────────────────────────────────────────────

function BloqueBreakEven({ be, alquilerPct }: { be: BreakEvenInfo; alquilerPct: number }) {
  const margen     = be.margen_sobre_be_pct;
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
      <div style={{ display: "flex", gap: 0, marginBottom: 14, alignItems: "stretch", borderBottom: `1px solid ${C.border}`, paddingBottom: 14 }}>
        <div style={{
          flex: 1, padding: "8px 0", textAlign: "center",
          borderRight: `1px solid ${C.border}`,
        }}>
          <div style={{ fontSize: 32, fontWeight: 800, color: C.yellow, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>{be.clientes_be}</div>
          <div style={{ fontSize: 10, color: C.subtle, textTransform: "uppercase", letterSpacing: "0.05em", marginTop: 5 }}>
            clientes/día mínimos
          </div>
        </div>
        <div style={{
          flex: 1, padding: "8px 0", textAlign: "center",
        }}>
          <div style={{ fontSize: 32, fontWeight: 800, color: suficiente ? C.green : C.red, lineHeight: 1, fontVariantNumeric: "tabular-nums" }}>
            {Math.round(be.clientes_base)}
          </div>
          <div style={{ fontSize: 10, color: C.subtle, textTransform: "uppercase", letterSpacing: "0.05em", marginTop: 5 }}>
            clientes/día estimados
          </div>
        </div>
      </div>
      <div style={{ height: 150 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={be.chart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis
              dataKey="clientes" type="number"
              tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false}
              label={{ value: "clientes/día", position: "insideBottomRight", offset: -4, fill: C.subtle, fontSize: 10 }}
            />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
            <ReTooltip content={<TooltipBase />} />
            <ReferenceLine
              x={be.clientes_be} stroke={C.yellow} strokeDasharray="5 3"
              label={{ value: `BE: ${be.clientes_be}`, fill: C.yellow, fontSize: 10, position: "top" }}
            />
            <ReferenceLine
              x={be.clientes_base} stroke={C.base} strokeDasharray="5 3"
              label={{ value: `Base: ${Math.round(be.clientes_base)}`, fill: C.base, fontSize: 10, position: "top" }}
            />
            <Line type="monotone" dataKey="ingresos"       name="Ingresos"       stroke={C.green} strokeWidth={1.5} dot={false} />
            <Line type="monotone" dataKey="costes_totales" name="Costes totales" stroke={C.red}   strokeWidth={2} dot={false} strokeDasharray="4 2" />
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

// ─── BLOQUE: Métricas operativas avanzadas ────────────────────────────────────

function BloqueMetricasAvanzadas({ f }: { f: FinancieroResponse }) {
  const ec = f.estructura_costes;
  const eb = f.economia_base;
  if (!ec || !eb) return null;

  const params     = (f.parametros ?? {}) as ParamRecord;
  const numEmpRaw2 = params.num_empleados;
  const numEmp     = Math.max(1, typeof numEmpRaw2 === "number" ? Math.round(numEmpRaw2) : 1);

  const costePorEmp         = Math.round(ec.personal / numEmp);
  const totalCostes         = ec.alquiler + ec.personal + ec.variable + ec.otros;
  const ratioCostesIngresos = ec.ingresos_totales > 0
    ? Math.round((totalCostes / ec.ingresos_totales) * 100)
    : 0;
  const ocupacionPct = Math.round(
    (eb.ocupacion_efectiva ?? (f as unknown as { ocupacion_efectiva?: number }).ocupacion_efectiva ?? 0) * 100,
  );

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Métricas operativas"
        badge={
          <span style={{
            fontSize: 10, color: C.muted, background: C.surface2,
            border: `1px solid ${C.border}`, borderRadius: 100, padding: "2px 8px",
          }}>régimen estable</span>
        }
      />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <StatCard
          label="Coste por empleado"
          value={`${fmt(costePorEmp)} €`}
          sub={`${numEmp} empleado${numEmp !== 1 ? "s" : ""} en nómina`}
          tooltip="Coste mensual medio por empleado incluyendo salario bruto y seguridad social (factor ×1.31 SS)."
        />
        <StatCard
          label="Ingreso por cliente"
          value={`${Math.round(eb.ticket_medio)} €`}
          sub="ticket medio estimado"
          tooltip="Importe medio que genera cada visita o compra. Basado en la mediana de precios de competidores."
        />
        <StatCard
          label="Ratio costes / ingresos"
          value={`${ratioCostesIngresos}%`}
          sub={ratioCostesIngresos < 60 ? "eficiente" : ratioCostesIngresos < 75 ? "normal" : "riesgo — costes altos"}
          color={ratioCostesIngresos < 60 ? C.green : ratioCostesIngresos < 75 ? C.yellow : C.red}
          tooltip="Si superas el 75% el negocio tiene poco colchón ante imprevistos — toca reducir costes o subir ticket medio."
        />
        <StatCard
          label="Capacidad utilizada"
          value={`${ocupacionPct}%`}
          sub="sobre capacidad máxima (techo 80%)"
          color={ocupacionPct > 85 ? C.red : ocupacionPct > 65 ? C.yellow : C.green}
          tooltip="Porcentaje de tu capacidad operativa que estás usando. El modelo limita al 80% en régimen estable para ser conservador."
        />
      </div>
    </section>
  );
}

// ─── BLOQUE: Sensibilidad (tornado chart) ────────────────────────────────────

function BloqueSensibilidad({ items }: { items: SensitividadItem[] }) {
  if (!items.length) return null;
  const maxAbs = Math.max(...items.map(i => Math.abs(i.impacto_ebitda)), 1);

  return (
    <section className={styles.section}>
      <SectionTitle
        title="Sensibilidad — impacto si variable sube 10%"
        badge={
          <span style={{ fontSize: 10, color: C.muted, background: C.surface2,
            border: `1px solid ${C.border}`, borderRadius: 100, padding: "2px 8px" }}>
            EBITDA año 1
          </span>
        }
      />
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        {items.map((item) => {
          const positive = item.impacto_ebitda >= 0;
          const pct      = Math.abs(item.impacto_ebitda) / maxAbs * 100;
          const color    = positive ? C.green : C.red;
          return (
            <div key={item.variable}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                <span style={{ fontSize: 11, color: C.muted }}>{item.label}</span>
                <span style={{ fontSize: 11, fontWeight: 700, color, fontVariantNumeric: "tabular-nums" }}>
                  {positive ? "+" : ""}{fmt(item.impacto_ebitda)} €
                </span>
              </div>
              <div style={{ height: 6, borderRadius: 4, background: "rgba(255,255,255,0.06)" }}>
                <div style={{
                  height: "100%", borderRadius: 4, width: `${pct}%`,
                  background: color, opacity: 0.7,
                }} />
              </div>
            </div>
          );
        })}
      </div>
      <ChartExplanation>
        Cada barra muestra cuánto cambia el EBITDA del año 1 si esa variable aumenta un 10%.
        Las barras verdes son palancas de mejora; las rojas, riesgos que hay que negociar o controlar.
      </ChartExplanation>
    </section>
  );
}

// ─── Leyenda de gráficos ──────────────────────────────────────────────────────
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

// ─── CHART: EBITDA mensual — 36 meses ────────────────────────────────────────
function ChartEbitdaMensual({ f }: { f: FinancieroResponse }) {
  if (!f.proyeccion?.length) return null;
  const costeMercPct = ((f.parametros as Record<string, unknown>).coste_mercancia_pct as { valor_usado?: number })?.valor_usado ?? 0.40;
  const margenUnit   = 1 - costeMercPct;
  const mensualData  = f.proyeccion.map(m => {
    const cfMes = m.costes_fijos ?? 0;
    const low   = m.ingresos_base_low  != null ? Math.round(m.ingresos_base_low  * margenUnit) - cfMes : null;
    const high  = m.ingresos_base_high != null ? Math.round(m.ingresos_base_high * margenUnit) - cfMes : null;
    return { mes: m.mes, ebitda: m.ebitda_base, ebitda_low: low, ebitda_high: high };
  });
  return (
    <section className={styles.section}>
      <SectionTitle title="EBITDA mensual — 36 meses" accent={C.green} info="ebitdaMensual" />
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={mensualData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false}
              tickFormatter={v => (v % 6 === 0 || v === 1) ? `M${v}` : ""} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.18)" />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number) => [`${fmt(v)} €`, "EBITDA base"]} labelFormatter={v => `Mes ${v}`} />
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
    </section>
  );
}

// ─── CHART: Caja acumulada — 3 escenarios ────────────────────────────────────
function ChartCajaAcumulada({ f }: { f: FinancieroResponse }) {
  if (!f.proyeccion?.length) return null;
  const cajaData = f.proyeccion
    .filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({
      mes: `M${m.mes}`,
      Conservador: m.acumulado_conservador,
      Base:        m.acumulado_base,
      Optimista:   m.acumulado_optimista,
    }));
  return (
    <section className={styles.section}>
      <SectionTitle title="Caja acumulada — 3 escenarios" accent={C.conservador} info="cajaAcumulada" />
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={cajaData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <defs>
              {([["gOptC", C.optimista], ["gBaseC", C.base], ["gConsC", C.conservador]] as const).map(([id, color]) => (
                <linearGradient key={id} id={id} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor={color} stopOpacity={0.08} />
                  <stop offset="95%" stopColor={color} stopOpacity={0} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.12)" strokeDasharray="4 4" />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number, name: string) => [`${fmt(v)} €`, name]} />
            <Area type="monotone" dataKey="Optimista"   stroke={C.optimista}   fill="url(#gOptC)"  strokeWidth={1.5} dot={false} />
            <Area type="monotone" dataKey="Base"        stroke={C.base}        fill="url(#gBaseC)" strokeWidth={1.5} dot={false} />
            <Area type="monotone" dataKey="Conservador" stroke={C.conservador} fill="url(#gConsC)" strokeWidth={1.5} dot={false} />
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
        La curva base cruza cero en el mes {f.payback_meses_base >= 999 ? "nunca (ajusta parámetros)" : f.payback_meses_base}.
      </ChartExplanation>
    </section>
  );
}

// ─── CHART: Caja real vs EBITDA acumulado ────────────────────────────────────
function ChartCajaVsEbitda({ f }: { f: FinancieroResponse }) {
  if (!f.proyeccion?.length) return null;
  let cumEbitda = 0;
  const cajaVsEbitdaData = f.proyeccion.map(m => {
    cumEbitda += m.ebitda_base;
    return { mes: m.mes, ebitdaCum: Math.round(cumEbitda), cajaReal: m.acumulado_base };
  }).filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({ mes: `M${m.mes}`, "EBITDA acum.": m.ebitdaCum, "Caja real": m.cajaReal }));
  return (
    <section className={styles.section}>
      <SectionTitle title="Caja real vs EBITDA acumulado" accent={C.indigo} info="cajaVsEbitda" />
      <div style={{
        padding: "8px 0", marginBottom: 10,
        borderBottom: `1px solid ${C.border}`,
        fontSize: 11, color: C.subtle, lineHeight: 1.5,
      }}>
        <span style={{ color: C.muted, fontWeight: 600 }}>EBITDA acumulado</span> = beneficio operativo generado.{" "}
        <span style={{ color: C.muted, fontWeight: 600 }}>Caja real</span> = EBITDA menos la inversión inicial.
      </div>
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={cajaVsEbitdaData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="gEbitdaAcum" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={C.indigo} stopOpacity={0.07} />
                <stop offset="95%" stopColor={C.indigo} stopOpacity={0} />
              </linearGradient>
              <linearGradient id="gCajaReal" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={C.base} stopOpacity={0.06} />
                <stop offset="95%" stopColor={C.base} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.18)" strokeDasharray="4 4" />
            {f.payback_meses_base < 999 && (() => {
              const pbLabel = `M${f.payback_meses_base}`;
              const inData  = cajaVsEbitdaData.some(d => d.mes === pbLabel);
              if (!inData) return null;
              return (
                <ReferenceLine x={pbLabel} stroke={C.base} strokeDasharray="3 3"
                  label={{ value: `Payback M${f.payback_meses_base}`, fill: C.base, fontSize: 10, position: "top" }} />
              );
            })()}
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number, name: string) => [`${fmt(v)} €`, name]} />
            <Area type="monotone" dataKey="EBITDA acum." stroke={C.indigo} fill="url(#gEbitdaAcum)" strokeWidth={1.5} dot={false} />
            <Area type="monotone" dataKey="Caja real"    stroke={C.base}   fill="url(#gCajaReal)"   strokeWidth={1.5} dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      {f.payback_meses_base < 999 && (
        <div style={{
          marginTop: 8, fontSize: 11, color: C.subtle, textAlign: "center",
        }}>
          Caja negativa hasta el mes{" "}
          <span style={{ color: C.muted, fontWeight: 700 }}>{f.payback_meses_base}</span>{" "}
          — recuperas toda la inversión en ese punto
        </div>
      )}
      <LegendRow items={[
        { color: C.indigo, label: "EBITDA acumulado (beneficio operativo)" },
        { color: C.base,   label: `Caja real (payback mes ${f.payback_meses_base >= 999 ? ">36" : f.payback_meses_base})` },
      ]} />
      <ChartExplanation>
        La brecha vertical entre ambas curvas representa el capital invertido aún no recuperado.
        Cuando la caja real cruza cero, has recuperado toda la inversión inicial.
      </ChartExplanation>
    </section>
  );
}

// ─── CHART: Comparativa escenarios — 3 años ──────────────────────────────────
function ChartComparativa({ f }: { f: FinancieroResponse }) {
  if (!f.proyeccion?.length) return null;
  const p = f.proyeccion;
  const comparativaData = [
    { periodo: "Año 1",
      Conservador: Math.round(p.slice(0, 12).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(p.slice(0, 12).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(p.slice(0, 12).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000) },
    { periodo: "Año 2",
      Conservador: Math.round(p.slice(12, 24).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(p.slice(12, 24).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(p.slice(12, 24).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000) },
    { periodo: "Año 3",
      Conservador: Math.round(p.slice(24, 36).reduce((s, m) => s + m.ingresos_conservador, 0) / 1000),
      Base:        Math.round(p.slice(24, 36).reduce((s, m) => s + m.ingresos_base, 0) / 1000),
      Optimista:   Math.round(p.slice(24, 36).reduce((s, m) => s + m.ingresos_optimista, 0) / 1000) },
  ];
  return (
    <section className={styles.section}>
      <SectionTitle title="Comparativa escenarios — 3 años" accent={C.yellow} info="comparativa" />
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={comparativaData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="periodo" tick={{ fill: C.subtle, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={v => `${v}k`} width={34} />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number, name: string) => [`${fmt(v)}k €`, name]} />
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
    </section>
  );
}

// ─── CHART: Escenario estrés — cash runway ────────────────────────────────────
function ChartEstres({ f }: { f: FinancieroResponse }) {
  if (!f.proyeccion?.length) return null;
  const stressData = f.proyeccion
    .filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({ mes: `M${m.mes}`, Base: m.acumulado_base, Estrés: m.acumulado_stress ?? 0 }));
  return (
    <section className={styles.section}>
      <SectionTitle title="Escenario estrés — cash runway" accent={C.red} info="estres" />
      <div style={{
        padding: "8px 12px", borderRadius: 8, marginBottom: 10,
        background: "rgba(239,68,68,0.06)", border: "1px solid rgba(239,68,68,0.2)",
        fontSize: 11, color: C.muted, lineHeight: 1.5,
      }}>
        <span style={{ color: C.red, fontWeight: 700 }}>Escenario estrés:</span>{" "}
        ingresos al 40% con <strong style={{ color: C.text }}>costes fijos intactos</strong>.
        Si la caja estrés nunca sube a cero, necesitas reserva adicional para sobrevivir.
      </div>
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={stressData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="gStress" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={C.red}  stopOpacity={0.07} />
                <stop offset="95%" stopColor={C.red}  stopOpacity={0} />
              </linearGradient>
              <linearGradient id="gBaseS" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor={C.base} stopOpacity={0.06} />
                <stop offset="95%" stopColor={C.base} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="mes" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} width={34} />
            <ReferenceLine y={0} stroke="rgba(255,255,255,0.18)" strokeDasharray="4 4" />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number, name: string) => [`${fmt(v)} €`, name]} />
            <Area type="monotone" dataKey="Base"   name="Base (×1.0)"    stroke={C.base} fill="url(#gBaseS)" strokeWidth={1.5} dot={false} strokeDasharray="4 3" />
            <Area type="monotone" dataKey="Estrés" name="Estrés (×0.40)" stroke={C.red}  fill="url(#gStress)" strokeWidth={2}  dot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      <LegendRow items={[
        { color: C.base, label: `Base (payback ${f.payback_meses_base >= 999 ? ">36m" : f.payback_meses_base + "m"})` },
        { color: C.red,  label: `Estrés (payback ${(f.payback_meses_stress ?? 999) >= 999 ? ">36m" : f.payback_meses_stress + "m"})` },
      ]} />
      <ChartExplanation>
        Si la curva de estrés nunca cruza cero, el negocio necesita reserva de tesorería adicional.
        Compara con tu capital disponible para evaluar el riesgo real de quedarte sin caja.
      </ChartExplanation>
    </section>
  );
}

// ─── CHART: Distribución de costes mensuales ─────────────────────────────────
function ChartCostesDistribucion({ f }: { f: FinancieroResponse }) {
  const ec = f.estructura_costes;
  if (!ec) return null;
  const costesDistData = [
    { name: "Personal",  value: ec.personal, fill: C.personal },
    { name: "Alquiler",  value: ec.alquiler, fill: C.alquiler },
    { name: "Variable",  value: ec.variable, fill: C.variable },
    { name: "Otros",     value: ec.otros,    fill: C.otros    },
  ].sort((a, b) => b.value - a.value);
  return (
    <section className={styles.section}>
      <SectionTitle title="Distribución de costes mensuales" accent={C.yellow} info="costes" />
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={costesDistData} layout="vertical" margin={{ top: 4, right: 50, left: 8, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" horizontal={false} />
            <XAxis type="number" tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={fmtK} />
            <YAxis type="category" dataKey="name" tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} width={64} />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number) => [`${fmt(v)} €/mes`, ""]} />
            <Bar dataKey="value" name="Coste" radius={[0, 4, 4, 0]}>
              {costesDistData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <ChartExplanation>
        Distribución de costes operativos mensuales en régimen estable.{" "}
        {ec.personal > ec.alquiler
          ? `El personal (${fmt(ec.personal)} €) supera al alquiler (${fmt(ec.alquiler)} €) — palanca clave: empleados y turnos.`
          : `El alquiler (${fmt(ec.alquiler)} €) es el mayor coste — negocia bien el contrato.`}
      </ChartExplanation>
    </section>
  );
}

// ─── CHART: Capacidad vs demanda ─────────────────────────────────────────────
function ChartCapacidad({ f }: { f: FinancieroResponse }) {
  const eb = f.economia_base;
  if (!eb) return null;
  const ocupacionEfectiva = eb.ocupacion_efectiva
    ?? (f as unknown as { ocupacion_efectiva?: number }).ocupacion_efectiva
    ?? 0.8;
  const maxCap = f.capacity_model?.max_clients_day
    ?? Math.round(eb.clientes_dia / Math.max(0.1, ocupacionEfectiva));
  const capacidadData = [
    { name: "Demanda estimada", value: Math.round(eb.clientes_dia), fill: C.green  },
    { name: "Capacidad máxima", value: maxCap,                      fill: C.indigo },
  ];
  return (
    <section className={styles.section}>
      <SectionTitle title="Capacidad vs demanda" accent={C.indigo} info="capacidad" />
      <div style={{ height: 180 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={capacidadData} margin={{ top: 8, right: 24, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis dataKey="name" tick={{ fill: C.muted, fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: C.subtle, fontSize: 10 }} axisLine={false} tickLine={false} width={34}
              label={{ value: "clientes/día", angle: -90, position: "insideLeft", fill: C.subtle, fontSize: 10, offset: 10 }} />
            <ReTooltip contentStyle={{ background: "#111827", border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }}
              formatter={(v: number) => [`${Math.round(v)} clientes/día`, ""]} />
            <Bar dataKey="value" name="Clientes/día" radius={[4, 4, 0, 0]} maxBarSize={60}>
              {capacidadData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.fill} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <ChartExplanation>
        Demanda estimada: <strong>{Math.round(eb.clientes_dia)} clientes/día</strong> en régimen estable.{" "}
        Capacidad máxima: <strong>{maxCap} clientes/día</strong>.{" "}
        Utilizas el <strong>{Math.round(ocupacionEfectiva * 100)}%</strong> de tu capacidad —{" "}
        {ocupacionEfectiva > 0.85
          ? "estás muy cerca del límite, considera ampliar."
          : ocupacionEfectiva > 0.65
            ? "hay margen para crecer sin añadir recursos."
            : "tienes capacidad ociosa significativa."}
      </ChartExplanation>
    </section>
  );
}

// ─── BLOQUE 6: Métricas clave ─────────────────────────────────────────────────

function BloqueMetricas({ m }: { m: MetricasClave }) {
  const colorRoi = (v: number) => v >= 0.40 ? C.green : v >= 0 ? C.yellow : C.red;
  const roiStress = m.roi_stress ?? -1;
  const pbStress  = m.payback_stress ?? 999;

  return (
    <section className={styles.section}>
      <SectionTitle title="Métricas clave" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <StatCard
          label="ROI 3 años (base)"
          value={`${Math.round(m.roi_base * 100)}%`}
          sub={`Cons: ${Math.round(m.roi_conservador * 100)}% · Opt: ${Math.round(m.roi_optimista * 100)}%`}
          color={colorRoi(m.roi_base)}
          tooltip="ROI: Retorno sobre la inversión en 3 años. Indica cuánto ganas respecto al dinero invertido. Por encima del 40% es rentable."
        />
        <StatCard
          label="ROI estrés (×0.40)"
          value={roiStress <= -1 ? "—" : `${Math.round(roiStress * 100)}%`}
          sub="ingresos mínimos, costes fijos intactos"
          color={roiStress >= 0 ? C.green : C.red}
          tooltip="ROI en el peor escenario realista: solo el 40% de los ingresos esperados con todos los costes fijos vigentes."
        />
        <StatCard
          label="Margen bruto"
          value={`${Math.round(m.margen_bruto_pct * 100)}%`}
          sub="ingresos - coste mercancía"
          color={m.margen_bruto_pct >= 0.45 ? C.green : m.margen_bruto_pct >= 0.30 ? C.yellow : C.red}
          tooltip="Margen bruto: porcentaje de beneficio real sobre ingresos antes de descontar costes fijos. Por encima del 45% es saludable."
        />
        <StatCard
          label="Payback"
          value={m.payback_meses >= 999 ? ">36m" : `${m.payback_meses}m`}
          sub={`Estrés: ${pbStress >= 999 ? ">36m" : pbStress + "m"}`}
          color={m.payback_meses <= 18 ? C.green : m.payback_meses <= 30 ? C.yellow : C.red}
          tooltip="Payback: meses hasta recuperar toda la inversión inicial. Menos de 18 meses es excelente; más de 30 meses es arriesgado."
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
          padding: "10px 0",
          fontSize: 12, color: C.subtle,
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
            display: "flex", alignItems: "flex-start", gap: 10, padding: "10px 0",
            fontSize: 12, lineHeight: 1.55,
            borderBottom: `1px solid ${C.border}`,
          }}>
            <span style={{
              fontSize: 9, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: "0.06em", flexShrink: 0, marginTop: 2,
              color: r.tipo === "bloqueo" ? C.red : C.yellow,
            }}>
              {r.tipo === "bloqueo" ? "●" : "·"}
            </span>
            <div>
              <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.06em", color: r.tipo === "bloqueo" ? C.red : C.yellow, marginRight: 6 }}>
                {r.tipo === "bloqueo" ? "Bloqueo" : "Aviso"}
              </span>
              <span style={{ color: C.muted }}>{r.mensaje}</span>
            </div>
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
          const bg     = "transparent";
          const border = C.border;
          return (
            <div key={i} style={{
              padding: "12px 0",
              borderBottom: `1px solid ${C.border}`,
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                <span style={{
                  width: 5, height: 5, borderRadius: "50%", background: color, flexShrink: 0,
                }} />
                <span style={{
                  fontSize: 9, fontWeight: 700, textTransform: "uppercase",
                  letterSpacing: "0.06em", color: C.subtle,
                }}>
                  {isRisk ? "Riesgo" : "Oportunidad"}
                </span>
              </div>
              <p style={{ margin: "0 0 5px", fontSize: 12, color: C.text, fontWeight: 500, lineHeight: 1.5 }}>
                {ins.message}
              </p>
              <p style={{ margin: 0, fontSize: 11, color: C.muted, lineHeight: 1.55 }}>
                {ins.suggestion}
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
  reforma_local:    "Reforma del local",
  equipamiento:     "Equipamiento",
  deposito_fianza:  "Depósito fianza (Art.36 LAU)",
  otros_iniciales:  "Licencias y gestoría",
};

function ParamGroup({ icon, label, children }: {
  color?: string; icon: string; label: string; children: React.ReactNode;
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
          width: 16, display: "flex", alignItems: "center",
          justifyContent: "center", fontSize: 11, fontWeight: 700, flexShrink: 0,
          color: C.subtle,
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
          fontSize: 13, fontWeight: 800, fontVariantNumeric: "tabular-nums",
          color, letterSpacing: "-0.01em",
          lineHeight: 1, minWidth: 60, textAlign: "right",
        }}>
          {Math.round(value).toLocaleString("es-ES")}{unit && ` ${unit.trim()}`}
        </span>
      </div>
      <div style={{ position: "relative", height: 20, display: "flex", alignItems: "center" }}>
        <div style={{
          position: "absolute", left: 0, right: 0, height: 4,
          borderRadius: 99, background: "rgba(255,255,255,0.06)",
        }} />
        <div style={{
          position: "absolute", left: 0, width: `${pct}%`, height: 4,
          borderRadius: 99, background: color, opacity: 0.65, transition: "width 0.05s",
        }} />
        <div style={{
          position: "absolute", left: `calc(${pct}% - 6px)`,
          width: 12, height: 12, borderRadius: "50%", background: color,
          border: "2px solid rgba(13,18,32,0.9)", transition: "left 0.05s",
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
  const [overrides, setOverrides]             = useState<Record<string, number>>({});
  const [businessContext, setBusinessContext] = useState<BusinessContext>({ tipo: "nuevo" });
  const [capitalInput, setCapitalInput]       = useState<string>("");
  const [recalculating, setRecalculating]     = useState(false);

  const refetch = useCallback(async (
    newOverrides: Record<string, number>,
    bc?: BusinessContext,
  ) => {
    setRecalculating(true);
    try {
      const data = await api.financiero(zonaId, sessionId, newOverrides, bc ?? businessContext);
      onUpdate(data);
    } catch (e) {
      console.error("Error recalculando:", e);
    } finally {
      setRecalculating(false);
    }
  }, [zonaId, sessionId, onUpdate, businessContext]);

  const debouncedRefetch = useDebounce(refetch as (...args: unknown[]) => void, 300);

  const handleOverride = (key: string, value: number) => {
    const next = { ...overrides, [key]: value };
    setOverrides(next);
    debouncedRefetch(next);
  };

  const handleBatchOverride = (batch: Record<string, number>) => {
    const next = { ...overrides, ...batch };
    setOverrides(next);
    debouncedRefetch(next);
  };

  const handleBusinessContext = (next: BusinessContext) => {
    setBusinessContext(next);
    refetch(overrides, next);
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
    const capitalVal = Number(capitalInput);
    const capitalOk  = capitalVal > 0;
    const handleSubmit = () => {
      if (!capitalOk) return;
      const bc = { ...businessContext, capital_inicial: capitalVal };
      setBusinessContext(bc);
      refetch(overrides, bc);
    };
    return (
      <div className={styles.capitalGate}>
        <div style={{ fontSize: 13, fontWeight: 700, color: C.text, marginBottom: 4 }}>
          Capital inicial disponible
        </div>
        <div style={{ fontSize: 11, color: C.muted, marginBottom: 16, lineHeight: 1.55 }}>
          Indica el capital que tienes disponible para abrir el negocio. Es necesario para calcular el análisis.
        </div>

        <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
          {(["nuevo", "traspaso"] as const).map(t => (
            <button key={t} onClick={() => setBusinessContext(prev => ({ ...prev, tipo: t }))} style={{
              all: "unset", cursor: "pointer", fontSize: 11, fontWeight: 600,
              padding: "4px 14px", borderRadius: 6, textTransform: "capitalize",
              background: businessContext.tipo === t ? "rgba(16,185,129,0.15)" : C.surface,
              border: `1px solid ${businessContext.tipo === t ? "rgba(16,185,129,0.4)" : C.border}`,
              color: businessContext.tipo === t ? C.green : C.muted,
              transition: "all 0.15s",
            }}>{t}</button>
          ))}
        </div>

        <div style={{ position: "relative", marginBottom: 12 }}>
          <input
            type="number" min={1000} placeholder="ej. 30000"
            value={capitalInput}
            onChange={e => setCapitalInput(e.target.value)}
            onKeyDown={e => e.key === "Enter" && handleSubmit()}
            autoFocus
            style={{
              width: "100%", boxSizing: "border-box",
              background: C.surface, border: `1px solid ${capitalInput && !capitalOk ? C.red : C.border}`,
              borderRadius: 8, color: C.text, fontSize: 14, fontWeight: 600,
              padding: "10px 52px 10px 12px",
            }}
          />
          <span style={{
            position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)",
            fontSize: 11, color: C.muted, pointerEvents: "none",
          }}>€</span>
        </div>

        <button
          onClick={handleSubmit}
          disabled={!capitalOk || recalculating}
          style={{
            all: "unset", cursor: capitalOk && !recalculating ? "pointer" : "not-allowed",
            width: "100%", boxSizing: "border-box", textAlign: "center",
            padding: "10px 0", borderRadius: 8, fontWeight: 700, fontSize: 13,
            background: capitalOk ? C.green : C.surface2,
            color: capitalOk ? "#0F1923" : C.subtle,
            opacity: recalculating ? 0.6 : 1,
            transition: "all 0.15s",
          }}
        >
          {recalculating ? "Calculando…" : "Calcular análisis"}
        </button>
      </div>
    );
  }

  const f      = financiero;
  const params = f.parametros ?? {};

  return (
    <div className={styles.container}>

      {/* ── Contexto del negocio (siempre visible) ── */}
      <section className={styles.section} style={{ paddingBottom: 10 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          {(["nuevo", "traspaso"] as const).map(t => (
            <button key={t} onClick={() => handleBusinessContext({ ...businessContext, tipo: t })} style={{
              all: "unset", cursor: "pointer", fontSize: 11, fontWeight: 600,
              padding: "4px 12px", borderRadius: 6, textTransform: "capitalize",
              background: businessContext.tipo === t ? "rgba(16,185,129,0.15)" : C.surface,
              border: `1px solid ${businessContext.tipo === t ? "rgba(16,185,129,0.4)" : C.border}`,
              color: businessContext.tipo === t ? C.green : C.muted,
              transition: "all 0.15s",
            }}>{t}</button>
          ))}
          <div style={{ display: "flex", alignItems: "center", gap: 4, marginLeft: "auto" }}>
            <span style={{ fontSize: 10, color: C.muted }}>Capital:</span>
            <input
              type="number" min={1000}
              value={businessContext.capital_inicial ?? ""}
              onChange={e => {
                const val = e.target.value ? Number(e.target.value) : undefined;
                handleBusinessContext({ ...businessContext, capital_inicial: val });
              }}
              style={{
                background: C.surface, border: `1px solid ${C.border}`, borderRadius: 6,
                color: C.text, fontSize: 12, padding: "3px 6px", width: 90, textAlign: "right",
              }}
            />
            <span style={{ fontSize: 10, color: C.muted }}>€</span>
          </div>
        </div>
      </section>

      {/* ── BLOQUE 1: Decisión rápida ── */}
      {f.decision && <BloqueDecision d={f.decision} viabilityScore={computeViabilityScore(f)} />}

      {/* ── Avisos del modelo ── */}
      <BloqueValidationFlags flags={f.validation_flags ?? []} />

      {/* ── Correcciones automáticas ── */}
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

      {/* ── Configuración editable (MOVIDA ARRIBA) ── */}
      <BloqueConfigEditable f={f} overrides={overrides} onOverride={handleOverride} onBatchOverride={handleBatchOverride} recalculating={recalculating} />

      {/* ── BLOQUE 2: Economía base ── */}
      <BloqueEconomia f={f} />

      {/* ── BLOQUE 3: Estructura de costes ── */}
      {f.estructura_costes && <BloqueCostes ec={f.estructura_costes} />}

      {/* ── CHART: EBITDA mensual ── */}
      <ChartEbitdaMensual f={f} />

      {/* ── BLOQUE 4: Break-even ── */}
      {f.break_even && <BloqueBreakEven be={f.break_even} alquilerPct={f.alquiler_sobre_ventas_pct} />}

      {/* ── CHART: Caja acumulada ── */}
      <ChartCajaAcumulada f={f} />

      {/* ── Métricas operativas avanzadas ── */}
      <BloqueMetricasAvanzadas f={f} />

      {/* ── CHART: Caja vs EBITDA ── */}
      <ChartCajaVsEbitda f={f} />

      {/* ── Sensibilidad ── */}
      {f.sensibilidad && f.sensibilidad.length > 0 && (
        <BloqueSensibilidad items={f.sensibilidad} />
      )}

      {/* ── CHART: Comparativa anual ── */}
      <ChartComparativa f={f} />

      {/* ── BLOQUE 6: Métricas clave ── */}
      {f.metricas_clave && <BloqueMetricas m={f.metricas_clave} />}

      {/* ── CHART: Distribución costes ── */}
      <ChartCostesDistribucion f={f} />

      {/* ── BLOQUE 7: Riesgos ── */}
      <BloqueRiesgos riesgos={f.riesgos ?? []} />

      {/* ── CHART: Estrés ── */}
      <ChartEstres f={f} />

      {/* ── BLOQUE 8: Insights ── */}
      <BloqueInsights insights={f.insights ?? []} />

      {/* ── CHART: Capacidad vs demanda ── */}
      <ChartCapacidad f={f} />

      {/* ── Desglose inversión inicial ── */}
      {f.desglose_inversion && (
        <section className={styles.section}>
          <div className={styles.sectionHeader}>
            <h3 className={styles.sectionTitle}>Desglose inversión inicial</h3>
            <span className={styles.totalBadge}>{fmt(f.inversion_total)} €</span>
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
    </div>
  );
}
