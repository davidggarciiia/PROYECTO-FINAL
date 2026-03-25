"use client";

import React, { useState, useCallback, useRef } from "react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid,
  Tooltip as ReTooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";
import type { FinancieroResponse } from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./FinancialPanel.module.css";

interface Props {
  financiero: FinancieroResponse | null;
  loading: boolean;
  zonaId: string;
  sessionId: string;
  onUpdate: (data: FinancieroResponse) => void;
}

const LABELS_DESGLOSE: Record<string, string> = {
  reforma_local: "Reforma del local",
  equipamiento: "Equipamiento",
  deposito_fianza: "Depósito fianza (Art.36 LAU)",
  otros_iniciales: "Licencias y gestoría",
};

function useDebounce(fn: (...args: unknown[]) => void, delay: number) {
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  return useCallback((...args: unknown[]) => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => fn(...args), delay);
  }, [fn, delay]);
}

function KPI({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className={styles.kpi}>
      <span className={styles.kpiVal} style={color ? { color } : {}}>{value}</span>
      <span className={styles.kpiLabel}>{label}</span>
      {sub && <span className={styles.kpiSub}>{sub}</span>}
    </div>
  );
}

export default function FinancialPanel({ financiero, loading, zonaId, sessionId, onUpdate }: Props) {
  const [overrides, setOverrides] = useState<Record<string, number>>({});

  const refetch = useCallback(async (newOverrides: Record<string, number>) => {
    try {
      const data = await api.financiero(zonaId, sessionId, newOverrides);
      onUpdate(data);
    } catch (e) {
      console.error("Error recalculando:", e);
    }
  }, [zonaId, sessionId, onUpdate]);

  const debouncedRefetch = useDebounce(refetch as (...args: unknown[]) => void, 300);

  const handleOverride = (key: string, value: number) => {
    const next = { ...overrides, [key]: value };
    setOverrides(next);
    debouncedRefetch(next);
  };

  if (loading) {
    return (
      <div className={styles.loadingState}>
        <div className="spinner" />
        <span>Calculando proyección financiera...</span>
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
  const params = f.parametros || {};

  // Chart data — every 3 months
  const chartData = f.proyeccion
    .filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({
      mes: `M${m.mes}`,
      conservador: Math.round(m.acumulado_conservador),
      optimista: Math.round(m.acumulado_optimista),
    }));

  // Find break-even month
  const breakEvenMonth = f.proyeccion.find(m => m.acumulado_conservador >= 0)?.mes;

  console.log("override", overrides)

  return (
    <div className={styles.container}>
      {/* ── KPIs ── */}
      <div className={styles.kpiGrid}>
        <KPI
          label="Inversión inicial"
          value={`${Math.round(f.inversion_total).toLocaleString("es-ES")} €`}
        />
        <KPI
          label="Ingresos/año"
          value={`${Math.round(f.ingresos_anuales_conservador).toLocaleString("es-ES")} €`}
          sub="escenario conservador"
        />
        <KPI
          label="Payback"
          value={`${f.payback_meses_conservador} meses`}
          color={f.payback_meses_conservador <= 18 ? "var(--green)" : f.payback_meses_conservador <= 30 ? "var(--yellow)" : "var(--red)"}
        />
        <KPI
          label="ROI 3 años"
          value={`${Math.round(f.roi_3a_conservador * 100)}%`}
          color={f.roi_3a_conservador >= 0.5 ? "var(--green)" : f.roi_3a_conservador >= 0.2 ? "var(--yellow)" : "var(--red)"}
        />
      </div>

      {/* ── Rent alert ── */}
      {f.alerta_alquiler && (
        <div className={styles.rentAlert}>
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" style={{flexShrink:0}}>
            <path d="M7 1.5l5.5 9.5H1.5L7 1.5z" stroke="var(--yellow)" strokeWidth="1.3"/>
            <path d="M7 5.5v2.5M7 9.5h.01" stroke="var(--yellow)" strokeWidth="1.3" strokeLinecap="round"/>
          </svg>
          El alquiler representa el <strong>{Math.round(f.alquiler_sobre_ventas_pct * 100)}%</strong> de las ventas — por encima del 15% recomendado
        </div>
      )}

      {/* ── Desglose inversión ── */}
      {f.desglose_inversion && (
        <section className={styles.section}>
          <div className={styles.sectionHeader}>
            <h3 className={styles.sectionTitle}>Desglose inversión inicial</h3>
            <span className={styles.totalBadge}>
              {Math.round(f.inversion_total).toLocaleString("es-ES")} €
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
                      <span className={styles.desgloseLabel}>
                        {LABELS_DESGLOSE[key] ?? key}
                      </span>
                      <span className={styles.desgloseValue}>
                        {Math.round(val).toLocaleString("es-ES")} €
                      </span>
                    </div>

                    <div className={styles.desgloseBar}>
                      <div
                        className={styles.desgloseBarFill}
                        style={{ width: `${pct}%` }}
                      />
                    </div>

                    <span className={styles.desglosePct}>
                      {pct.toFixed(0)}%
                    </span>
                  </div>
                );
              })}
          </div>

          {params.num_empleados && (
            <div className={styles.empleadosBox}>
              👥 {params.num_empleados} empleado
              {params.num_empleados > 1 ? "s" : ""}
            </div>
          )}
        </section>
      )}

      {/* ── EBITDA chart ── */}
      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <h3 className={styles.sectionTitle}>Resultado acumulado — 36 meses</h3>
          {breakEvenMonth && (
            <span className={styles.breakEvenTag}>Break-even mes {breakEvenMonth}</span>
          )}
        </div>
        <div className={styles.chartWrap}>
          <ResponsiveContainer width="100%" height={170}>
            <AreaChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="gOpt" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10B981" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#10B981" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="gCons" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366F1" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#6366F1" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
              <XAxis dataKey="mes" tick={{ fill: "#475569", fontSize: 10 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: "#475569", fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={v => `${Math.round(v / 1000)}k`} width={34} />
              <ReferenceLine y={0} stroke="rgba(255,255,255,0.1)" strokeDasharray="4 4" />
              <ReTooltip
                contentStyle={{ background: "#0D1220", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 8, fontSize: 12 }}
                labelStyle={{ color: "#94A3B8" }}
                formatter={(v: number) => [`${v.toLocaleString("es-ES")} €`]}
              />
              <Area type="monotone" dataKey="optimista" name="Optimista" stroke="#10B981" fill="url(#gOpt)" strokeWidth={2} dot={false} />
              <Area type="monotone" dataKey="conservador" name="Conservador" stroke="#6366F1" fill="url(#gCons)" strokeWidth={2} dot={false} />
            </AreaChart>
          </ResponsiveContainer>
          <div className={styles.chartLegend}>
            <span className={styles.legendItem}><span className={styles.legendDot} style={{ background: "#10B981" }} /> Optimista</span>
            <span className={styles.legendItem}><span className={styles.legendDot} style={{ background: "#6366F1" }} /> Conservador</span>
          </div>
        </div>
      </section>

      {/* ── Breakeven ── */}
      <section className={styles.section}>
        <div className={styles.breakevenCard}>
          <div className={styles.breakevenLeft}>
            <span className={styles.breakevenNum}>{f.breakeven_clientes_dia}</span>
            <span className={styles.breakevenLabel}>clientes/día</span>
          </div>
          <div className={styles.breakevenRight}>
            <p>Punto de equilibrio</p>
            <p>Margen bruto: <strong>{Math.round(f.margen_bruto_pct * 100)}%</strong></p>
          </div>
        </div>
      </section>

      {/* ── Sliders ── */}
      <section className={styles.section}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
          <h3 className={styles.sectionTitle} style={{ margin: 0 }}>Ajustar parámetros</h3>
          <span style={{
            fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase",
            color: "#475569", background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.07)",
            borderRadius: 4, padding: "2px 7px",
          }}>interactivo</span>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>

          {/* Ingresos */}
          <ParamGroup color="#10B981" icon="↑" label="Ingresos">
            {params.ticket_medio && (
              <SliderParam color="#10B981"
                label="Ticket medio" unit="€"
                value={overrides.ticket_medio ?? params.ticket_medio.valor_usado}
                min={params.ticket_medio.rango_min} max={params.ticket_medio.rango_max}
                fuente={params.ticket_medio.fuente}
                onChange={v => handleOverride("ticket_medio", v)} />
            )}
            {params.clientes_dia_conservador && (
              <SliderParam color="#10B981"
                label="Clientes / día" unit=""
                value={overrides.clientes_dia_conservador ?? params.clientes_dia_conservador.valor_usado}
                min={params.clientes_dia_conservador.rango_min} max={params.clientes_dia_conservador.rango_max}
                fuente={params.clientes_dia_conservador.fuente}
                onChange={v => handleOverride("clientes_dia_conservador", v)} />
            )}
            {params.clientes_dia_optimista && (
              <SliderParam color="#10B981"
                label="Clientes / día (optimista)" unit=""
                value={overrides.clientes_dia_optimista ?? params.clientes_dia_optimista.valor_usado}
                min={params.clientes_dia_optimista.rango_min} max={params.clientes_dia_optimista.rango_max}
                fuente={params.clientes_dia_optimista.fuente}
                onChange={v => handleOverride("clientes_dia_optimista", v)} />
            )}
            {params.dias_apertura_mes && (
              <SliderParam color="#10B981"
                label="Días apertura / mes" unit=" días"
                value={overrides.dias_apertura_mes ?? params.dias_apertura_mes.valor_usado}
                min={params.dias_apertura_mes.rango_min} max={params.dias_apertura_mes.rango_max}
                fuente={params.dias_apertura_mes.fuente}
                onChange={v => handleOverride("dias_apertura_mes", v)} />
            )}
          </ParamGroup>

          {/* Costes fijos */}
          <ParamGroup color="#F59E0B" icon="≡" label="Costes fijos">
            {params.alquiler_mensual && (
              <SliderParam color="#F59E0B"
                label="Alquiler mensual" unit="€"
                value={overrides.alquiler_mensual ?? params.alquiler_mensual.valor_usado}
                min={params.alquiler_mensual.rango_min} max={params.alquiler_mensual.rango_max}
                fuente={params.alquiler_mensual.fuente}
                onChange={v => handleOverride("alquiler_mensual", v)} />
            )}
            {params.salarios_mensual && (
              <SliderParam color="#F59E0B"
                label="Salarios mensuales" unit="€"
                value={overrides.salarios_mensual ?? params.salarios_mensual.valor_usado}
                min={params.salarios_mensual.rango_min} max={params.salarios_mensual.rango_max}
                fuente={params.salarios_mensual.fuente}
                onChange={v => handleOverride("salarios_mensual", v)} />
            )}
            {params.otros_fijos_mensual && (
              <SliderParam color="#F59E0B"
                label="Otros costes fijos" unit="€"
                value={overrides.otros_fijos_mensual ?? params.otros_fijos_mensual.valor_usado}
                min={params.otros_fijos_mensual.rango_min} max={params.otros_fijos_mensual.rango_max}
                fuente={params.otros_fijos_mensual.fuente}
                onChange={v => handleOverride("otros_fijos_mensual", v)} />
            )}
          </ParamGroup>

          {/* Margen */}
          <ParamGroup color="#6366F1" icon="%" label="Margen">
            {params.coste_mercancia_pct && (
              <SliderParam color="#6366F1"
                label="Coste mercancía" unit="%"
                value={Math.round((overrides.coste_mercancia_pct ?? params.coste_mercancia_pct.valor_usado) * 100)}
                min={Math.round(params.coste_mercancia_pct.rango_min * 100)}
                max={Math.round(params.coste_mercancia_pct.rango_max * 100)}
                fuente={params.coste_mercancia_pct.fuente}
                onChange={v => handleOverride("coste_mercancia_pct", v / 100)} />
            )}
          </ParamGroup>

          {/* Inversión inicial */}
          <ParamGroup color="#A78BFA" icon="⬡" label="Inversión inicial">
            {params.reforma_local && (
              <SliderParam color="#A78BFA"
                label="Reforma" unit=" €"
                value={overrides.reforma_local ?? params.reforma_local.valor_usado}
                min={params.reforma_local.rango_min} max={params.reforma_local.rango_max}
                fuente={params.reforma_local.fuente}
                onChange={v => handleOverride("reforma_local", v)} />
            )}
          </ParamGroup>

        </div>
      </section>
    </div>
  );
}

// ─── ParamGroup ────────────────────────────────────────────────────────────────

interface ParamGroupProps {
  color: string;
  icon: string;
  label: string;
  children: React.ReactNode;
}

function ParamGroup({ color, icon, label, children }: ParamGroupProps) {
  const [open, setOpen] = useState(true);
  return (
    <div style={{
      borderRadius: 10,
      border: "1px solid rgba(255,255,255,0.06)",
      background: "rgba(255,255,255,0.02)",
      overflow: "hidden",
    }}>
      {/* Group header */}
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          all: "unset", cursor: "pointer", display: "flex", alignItems: "center",
          gap: 8, width: "100%", padding: "9px 12px", boxSizing: "border-box",
          borderBottom: open ? "1px solid rgba(255,255,255,0.05)" : "none",
        }}
      >
        <span style={{
          width: 22, height: 22, borderRadius: 6, display: "flex", alignItems: "center",
          justifyContent: "center", fontSize: 11, fontWeight: 700, flexShrink: 0,
          background: `${color}22`, color, border: `1px solid ${color}44`,
        }}>{icon}</span>
        <span style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase", color: "#94A3B8", flex: 1 }}>
          {label}
        </span>
        <svg width="10" height="10" viewBox="0 0 10 10" style={{ color: "#475569", transform: open ? "rotate(180deg)" : "none", transition: "transform 0.2s" }}>
          <path d="M2 3.5l3 3 3-3" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinecap="round" />
        </svg>
      </button>

      {/* Sliders */}
      {open && (
        <div style={{ padding: "10px 12px", display: "flex", flexDirection: "column", gap: 14 }}>
          {children}
        </div>
      )}
    </div>
  );
}

// ─── SliderParam ───────────────────────────────────────────────────────────────

interface SliderProps {
  label: string;
  unit: string;
  value: number;
  min: number;
  max: number;
  fuente: string;
  color: string;
  onChange: (v: number) => void;
}

function SliderParam({ label, unit, value, min, max, fuente, color, onChange }: SliderProps) {
  const pct = Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>

      {/* Label + value badge */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
        <span style={{ fontSize: 12, color: "#94A3B8", fontWeight: 500, lineHeight: 1 }}>{label}</span>
        <span style={{
          fontSize: 12, fontWeight: 700, fontVariantNumeric: "tabular-nums",
          color, background: `${color}18`, border: `1px solid ${color}33`,
          borderRadius: 5, padding: "2px 8px", letterSpacing: "0.02em", lineHeight: 1.6,
          minWidth: 60, textAlign: "right",
        }}>
          {Math.round(value).toLocaleString("es-ES")}{unit && `\u00A0${unit.trim()}`}
        </span>
      </div>

      {/* Track */}
      <div style={{ position: "relative", height: 20, display: "flex", alignItems: "center" }}>
        {/* Background rail */}
        <div style={{
          position: "absolute", left: 0, right: 0, height: 4,
          borderRadius: 99, background: "rgba(255,255,255,0.06)",
        }} />
        {/* Filled portion */}
        <div style={{
          position: "absolute", left: 0, width: `${pct}%`, height: 4,
          borderRadius: 99,
          background: `linear-gradient(90deg, ${color}88, ${color})`,
          boxShadow: `0 0 8px ${color}55`,
          transition: "width 0.05s",
        }} />
        {/* Thumb dot */}
        <div style={{
          position: "absolute", left: `calc(${pct}% - 7px)`,
          width: 14, height: 14, borderRadius: "50%",
          background: color,
          boxShadow: `0 0 0 3px ${color}33, 0 0 10px ${color}88`,
          border: "2px solid rgba(13,18,32,0.8)",
          transition: "left 0.05s",
          pointerEvents: "none",
        }} />
        {/* Native range — invisible but interactive */}
        <input
          type="range"
          min={min} max={max}
          step={Math.max(1, Math.round((max - min) / 100))}
          value={value}
          onChange={e => onChange(Number(e.target.value))}
          style={{
            position: "absolute", inset: 0, width: "100%", height: "100%",
            opacity: 0, cursor: "pointer", margin: 0,
          }}
        />
      </div>

      {/* Min / fuente / max */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ fontSize: 10, color: "#334155", fontVariantNumeric: "tabular-nums" }}>
          {min.toLocaleString("es-ES")}
        </span>
        <span style={{
          fontSize: 9.5, color: "#334155", fontStyle: "italic", letterSpacing: "0.02em",
          maxWidth: "55%", textAlign: "center", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {fuente}
        </span>
        <span style={{ fontSize: 10, color: "#334155", fontVariantNumeric: "tabular-nums" }}>
          {max.toLocaleString("es-ES")}
        </span>
      </div>

    </div>
  );
}