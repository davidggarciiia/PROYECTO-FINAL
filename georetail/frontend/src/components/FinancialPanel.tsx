"use client";

import { useState, useCallback, useRef } from "react";
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
          <rect x="4" y="20" width="5" height="8" rx="2" fill="var(--accent)" opacity="0.4"/>
          <rect x="13.5" y="12" width="5" height="16" rx="2" fill="var(--accent)" opacity="0.6"/>
          <rect x="23" y="6" width="5" height="22" rx="2" fill="var(--accent)"/>
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
      optimista:   Math.round(m.acumulado_optimista),
    }));

  // Find break-even month
  const breakEvenMonth = f.proyeccion.find(m => m.acumulado_conservador >= 0)?.mes;

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
                  <stop offset="5%" stopColor="#10B981" stopOpacity={0.25}/>
                  <stop offset="95%" stopColor="#10B981" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="gCons" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#6366F1" stopOpacity={0.25}/>
                  <stop offset="95%" stopColor="#6366F1" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false}/>
              <XAxis dataKey="mes" tick={{ fill: "#475569", fontSize: 10 }} axisLine={false} tickLine={false}/>
              <YAxis tick={{ fill: "#475569", fontSize: 10 }} axisLine={false} tickLine={false} tickFormatter={v => `${Math.round(v/1000)}k`} width={34}/>
              <ReferenceLine y={0} stroke="rgba(255,255,255,0.1)" strokeDasharray="4 4"/>
              <ReTooltip
                contentStyle={{ background: "#0D1220", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 8, fontSize: 12 }}
                labelStyle={{ color: "#94A3B8" }}
                formatter={(v: number) => [`${v.toLocaleString("es-ES")} €`]}
              />
              <Area type="monotone" dataKey="optimista" name="Optimista" stroke="#10B981" fill="url(#gOpt)" strokeWidth={2} dot={false}/>
              <Area type="monotone" dataKey="conservador" name="Conservador" stroke="#6366F1" fill="url(#gCons)" strokeWidth={2} dot={false}/>
            </AreaChart>
          </ResponsiveContainer>
          <div className={styles.chartLegend}>
            <span className={styles.legendItem}><span className={styles.legendDot} style={{background:"#10B981"}}/> Optimista</span>
            <span className={styles.legendItem}><span className={styles.legendDot} style={{background:"#6366F1"}}/> Conservador</span>
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
        <h3 className={styles.sectionTitle}>Ajustar parámetros</h3>
        <div className={styles.sliders}>
          {params.ticket_medio && (
            <SliderParam
              label="Ticket medio"
              unit="€"
              value={overrides.ticket_medio ?? params.ticket_medio.valor_usado}
              min={params.ticket_medio.rango_min}
              max={params.ticket_medio.rango_max}
              fuente={params.ticket_medio.fuente}
              onChange={v => handleOverride("ticket_medio", v)}
            />
          )}
          {params.alquiler_mensual && (
            <SliderParam
              label="Alquiler mensual"
              unit="€"
              value={overrides.alquiler_mensual ?? params.alquiler_mensual.valor_usado}
              min={params.alquiler_mensual.rango_min}
              max={params.alquiler_mensual.rango_max}
              fuente={params.alquiler_mensual.fuente}
              onChange={v => handleOverride("alquiler_mensual", v)}
            />
          )}
          {params.clientes_dia_conservador && (
            <SliderParam
              label="Clientes / día"
              unit=""
              value={overrides.clientes_dia_conservador ?? params.clientes_dia_conservador.valor_usado}
              min={params.clientes_dia_conservador.rango_min}
              max={params.clientes_dia_conservador.rango_max}
              fuente={params.clientes_dia_conservador.fuente}
              onChange={v => handleOverride("clientes_dia_conservador", v)}
            />
          )}
        </div>
      </section>
    </div>
  );
}

interface SliderProps {
  label: string;
  unit: string;
  value: number;
  min: number;
  max: number;
  fuente: string;
  onChange: (v: number) => void;
}

function SliderParam({ label, unit, value, min, max, fuente, onChange }: SliderProps) {
  const pct = ((value - min) / (max - min)) * 100;
  return (
    <div className={styles.slider}>
      <div className={styles.sliderTop}>
        <span className={styles.sliderLabel}>{label}</span>
        <span className={styles.sliderVal}>{Math.round(value).toLocaleString("es-ES")}{unit && ` ${unit}`}</span>
      </div>
      <div className={styles.sliderTrack}>
        <div className={styles.sliderProgress} style={{ width: `${pct}%` }} />
        <input
          type="range"
          min={min} max={max}
          step={Math.max(1, Math.round((max - min) / 100))}
          value={value}
          onChange={e => onChange(Number(e.target.value))}
          className={styles.sliderInput}
        />
      </div>
      <div className={styles.sliderMeta}>
        <span>{min.toLocaleString("es-ES")}</span>
        <span className={styles.sliderFuente}>{fuente}</span>
        <span>{max.toLocaleString("es-ES")}</span>
      </div>
    </div>
  );
}
