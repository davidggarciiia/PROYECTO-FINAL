"use client";

import { useState, useCallback, useRef } from "react";
import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip as ReTooltip,
  ResponsiveContainer, Legend
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

export default function FinancialPanel({ financiero, loading, zonaId, sessionId, onUpdate }: Props) {
  const [overrides, setOverrides] = useState<Record<string, number>>({});

  const refetch = useCallback(async (newOverrides: Record<string, number>) => {
    try {
      const data = await api.financiero(zonaId, sessionId, newOverrides);
      onUpdate(data);
    } catch (e) {
      console.error("Error recalculando financiero:", e);
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
      <div className={styles.loading}>
        <div className="spinner" />
        <span>Calculando proyección financiera...</span>
      </div>
    );
  }

  if (!financiero) {
    return <div className={styles.empty}>Cargando análisis financiero...</div>;
  }

  const f = financiero;

  const chartData = f.proyeccion
    .filter((_, i) => i % 3 === 2 || i === 0)
    .map(m => ({
      mes: `M${m.mes}`,
      conservador: Math.round(m.acumulado_conservador),
      optimista: Math.round(m.acumulado_optimista),
    }));

  const params = f.parametros || {};

  return (
    <div className={styles.container}>
      {/* KPIs */}
      <div className={styles.kpis}>
        <div className={styles.kpi}>
          <span className={styles.kpiVal}>{Math.round(f.inversion_total).toLocaleString("es-ES")} €</span>
          <span className={styles.kpiLabel}>Inversión inicial</span>
        </div>
        <div className={styles.kpi}>
          <span className={styles.kpiVal}>{Math.round(f.ingresos_anuales_conservador).toLocaleString("es-ES")} €</span>
          <span className={styles.kpiLabel}>Ingresos/año</span>
        </div>
        <div className={styles.kpi}>
          <span className={styles.kpiVal}>{f.payback_meses_conservador}m</span>
          <span className={styles.kpiLabel}>Payback</span>
        </div>
        <div className={styles.kpi}>
          <span className={styles.kpiVal}>{Math.round(f.roi_3a_conservador * 100)}%</span>
          <span className={styles.kpiLabel}>ROI 3 años</span>
        </div>
      </div>

      {/* Alert */}
      {f.alerta_alquiler && (
        <div className={styles.alertaAlquiler}>
          ⚠ El alquiler supera el {Math.round(f.alquiler_sobre_ventas_pct * 100)}% de las ventas — riesgo alto
        </div>
      )}

      {/* Chart */}
      <section>
        <div className={styles.sectionTitle}>Resultado acumulado 36 meses</div>
        <div className={styles.chart}>
          <ResponsiveContainer width="100%" height={180}>
            <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
              <defs>
                <linearGradient id="kpColorCons" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#4F96F7" stopOpacity={0.35} />
                  <stop offset="95%" stopColor="#4F96F7" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="kpColorOpt" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10D48E" stopOpacity={0.35} />
                  <stop offset="95%" stopColor="#10D48E" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
              <XAxis dataKey="mes" tick={{ fill: "#7A90B8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#7A90B8", fontSize: 11 }} tickFormatter={v => `${Math.round(v / 1000)}k`} />
              <ReTooltip
                contentStyle={{ background: "#0C1328", border: "1px solid rgba(255,255,255,0.1)", borderRadius: "10px" }}
                labelStyle={{ color: "#EEF2FF" }}
                formatter={(v: number) => [`${v.toLocaleString("es-ES")} €`]}
              />
              <Legend wrapperStyle={{ fontSize: 12, color: "#7A90B8" }} />
              <Area type="monotone" dataKey="optimista" name="Optimista" stroke="#10D48E" fill="url(#kpColorOpt)" strokeWidth={2} />
              <Area type="monotone" dataKey="conservador" name="Conservador" stroke="#4F96F7" fill="url(#kpColorCons)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>

      {/* Sliders */}
      <section>
        <div className={styles.sectionTitle}>Ajustar parámetros</div>
        <div className={styles.sliders}>
          {params.ticket_medio && (
            <SliderParam
              label="Ticket medio (€)"
              value={overrides.ticket_medio ?? params.ticket_medio.valor_usado}
              min={params.ticket_medio.rango_min}
              max={params.ticket_medio.rango_max}
              fuente={params.ticket_medio.fuente}
              onChange={v => handleOverride("ticket_medio", v)}
            />
          )}
          {params.alquiler_mensual && (
            <SliderParam
              label="Alquiler mensual (€)"
              value={overrides.alquiler_mensual ?? params.alquiler_mensual.valor_usado}
              min={params.alquiler_mensual.rango_min}
              max={params.alquiler_mensual.rango_max}
              fuente={params.alquiler_mensual.fuente}
              onChange={v => handleOverride("alquiler_mensual", v)}
            />
          )}
          {params.clientes_dia_conservador && (
            <SliderParam
              label="Clientes/día (conservador)"
              value={overrides.clientes_dia_conservador ?? params.clientes_dia_conservador.valor_usado}
              min={params.clientes_dia_conservador.rango_min}
              max={params.clientes_dia_conservador.rango_max}
              fuente={params.clientes_dia_conservador.fuente}
              onChange={v => handleOverride("clientes_dia_conservador", v)}
            />
          )}
        </div>
      </section>

      {/* Breakeven */}
      <section>
        <div className={styles.sectionTitle}>Punto de equilibrio</div>
        <div className={styles.breakeven}>
          Necesitas <strong>{f.breakeven_clientes_dia} clientes/día</strong> para cubrir costes fijos
        </div>
      </section>
    </div>
  );
}

interface SliderProps {
  label: string;
  value: number;
  min: number;
  max: number;
  fuente: string;
  onChange: (v: number) => void;
}

function SliderParam({ label, value, min, max, fuente, onChange }: SliderProps) {
  return (
    <div className={styles.slider}>
      <div className={styles.sliderHeader}>
        <span>{label}</span>
        <span className={styles.sliderVal}>{Math.round(value).toLocaleString("es-ES")}</span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={Math.max(1, Math.round((max - min) / 100))}
        value={value}
        onChange={e => onChange(Number(e.target.value))}
        className={styles.range}
      />
      <div className={styles.sliderFuente}>Fuente: {fuente}</div>
    </div>
  );
}
