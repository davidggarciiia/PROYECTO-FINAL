"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { api } from "@/lib/api";
import type {
  BuscarResponse,
  FlagsLegales,
  OpcionesCuestionarioResponse,
  OverridesFinancieros,
  PerfilEstructurado,
  SubsectorOpcion,
  ZonaPreview,
} from "@/lib/types";
import styles from "./QuestionnaireForm.module.css";

/* ------------------------------------------------------------------ */
/* Props                                                              */
/* ------------------------------------------------------------------ */

interface Props {
  sessionId: string;
  onResults: (zonas: ZonaPreview[], sessionId: string) => void;
  onError: (msg: string) => void;
  onCuestionarioSenal: (pregunta: string, progreso: number, sessionId: string) => void;
  loading: boolean;
  setLoading: (v: boolean) => void;
}

/* Estado interno tipado con todos los campos del PerfilEstructurado.        */
/* Usamos strings para selects y Record<string,boolean> para multiselects    */
/* con el fin de simplificar el control del DOM.                             */
interface FormState {
  sector: string;
  subsector: string;
  // publico_objetivo
  edad_rango: string;
  nivel_socioeconomico: string;
  estilo_vida: Record<string, boolean>;
  horarios_pico: Record<string, boolean>;
  // operacion
  modelo_servicio: string;
  ticket_tier: number; // 1..5
  escala_operativa: string;
  horarios_apertura: Record<string, boolean>;
  // ubicacion
  densidad_preferida: string;
  tipo_calle: string;
  flujo_tipo: string;
  // filtros
  presupuesto_max: string;
  m2_min: string;
  m2_max: string;
  distritos: Record<string, boolean>;
  // legal / financiero / matices
  flags_legales: Record<string, boolean>;
  ticket_medio: string;
  clientes_dia: string;
  num_empleados: string;
  m2_objetivo: string;
  matices: string;
}

const INITIAL: FormState = {
  sector: "",
  subsector: "",
  edad_rango: "",
  nivel_socioeconomico: "",
  estilo_vida: {},
  horarios_pico: {},
  modelo_servicio: "",
  ticket_tier: 0,
  escala_operativa: "",
  horarios_apertura: {},
  densidad_preferida: "",
  tipo_calle: "",
  flujo_tipo: "",
  presupuesto_max: "",
  m2_min: "",
  m2_max: "",
  distritos: {},
  flags_legales: {},
  ticket_medio: "",
  clientes_dia: "",
  num_empleados: "",
  m2_objetivo: "",
  matices: "",
};

/* ------------------------------------------------------------------ */
/* Helpers                                                            */
/* ------------------------------------------------------------------ */

function checkedKeys(record: Record<string, boolean>): string[] {
  return Object.entries(record)
    .filter(([, v]) => v)
    .map(([k]) => k);
}

function parseNumberOrUndef(s: string): number | undefined {
  const t = s.trim();
  if (!t) return undefined;
  const n = Number(t);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

function pickLiteral<T extends string>(value: string, valid: readonly T[]): T | undefined {
  return (valid as readonly string[]).includes(value) ? (value as T) : undefined;
}

/* ------------------------------------------------------------------ */
/* Componente                                                         */
/* ------------------------------------------------------------------ */

export default function QuestionnaireForm({
  sessionId,
  onResults,
  onError,
  onCuestionarioSenal,
  loading,
  setLoading,
}: Props) {
  const [opciones, setOpciones] = useState<OpcionesCuestionarioResponse | null>(null);
  const [form, setForm] = useState<FormState>(INITIAL);
  const [loadErr, setLoadErr] = useState<string>("");

  // Carga del catálogo al montar
  useEffect(() => {
    let cancelled = false;
    api.opcionesCuestionario()
      .then((o) => { if (!cancelled) setOpciones(o); })
      .catch((e) => { if (!cancelled) setLoadErr(e instanceof Error ? e.message : String(e)); });
    return () => { cancelled = true; };
  }, []);

  const subsectores: SubsectorOpcion[] = useMemo(() => {
    if (!opciones || !form.sector) return [];
    return opciones.subsectores_por_sector[form.sector] || [];
  }, [opciones, form.sector]);

  const flagsDelSector = useMemo(() => {
    if (!opciones || !form.sector) return [];
    return opciones.flags_legales_por_sector[form.sector] || [];
  }, [opciones, form.sector]);

  /* ---------- Construir payload PerfilEstructurado ---------- */
  const buildPayload = useCallback((): PerfilEstructurado => {
    const flagsLegales: FlagsLegales = {};
    for (const codigo of checkedKeys(form.flags_legales)) {
      (flagsLegales as Record<string, boolean>)[codigo] = true;
    }
    const overridesFin: OverridesFinancieros = {};
    const ticket = parseNumberOrUndef(form.ticket_medio);
    const clientes = parseNumberOrUndef(form.clientes_dia);
    const empleados = parseNumberOrUndef(form.num_empleados);
    const m2Obj = parseNumberOrUndef(form.m2_objetivo);
    if (ticket) overridesFin.ticket_medio = ticket;
    if (clientes) overridesFin.clientes_dia = clientes;
    if (empleados) overridesFin.num_empleados = Math.round(empleados);
    if (m2Obj) overridesFin.m2_objetivo = m2Obj;

    const payload: PerfilEstructurado = {
      sector: form.sector,
      subsector: form.subsector || undefined,
      publico_objetivo: {
        edad_rango: form.edad_rango || undefined,
        nivel_socioeconomico: pickLiteral(
          form.nivel_socioeconomico,
          ["bajo", "medio", "medio-alto", "alto"] as const,
        ),
        estilo_vida: checkedKeys(form.estilo_vida),
        horarios_pico: checkedKeys(form.horarios_pico),
      },
      operacion: {
        modelo_servicio: pickLiteral(
          form.modelo_servicio,
          ["take_away", "mesas", "mixto", "delivery_only"] as const,
        ),
        ticket_tier_p1_p5: form.ticket_tier > 0 ? form.ticket_tier : undefined,
        escala_operativa: pickLiteral(
          form.escala_operativa,
          ["solo", "micro", "pequeña", "mediana"] as const,
        ),
        horarios_apertura: checkedKeys(form.horarios_apertura),
      },
      ubicacion_ideal: {
        densidad_preferida: pickLiteral(
          form.densidad_preferida,
          ["baja", "media", "alta"] as const,
        ),
        tipo_calle: form.tipo_calle || undefined,
        flujo_tipo: form.flujo_tipo || undefined,
      },
      presupuesto_max: parseNumberOrUndef(form.presupuesto_max),
      m2_min: parseNumberOrUndef(form.m2_min),
      m2_max: parseNumberOrUndef(form.m2_max),
      distritos: checkedKeys(form.distritos),
      flags_legales: Object.keys(flagsLegales).length ? flagsLegales : undefined,
      overrides_financieros:
        Object.keys(overridesFin).length ? overridesFin : undefined,
      matices: form.matices.trim() || undefined,
    };
    return payload;
  }, [form]);

  /* ---------- Submit ---------- */
  const handleSubmit = useCallback(async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!form.sector) {
      onError("Elige un sector para continuar.");
      return;
    }
    setLoading(true);
    try {
      const res: BuscarResponse = await api.buscar({
        descripcion: "",
        session_id: sessionId || undefined,
        perfil_estructurado: buildPayload(),
      });
      const zonas = res.zonas ?? [];
      if (res.estado === "ok" && zonas.length > 0) {
        const zonasPreview: ZonaPreview[] = zonas.map((z) => ({
          zona_id: z.zona_id,
          nombre: z.nombre,
          barrio: z.barrio,
          distrito: z.distrito,
          lat: z.lat,
          lng: z.lng,
          score_global: z.score_global,
          alquiler_mensual: z.alquiler_estimado,
          m2: z.m2_disponibles,
          color: z.color,
        }));
        onResults(zonasPreview, res.session_id);
      } else if (res.estado === "cuestionario" && res.pregunta) {
        onCuestionarioSenal(res.pregunta, res.progreso_cuestionario ?? 0, res.session_id);
      } else if (res.estado === "error_tipo_negocio" || res.estado === "inviable_legal") {
        onError(res.motivo || "No se puede continuar con esta configuración.");
      } else {
        onError("No se encontraron zonas con esos criterios. Prueba a ampliar filtros.");
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      onError(msg.includes("422")
        ? "Datos del cuestionario incompletos. Revisa el sector."
        : "Error al conectar con el servidor.");
    } finally {
      setLoading(false);
    }
  }, [form.sector, sessionId, buildPayload, onResults, onError, onCuestionarioSenal, setLoading]);

  /* ---------- UI helpers ---------- */
  const toggleMulti = (key: keyof FormState, codigo: string) => {
    setForm((f) => {
      const prev = f[key] as Record<string, boolean>;
      return { ...f, [key]: { ...prev, [codigo]: !prev[codigo] } };
    });
  };

  const canSubmit = !!form.sector && !loading;

  if (loadErr) {
    return <div className={styles.loadErr}>No se pudieron cargar las opciones: {loadErr}</div>;
  }

  if (!opciones) {
    return <div className={styles.loading}>Cargando opciones…</div>;
  }

  return (
    <form onSubmit={handleSubmit} className={styles.form}>
      {/* ── 1. Tipo de negocio (obligatorio) ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>1 · Tipo de negocio <span className={styles.req}>*</span></legend>
        <label className={styles.label}>Sector</label>
        <select
          className={styles.select}
          value={form.sector}
          onChange={(e) => setForm((f) => ({ ...f, sector: e.target.value, subsector: "" }))}
          required
        >
          <option value="">— Elige un sector —</option>
          {opciones.sectores.map((s) => (
            <option key={s.codigo} value={s.codigo}>{s.label}</option>
          ))}
        </select>
        {subsectores.length > 0 && (
          <>
            <label className={styles.label}>Tipo específico (opcional)</label>
            <select
              className={styles.select}
              value={form.subsector}
              onChange={(e) => setForm((f) => ({ ...f, subsector: e.target.value }))}
            >
              <option value="">— Ninguno —</option>
              {subsectores.map((s) => (
                <option key={s.codigo} value={s.codigo}>{s.label}</option>
              ))}
            </select>
          </>
        )}
      </fieldset>

      {/* ── 2. Público objetivo ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>2 · Público objetivo</legend>
        <label className={styles.label}>Nivel socioeconómico</label>
        <div className={styles.chipRow}>
          {opciones.niveles_socioeconomicos.map((n) => (
            <button
              key={n.codigo}
              type="button"
              className={`${styles.chip} ${form.nivel_socioeconomico === n.codigo ? styles.chipOn : ""}`}
              onClick={() => setForm((f) => ({
                ...f,
                nivel_socioeconomico: f.nivel_socioeconomico === n.codigo ? "" : n.codigo,
              }))}
            >
              {n.label}
            </button>
          ))}
        </div>
        <label className={styles.label}>Rango de edad</label>
        <input
          className={styles.input}
          type="text"
          placeholder="ej. 25-40"
          value={form.edad_rango}
          onChange={(e) => setForm((f) => ({ ...f, edad_rango: e.target.value }))}
        />
        <label className={styles.label}>Estilo de vida (puedes marcar varios)</label>
        <div className={styles.chipRow}>
          {opciones.estilos_vida.map((e) => (
            <button
              key={e.codigo}
              type="button"
              className={`${styles.chip} ${form.estilo_vida[e.codigo] ? styles.chipOn : ""}`}
              onClick={() => toggleMulti("estilo_vida", e.codigo)}
            >
              {e.label}
            </button>
          ))}
        </div>
        <label className={styles.label}>Horarios pico</label>
        <div className={styles.chipRow}>
          {opciones.horarios_pico.map((h) => (
            <button
              key={h.codigo}
              type="button"
              className={`${styles.chip} ${form.horarios_pico[h.codigo] ? styles.chipOn : ""}`}
              onClick={() => toggleMulti("horarios_pico", h.codigo)}
            >
              {h.label}
            </button>
          ))}
        </div>
      </fieldset>

      {/* ── 3. Operación ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>3 · Operación</legend>
        <label className={styles.label}>Modelo de servicio</label>
        <div className={styles.chipRow}>
          {opciones.modelos_servicio.map((m) => (
            <button
              key={m.codigo}
              type="button"
              className={`${styles.chip} ${form.modelo_servicio === m.codigo ? styles.chipOn : ""}`}
              onClick={() => setForm((f) => ({
                ...f,
                modelo_servicio: f.modelo_servicio === m.codigo ? "" : m.codigo,
              }))}
            >
              {m.label}
            </button>
          ))}
        </div>
        <label className={styles.label}>Ticket medio</label>
        <div className={styles.ticketRow}>
          {[1, 2, 3, 4, 5].map((n) => (
            <button
              key={n}
              type="button"
              className={`${styles.ticketBtn} ${form.ticket_tier === n ? styles.ticketOn : ""}`}
              onClick={() => setForm((f) => ({ ...f, ticket_tier: f.ticket_tier === n ? 0 : n }))}
              title={`Tier ${n} de 5`}
            >
              {"€".repeat(n)}
            </button>
          ))}
        </div>
        <label className={styles.label}>Escala operativa</label>
        <div className={styles.chipRow}>
          {opciones.escalas_operativas.map((e) => (
            <button
              key={e.codigo}
              type="button"
              className={`${styles.chip} ${form.escala_operativa === e.codigo ? styles.chipOn : ""}`}
              onClick={() => setForm((f) => ({
                ...f,
                escala_operativa: f.escala_operativa === e.codigo ? "" : e.codigo,
              }))}
            >
              {e.label}
            </button>
          ))}
        </div>
        <label className={styles.label}>Horarios de apertura</label>
        <div className={styles.chipRow}>
          {opciones.horarios_apertura.map((h) => (
            <button
              key={h.codigo}
              type="button"
              className={`${styles.chip} ${form.horarios_apertura[h.codigo] ? styles.chipOn : ""}`}
              onClick={() => toggleMulti("horarios_apertura", h.codigo)}
            >
              {h.label}
            </button>
          ))}
        </div>
      </fieldset>

      {/* ── 4. Ubicación ideal ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>4 · Ubicación ideal</legend>
        <label className={styles.label}>Densidad de la zona</label>
        <div className={styles.chipRow}>
          {opciones.densidades.map((d) => (
            <button
              key={d.codigo}
              type="button"
              className={`${styles.chip} ${form.densidad_preferida === d.codigo ? styles.chipOn : ""}`}
              onClick={() => setForm((f) => ({
                ...f,
                densidad_preferida: f.densidad_preferida === d.codigo ? "" : d.codigo,
              }))}
            >
              {d.label}
            </button>
          ))}
        </div>
        <label className={styles.label}>Tipo de calle</label>
        <select
          className={styles.select}
          value={form.tipo_calle}
          onChange={(e) => setForm((f) => ({ ...f, tipo_calle: e.target.value }))}
        >
          <option value="">— Sin preferencia —</option>
          {opciones.tipos_calle.map((t) => (
            <option key={t.codigo} value={t.codigo}>{t.label}</option>
          ))}
        </select>
        <label className={styles.label}>Tipo de flujo</label>
        <select
          className={styles.select}
          value={form.flujo_tipo}
          onChange={(e) => setForm((f) => ({ ...f, flujo_tipo: e.target.value }))}
        >
          <option value="">— Sin preferencia —</option>
          {opciones.tipos_flujo.map((t) => (
            <option key={t.codigo} value={t.codigo}>{t.label}</option>
          ))}
        </select>
      </fieldset>

      {/* ── 5. Filtros prácticos ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>5 · Filtros prácticos</legend>
        <label className={styles.label}>Presupuesto máx. alquiler (€/mes)</label>
        <input
          className={styles.input}
          type="number"
          min="0"
          placeholder="ej. 2500"
          value={form.presupuesto_max}
          onChange={(e) => setForm((f) => ({ ...f, presupuesto_max: e.target.value }))}
        />
        <div className={styles.row2}>
          <div>
            <label className={styles.label}>m² mín</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              placeholder="40"
              value={form.m2_min}
              onChange={(e) => setForm((f) => ({ ...f, m2_min: e.target.value }))}
            />
          </div>
          <div>
            <label className={styles.label}>m² máx</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              placeholder="120"
              value={form.m2_max}
              onChange={(e) => setForm((f) => ({ ...f, m2_max: e.target.value }))}
            />
          </div>
        </div>
        <label className={styles.label}>Distritos de interés</label>
        <div className={styles.chipRow}>
          {opciones.distritos_bcn.map((d) => (
            <button
              key={d}
              type="button"
              className={`${styles.chip} ${form.distritos[d] ? styles.chipOn : ""}`}
              onClick={() => toggleMulti("distritos", d)}
            >
              {d}
            </button>
          ))}
        </div>
      </fieldset>

      {/* ── 6. Legal / operativa especial (condicional al sector) ── */}
      {flagsDelSector.length > 0 && (
        <fieldset className={styles.fieldset}>
          <legend className={styles.legend}>6 · Características legales</legend>
          <p className={styles.hint}>
            Ajustaremos los trámites y licencias del roadmap legal según marques.
          </p>
          {flagsDelSector.map((flag) => (
            <label key={flag.codigo} className={styles.checkbox}>
              <input
                type="checkbox"
                checked={!!form.flags_legales[flag.codigo]}
                onChange={() => toggleMulti("flags_legales", flag.codigo)}
              />
              <span>
                <strong>{flag.label}</strong>
                {flag.descripcion && <small>{flag.descripcion}</small>}
              </span>
            </label>
          ))}
        </fieldset>
      )}

      {/* ── 7. Financiero opcional ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>7 · Financiero (opcional)</legend>
        <p className={styles.hint}>
          Si los rellenas, pre-configuran el análisis financiero al abrir una zona.
        </p>
        <div className={styles.row2}>
          <div>
            <label className={styles.label}>Ticket medio (€)</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              step="0.5"
              placeholder="4.5"
              value={form.ticket_medio}
              onChange={(e) => setForm((f) => ({ ...f, ticket_medio: e.target.value }))}
            />
          </div>
          <div>
            <label className={styles.label}>Clientes/día</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              placeholder="80"
              value={form.clientes_dia}
              onChange={(e) => setForm((f) => ({ ...f, clientes_dia: e.target.value }))}
            />
          </div>
        </div>
        <div className={styles.row2}>
          <div>
            <label className={styles.label}>Nº empleados</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              placeholder="3"
              value={form.num_empleados}
              onChange={(e) => setForm((f) => ({ ...f, num_empleados: e.target.value }))}
            />
          </div>
          <div>
            <label className={styles.label}>m² objetivo</label>
            <input
              className={styles.input}
              type="number"
              min="0"
              placeholder="70"
              value={form.m2_objetivo}
              onChange={(e) => setForm((f) => ({ ...f, m2_objetivo: e.target.value }))}
            />
          </div>
        </div>
      </fieldset>

      {/* ── 8. Matices (opcional, libre) ── */}
      <fieldset className={styles.fieldset}>
        <legend className={styles.legend}>8 · Matices (opcional)</legend>
        <p className={styles.hint}>Si hay algo importante que no encaja arriba, escríbelo aquí (máx 300 caracteres).</p>
        <textarea
          className={styles.textarea}
          rows={3}
          maxLength={300}
          placeholder="ej. Cafetería con mesa larga comunitaria y espacio para perros…"
          value={form.matices}
          onChange={(e) => setForm((f) => ({ ...f, matices: e.target.value }))}
        />
      </fieldset>

      <button
        type="submit"
        className={`btn btn-primary ${styles.submit}`}
        disabled={!canSubmit}
      >
        {loading ? <><span className="spinner" /> Analizando…</> : "Buscar zonas →"}
      </button>
    </form>
  );
}
