"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { api } from "@/lib/api";
import type { ZonaPreview } from "@/lib/types";
import QuestionnaireForm from "./QuestionnaireForm";
import styles from "./SearchBox.module.css";

type SearchMode = "texto" | "test";

interface Props {
  onResults: (zonas: ZonaPreview[], sessionId: string) => void;
  sessionId: string;
  externalQuery?: string;
  onQueryUsed?: () => void;
  examples?: string[];
  hasResults?: boolean;
  onOpenChange?: (open: boolean) => void;
}

interface CuestionarioState {
  active: boolean;
  pregunta: string;
  progreso: number;
  sessionId: string;
}

export default function SearchBox({
  onResults,
  sessionId,
  externalQuery,
  onQueryUsed,
  examples,
  hasResults,
  onOpenChange,
}: Props) {
  const [open, setOpen]       = useState(false);
  const [mode, setMode]       = useState<SearchMode>("texto");
  const [input, setInput]     = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState("");
  const [cuestionario, setCuestionario] = useState<CuestionarioState | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const openSearchBox = useCallback((value: boolean) => {
    setOpen(value);
    onOpenChange?.(value);
  }, [onOpenChange]);

  useEffect(() => {
    if (externalQuery) {
      setInput(externalQuery);
      openSearchBox(true);
      onQueryUsed?.();
    }
  }, [externalQuery, onQueryUsed, openSearchBox]);

  useEffect(() => {
    if (open) {
      const t = setTimeout(() => textareaRef.current?.focus(), 180);
      return () => clearTimeout(t);
    }
  }, [open]);

  const buscar = useCallback(async (descripcion: string) => {
    if (!descripcion.trim()) return;
    setLoading(true);
    setError("");
    try {
      const res = await api.buscar({
        descripcion,
        session_id: sessionId || undefined,
      });
      const zonas = res.zonas ?? [];

      if (res.estado === "ok" && zonas.length > 0) {
        const zonasPreview = zonas.map(z => ({
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
        setInput("");
        setCuestionario(null);
        openSearchBox(false);
      } else if (res.estado === "cuestionario") {
        setCuestionario({
          active: true,
          pregunta: res.pregunta ?? "¿Puedes darme más detalles sobre tu negocio?",
          progreso: res.progreso_cuestionario ?? 0,
          sessionId: res.session_id,
        });
        setInput("");
      } else if (res.estado === "error_tipo_negocio") {
        setError(res.motivo || "No puedo identificar el tipo de negocio. Sé más específico.");
      } else if (res.estado === "inviable_legal") {
        setError(res.motivo || "Este negocio tiene restricciones legales en Barcelona.");
      } else {
        setError("No se encontraron zonas. Intenta otra descripción.");
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "";
      setError(
        msg.includes("422")
          ? "Descripción demasiado corta. Añade más detalles."
          : "Error al conectar con el servidor."
      );
    } finally {
      setLoading(false);
    }
  }, [sessionId, onResults, openSearchBox]);

  const responderCuestionario = useCallback(async (respuesta: string) => {
    if (!respuesta.trim() || !cuestionario) return;
    setLoading(true);
    setError("");
    try {
      const res = await api.cuestionario({
        session_id: cuestionario.sessionId,
        respuesta,
      });
      if (res.trigger_busqueda) {
        // Cuestionario completo — lanzar búsqueda con la sesión acumulada
        const buscarRes = await api.buscar({
          descripcion: "",
          session_id: cuestionario.sessionId,
        });
        const zonas = buscarRes.zonas ?? [];
        if (buscarRes.estado === "ok" && zonas.length > 0) {
          const zonasPreview = zonas.map(z => ({
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
          onResults(zonasPreview, cuestionario.sessionId);
          setInput("");
          setCuestionario(null);
          openSearchBox(false);
        } else {
          setError("No se encontraron zonas tras el cuestionario. Intenta otra descripción.");
          setCuestionario(null);
        }
      } else {
        // Siguiente pregunta
        setCuestionario(prev => prev ? {
          ...prev,
          pregunta: res.pregunta ?? prev.pregunta,
          progreso: res.progreso_pct,
        } : null);
        setInput("");
      }
    } catch (e) {
      console.error("responderCuestionario error:", e);
      setError("Error procesando la respuesta. Intenta de nuevo.");
    } finally {
      setLoading(false);
    }
  }, [cuestionario, onResults, openSearchBox]);

  const handleSubmit = (e: { preventDefault(): void }) => {
    e.preventDefault();
    if (cuestionario?.active) {
      responderCuestionario(input);
    } else {
      buscar(input);
    }
  };

  const handleKeyDown = (e: { key: string; shiftKey: boolean; preventDefault(): void }) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSubmit({ preventDefault: () => {} }); }
    if (e.key === "Escape") {
      openSearchBox(false);
      setError("");
      setCuestionario(null);
    }
  };

  const cancelarCuestionario = () => {
    setCuestionario(null);
    setInput("");
    setError("");
  };

  return (
    <div className={`${styles.wrapper} ${open ? styles.wrapperOpen : ""}`}>

      {/* ── Floating ball ── */}
      <button
        className={styles.ball}
        onClick={() => { openSearchBox(!open); setError(""); setCuestionario(null); }}
        title={open ? "Cerrar búsqueda" : "Buscar ubicación"}
        aria-label="Buscar"
        aria-expanded={open}
      >
        {loading
          ? <div className="spinner" style={{ width: 20, height: 20 }} />
          : open
            ? <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M3 3l10 10M13 3L3 13" stroke="white" strokeWidth="2.2" strokeLinecap="round"/>
              </svg>
            : <svg width="22" height="22" viewBox="0 0 22 22" fill="none">
                <circle cx="9.5" cy="9.5" r="6" stroke="white" strokeWidth="2"/>
                <path d="M14 14L18 18" stroke="white" strokeWidth="2.2" strokeLinecap="round"/>
              </svg>
        }
      </button>

      {/* ── Expanding panel ── */}
      {open && (
        <div className={styles.panel}>

          {/* Tabs: Texto libre | Cuestionario tipo test */}
          {!cuestionario?.active && (
            <div className={styles.modeTabs} role="tablist" aria-label="Modo de búsqueda">
              <button
                type="button"
                role="tab"
                aria-selected={mode === "texto"}
                className={`${styles.modeTab} ${mode === "texto" ? styles.modeTabActive : ""}`}
                onClick={() => { setMode("texto"); setError(""); }}
              >
                ✍️ Texto libre
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={mode === "test"}
                className={`${styles.modeTab} ${mode === "test" ? styles.modeTabActive : ""}`}
                onClick={() => { setMode("test"); setError(""); }}
              >
                📋 Cuestionario
              </button>
            </div>
          )}

          {/* Cuestionario adaptativo (LLM señal) — solo rama texto */}
          {cuestionario?.active && (
            <div className={styles.cuestionarioHeader}>
              <div className={styles.cuestionarioProgress}>
                <div
                  className={styles.cuestionarioProgressBar}
                  style={{ width: `${cuestionario.progreso}%` }}
                />
              </div>
              <p className={styles.cuestionarioPregunta}>{cuestionario.pregunta}</p>
            </div>
          )}

          {mode === "test" && !cuestionario?.active ? (
            <QuestionnaireForm
              sessionId={sessionId}
              loading={loading}
              setLoading={setLoading}
              onResults={(zonas, sid) => {
                onResults(zonas, sid);
                setInput("");
                setCuestionario(null);
                openSearchBox(false);
              }}
              onError={(msg) => setError(msg)}
              onCuestionarioSenal={(pregunta, progreso, sid) => {
                setCuestionario({ active: true, pregunta, progreso, sessionId: sid });
              }}
            />
          ) : (
            <form onSubmit={handleSubmit} className={styles.form}>
              <textarea
                ref={textareaRef}
                value={input}
                onChange={e => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder={
                  cuestionario?.active
                    ? "Escribe tu respuesta..."
                    : "Describe tu negocio: tipo, público, estilo..."
                }
                rows={3}
                disabled={loading}
                className={styles.textarea}
              />
              <div className={styles.formActions}>
                <button
                  type="submit"
                  disabled={loading || !input.trim()}
                  className={`btn btn-primary ${styles.sendBtn}`}
                >
                  {loading
                    ? <><div className="spinner" /><span>Analizando...</span></>
                    : cuestionario?.active ? "Responder →" : "Buscar ubicación →"
                  }
                </button>
                {cuestionario?.active && (
                  <button
                    type="button"
                    onClick={cancelarCuestionario}
                    className={styles.cancelBtn}
                  >
                    Cancelar
                  </button>
                )}
              </div>
            </form>
          )}

          {error && (
            <div className={styles.error}>
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none" style={{ flexShrink: 0 }}>
                <circle cx="6" cy="6" r="5" stroke="var(--red)" strokeWidth="1.5"/>
                <path d="M6 3.5v3M6 8h.01" stroke="var(--red)" strokeWidth="1.5" strokeLinecap="round"/>
              </svg>
              {error}
            </div>
          )}

          {mode === "texto" && !cuestionario?.active && !hasResults && examples && examples.length > 0 && (
            <div className={styles.examples}>
              <div className={styles.examplesLabel}>Ejemplos</div>
              <div className={styles.exampleChips}>
                {examples.map(ex => (
                  <button
                    key={ex}
                    type="button"
                    className={styles.chip}
                    onClick={() => { setInput(ex); textareaRef.current?.focus(); }}
                  >
                    {ex}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

    </div>
  );
}
