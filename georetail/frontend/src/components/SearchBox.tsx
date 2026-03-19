"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { api } from "@/lib/api";
import type { ZonaPreview } from "@/lib/types";
import styles from "./SearchBox.module.css";

interface Props {
  onResults: (zonas: ZonaPreview[], sessionId: string) => void;
  sessionId: string;
  externalQuery?: string;
  onQueryUsed?: () => void;
}

export default function SearchBox({ onResults, sessionId, externalQuery, onQueryUsed }: Props) {
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pregunta, setPregunta] = useState<string | null>(null);
  const [progreso, setProgreso] = useState(0);
  const [currentSessionId, setCurrentSessionId] = useState(sessionId);
  const [successCount, setSuccessCount] = useState<number | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (externalQuery) {
      setInput(externalQuery);
      onQueryUsed?.();
      textareaRef.current?.focus();
    }
  }, [externalQuery, onQueryUsed]);

  const buscar = useCallback(async (descripcion: string, sid?: string) => {
    if (!descripcion.trim() && !sid) return;
    setLoading(true);
    setError("");
    setSuccessCount(null);
    try {
      const res = await api.buscar({
        descripcion,
        session_id: sid || currentSessionId || undefined,
      });
      setCurrentSessionId(res.session_id);
      setProgreso(res.progreso_pct ?? 0);

      if (res.estado === "cuestionario" && res.pregunta) {
        setPregunta(res.pregunta);
        setInput("");
      } else if (res.estado === "ok" && res.zonas.length > 0) {
        setPregunta(null);
        setSuccessCount(res.zonas.length);
        onResults(res.zonas, res.session_id);
        setInput("");
        setProgreso(0);
      } else if (res.estado === "error_tipo_negocio") {
        setError(res.mensaje || "No puedo identificar el tipo de negocio. Sé más específico.");
      } else if (res.estado === "inviable_legal") {
        setError(res.mensaje || "Este tipo de negocio tiene restricciones legales en Barcelona.");
      } else {
        setError("No se encontraron zonas. Intenta con otra descripción.");
      }
    } catch {
      setError("Error al conectar con el servidor.");
    } finally {
      setLoading(false);
    }
  }, [currentSessionId, onResults]);

  const responderCuestionario = useCallback(async (respuesta: string) => {
    if (!respuesta.trim() || !currentSessionId) return;
    setLoading(true);
    setError("");
    try {
      const res = await api.cuestionario({ session_id: currentSessionId, respuesta });
      setProgreso(res.progreso_pct ?? 0);

      if (res.trigger_busqueda) {
        setPregunta(null);
        await buscar("", currentSessionId);
      } else if (res.pregunta) {
        setPregunta(res.pregunta);
        setInput("");
      } else if (res.estado === "ok") {
        setPregunta(null);
        await buscar("", currentSessionId);
      }
    } catch {
      setError("Error al procesar respuesta.");
    } finally {
      setLoading(false);
    }
  }, [currentSessionId, buscar]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (pregunta) responderCuestionario(input);
    else buscar(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e as unknown as React.FormEvent);
    }
  };

  const progresoVisible = progreso > 0 && progreso < 100;

  return (
    <div className={styles.container}>
      {/* Progress bar */}
      <div className={`${styles.progressTrack} ${progresoVisible ? styles.progressVisible : ""}`}>
        <div
          className={styles.progressFill}
          style={{ width: `${progresoVisible ? progreso : loading ? 40 : 0}%` }}
        />
      </div>

      {/* Cuestionario bubble */}
      {pregunta && (
        <div className={styles.bubble}>
          <div className={styles.bubbleHeader}>
            <div className={styles.bubbleIcon}>
              <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                <circle cx="6" cy="6" r="5" stroke="currentColor" strokeWidth="1.5"/>
                <path d="M6 3.5v3M6 8h.01" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </svg>
            </div>
            <span className={styles.bubbleLabel}>Pregunta</span>
            {progreso > 0 && (
              <span className={styles.progresoLabel}>{progreso}%</span>
            )}
          </div>
          <p className={styles.bubbleText}>{pregunta}</p>
        </div>
      )}

      {/* Success hint */}
      {successCount !== null && !pregunta && (
        <div className={styles.successHint}>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
            <circle cx="6" cy="6" r="5" fill="var(--green)" opacity="0.2"/>
            <path d="M3.5 6l1.8 1.8L8.5 4.5" stroke="var(--green)" strokeWidth="1.5" strokeLinecap="round"/>
          </svg>
          {successCount} zonas encontradas
        </div>
      )}

      {/* Input form */}
      <form onSubmit={handleSubmit} className={styles.form}>
        <div className={styles.inputWrapper}>
          {!pregunta && !loading && (
            <div className={styles.searchIcon}>
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                <circle cx="6" cy="6" r="4.5" stroke="currentColor" strokeWidth="1.5"/>
                <path d="M9.5 9.5L12 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </svg>
            </div>
          )}
          {loading && <div className={`spinner ${styles.loadingIcon}`} />}
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={
              pregunta
                ? "Escribe tu respuesta..."
                : "Describe tu negocio: tipo, público, estilo..."
            }
            rows={pregunta ? 2 : 3}
            disabled={loading}
            className={styles.textarea}
          />
        </div>
        <button
          type="submit"
          disabled={loading || !input.trim()}
          className={`btn btn-primary ${styles.sendBtn}`}
        >
          {loading
            ? <><div className="spinner" /><span>Analizando...</span></>
            : pregunta
            ? "Responder →"
            : "Buscar ubicación →"
          }
        </button>
      </form>

      {/* Error */}
      {error && (
        <div className={styles.error}>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none" style={{flexShrink:0}}>
            <circle cx="6" cy="6" r="5" stroke="var(--red)" strokeWidth="1.5"/>
            <path d="M6 3.5v3M6 8h.01" stroke="var(--red)" strokeWidth="1.5" strokeLinecap="round"/>
          </svg>
          {error}
        </div>
      )}
    </div>
  );
}
