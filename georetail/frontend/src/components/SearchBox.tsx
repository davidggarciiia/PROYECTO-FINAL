"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import { api } from "@/lib/api";
import type { ZonaPreview } from "@/lib/types";
import styles from "./SearchBox.module.css";

interface Props {
  onResults: (zonas: ZonaPreview[], sessionId: string) => void;
  sessionId: string;
  initialText?: string;
  onExampleClear?: () => void;
}

export default function SearchBox({ onResults, sessionId, initialText, onExampleClear }: Props) {
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pregunta, setPregunta] = useState<string | null>(null);
  const [progreso, setProgreso] = useState(0);
  const [currentSessionId, setCurrentSessionId] = useState(sessionId);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (initialText) {
      setInput(initialText);
      textareaRef.current?.focus();
      onExampleClear?.();
    }
  }, [initialText, onExampleClear]);

  const buscar = useCallback(async (descripcion: string, sid?: string) => {
    if (!descripcion.trim()) return;
    setLoading(true);
    setError("");
    try {
      const res = await api.buscar({
        descripcion,
        session_id: sid || currentSessionId || undefined,
      });
      setCurrentSessionId(res.session_id);
      setProgreso(res.progreso_pct);

      if (res.estado === "cuestionario" && res.pregunta) {
        setPregunta(res.pregunta);
        setInput("");
      } else if (res.estado === "ok" && res.zonas.length > 0) {
        setPregunta(null);
        setProgreso(100);
        onResults(res.zonas, res.session_id);
      } else if (res.estado === "error_tipo_negocio") {
        setError(res.mensaje || "No puedo identificar el tipo de negocio. Sé más específico.");
      } else if (res.estado === "inviable_legal") {
        setError(res.mensaje || "Este negocio tiene restricciones legales en Barcelona.");
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
      setProgreso(res.progreso_pct);
      if (res.trigger_busqueda || res.estado === "ok") {
        setPregunta(null);
        await buscar("", currentSessionId);
      } else if (res.pregunta) {
        setPregunta(res.pregunta);
        setInput("");
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

  return (
    <div className={styles.container}>
      {progreso > 0 && progreso < 100 && (
        <div className={styles.progressBar}>
          <div className={styles.progressFill} style={{ width: `${progreso}%` }} />
        </div>
      )}

      {pregunta && (
        <div className={styles.pregunta}>
          <span className={styles.preguntaLabel}>Pregunta IA</span>
          <p>{pregunta}</p>
        </div>
      )}

      <form onSubmit={handleSubmit} className={styles.form}>
        <div className={`${styles.inputWrap} ${loading ? styles.busy : ""}`}>
          <textarea
            ref={textareaRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={pregunta
              ? "Escribe tu respuesta..."
              : "Ej: restaurante de tapas con terraza en el Eixample..."}
            rows={2}
            disabled={loading}
            className={styles.textarea}
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            className={`btn btn-primary ${styles.sendBtn}`}
          >
            {loading ? <span className="spinner" /> : pregunta ? "Responder" : "Buscar zonas"}
          </button>
        </div>
      </form>

      {error && <div className={styles.error}>{error}</div>}
    </div>
  );
}
