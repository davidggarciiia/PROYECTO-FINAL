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
  const [successCount, setSuccessCount] = useState<number | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (externalQuery) {
      setInput(externalQuery);
      onQueryUsed?.();
      textareaRef.current?.focus();
    }
  }, [externalQuery, onQueryUsed]);

  const buscar = useCallback(async (descripcion: string) => {
    if (!descripcion.trim()) return;
    setLoading(true);
    setError("");
    setSuccessCount(null);
    try {
      const res = await api.buscar({
        descripcion,
        session_id: sessionId || undefined,
      });

      const zonas = res.zonas ?? [];

      if (res.estado === "ok" && zonas.length > 0) {
        // Mapear ZonaResumen → ZonaPreview
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
        setSuccessCount(zonasPreview.length);
        onResults(zonasPreview, res.session_id);
        setInput("");
      } else if (res.estado === "cuestionario") {
        setError("Describe tu negocio con más detalle: tipo de local, público objetivo y presupuesto aproximado.");
      } else if (res.estado === "error_tipo_negocio") {
        setError(res.motivo || "No puedo identificar el tipo de negocio. Sé más específico.");
      } else if (res.estado === "inviable_legal") {
        setError(res.motivo || "Este tipo de negocio tiene restricciones legales en Barcelona.");
      } else {
        setError("No se encontraron zonas. Intenta con otra descripción.");
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "";
      if (msg.includes("422")) {
        setError("La descripción es demasiado corta. Añade más detalles sobre tu negocio.");
      } else {
        setError("Error al conectar con el servidor.");
      }
    } finally {
      setLoading(false);
    }
  }, [sessionId, onResults]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    buscar(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      buscar(input);
    }
  };

  return (
    <div className={styles.container}>
      {/* Success hint */}
      {successCount !== null && (
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
          {!loading && (
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
            placeholder="Describe tu negocio: tipo, público, estilo..."
            rows={3}
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
