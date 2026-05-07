"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { NarrativaDimension } from "@/lib/types";
import { api } from "@/lib/api";
import styles from "./NarrativaPanel.module.css";

interface Props {
  /** Clave de la dimensión: flujo_peatonal | demografia | ... (sin competencia). */
  dimKey: string;
  /** Nombre legible de la dimensión, para los textos del CTA. */
  dimNombre: string;
  /** ID de zona del dossier actual. */
  zonaId: string;
  /** Sesión activa, requerida por el endpoint. */
  sessionId: string;
}

type Estado =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "success"; data: NarrativaDimension }
  | { kind: "error"; mensaje: string };

/**
 * NarrativaPanel — apartado superior de cada DimSection.
 *
 * Muestra un CTA "¿No lo entiendes? Interpretar con IA" en estado idle.
 * Al pulsar, llama a POST /api/dimension/{dimKey}/{zonaId}/narrativa y
 * sustituye el CTA por la lectura interpretativa LLM (~50-60 palabras)
 * + 3 decisiones prácticas reescritas con los números reales de la zona.
 *
 * Cache: el backend cachea 30 días por (zona, dim, sector, perfil_hash).
 * Re-clicks instantáneos. Reinicia su estado si cambia zonaId o dimKey.
 */
export default function NarrativaPanel({
  dimKey,
  dimNombre,
  zonaId,
  sessionId,
}: Props) {
  const [estado, setEstado] = useState<Estado>({ kind: "idle" });
  const abortRef = useRef<AbortController | null>(null);

  // Reset al cambiar zona o dimensión
  useEffect(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setEstado({ kind: "idle" });
  }, [zonaId, dimKey]);

  // Cleanup al desmontar
  useEffect(() => () => {
    abortRef.current?.abort();
  }, []);

  const interpretar = useCallback(async () => {
    if (!sessionId || !zonaId) return;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setEstado({ kind: "loading" });

    try {
      const data = await api.dimensionNarrativa(dimKey, zonaId, sessionId, {
        signal: controller.signal,
      });
      if (controller.signal.aborted) return;
      setEstado({ kind: "success", data });
    } catch (err) {
      if (controller.signal.aborted) return;
      const mensaje =
        err instanceof Error
          ? err.message.replace(/^API .+? → \d+: /, "")
          : "Error desconocido";
      setEstado({ kind: "error", mensaje });
    } finally {
      if (abortRef.current === controller) abortRef.current = null;
    }
  }, [dimKey, zonaId, sessionId]);

  if (estado.kind === "idle") {
    return (
      <div className={styles.panel}>
        <div className={styles.idle}>
          <div className={styles.idleCopy}>
            <span className={styles.idleEyebrow}>¿No lo entiendes?</span>
            <span className={styles.idleTitle}>
              Interpreta {dimNombre.toLowerCase()} con IA
            </span>
            <span className={styles.idleHint}>
              Lectura interpretativa + 3 decisiones prácticas concretadas con los datos de esta zona.
            </span>
          </div>
          <button
            type="button"
            className={styles.btn}
            onClick={interpretar}
            aria-label={`Interpretar ${dimNombre} con IA`}
          >
            <span className={styles.btnIcon} aria-hidden="true">✦</span>
            Interpretar con IA
          </button>
        </div>
      </div>
    );
  }

  if (estado.kind === "loading") {
    return (
      <div className={styles.panel}>
        <div className={styles.loading}>
          <span className={styles.spinner} aria-hidden="true" />
          <span className={styles.loadingText}>
            Interpretando {dimNombre.toLowerCase()} con IA…
          </span>
        </div>
      </div>
    );
  }

  if (estado.kind === "error") {
    return (
      <div className={styles.panel}>
        <div className={styles.error}>
          <span className={styles.errorText}>
            No se pudo generar la interpretación: {estado.mensaje}
          </span>
          <button
            type="button"
            className={styles.retry}
            onClick={interpretar}
            aria-label="Reintentar interpretación"
          >
            Reintentar
          </button>
        </div>
      </div>
    );
  }

  // success
  const { data } = estado;
  const fechaISO = data.generado_at;
  const fechaCorta = (() => {
    try {
      return new Date(fechaISO).toLocaleString("es-ES", {
        day: "2-digit",
        month: "short",
        hour: "2-digit",
        minute: "2-digit",
      });
    } catch {
      return fechaISO;
    }
  })();

  return (
    <div className={styles.panel}>
      <div className={styles.success}>
        <header className={styles.successHead}>
          <span className={styles.successEyebrow}>
            <span className={styles.successDot} aria-hidden="true" />
            Interpretación IA
          </span>
          <span className={styles.successMeta}>
            {data.from_cache ? "cacheado" : "generado"} · {fechaCorta}
          </span>
        </header>

        <p className={styles.lectura}>{data.lectura}</p>

        {data.decisiones.length > 0 && (
          <>
            <div className={styles.decisionesTitle}>Decisiones prácticas</div>
            <ol className={styles.decisiones}>
              {data.decisiones.slice(0, 3).map((d, i) => (
                <li key={`${i}-${d.slice(0, 24)}`} className={styles.decision}>
                  <span className={styles.decisionNum}>
                    {String(i + 1).padStart(2, "0")}
                  </span>
                  <p className={styles.decisionText}>{d}</p>
                </li>
              ))}
            </ol>
          </>
        )}

        <button
          type="button"
          className={styles.regenerate}
          onClick={interpretar}
          aria-label="Regenerar interpretación"
        >
          ↻ Regenerar
        </button>
      </div>
    </div>
  );
}
