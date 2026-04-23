"use client";

import { useEffect, useState } from "react";
import styles from "./LoadingOverlay.module.css";

interface Props {
  visible: boolean;
  query?: string;
}

const STEPS = [
  "Interpretando tu idea con el LLM…",
  "Clasificando sector y concepto de negocio…",
  "Modulando pesos del modelo para tu idea…",
  "Cargando flujo peatonal (VCity + Popular Times)…",
  "Calculando competencia y saturación…",
  "Cruzando demografía y renta…",
  "Puntuando zonas de Barcelona…",
  "Generando análisis y explicaciones…",
];

export default function LoadingOverlay({ visible, query }: Props) {
  const [stepIdx, setStepIdx] = useState(0);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    if (!visible) {
      setStepIdx(0);
      setProgress(0);
      return;
    }
    // Rotación de mensajes y curva asintótica: avanza rápido hasta ~90% y
    // deja el 10% final para cuando llega la respuesta real.
    const stepId = window.setInterval(() => {
      setStepIdx((prev: number) => (prev + 1) % STEPS.length);
    }, 1400);
    const progId = window.setInterval(() => {
      setProgress((prev: number) => {
        if (prev >= 92) return prev;
        const remaining = 92 - prev;
        return Math.min(92, prev + Math.max(0.6, remaining * 0.08));
      });
    }, 180);
    return () => {
      window.clearInterval(stepId);
      window.clearInterval(progId);
    };
  }, [visible]);

  if (!visible) return null;

  const pct = Math.round(progress);

  return (
    <div className={styles.overlay} role="status" aria-live="polite">
      <div className={styles.card}>
        <div className={styles.spinnerBig} aria-hidden />
        <div className={styles.title}>Analizando Barcelona</div>
        <div className={styles.stepWrap} key={stepIdx}>
          <span className={styles.stepFade}>{STEPS[stepIdx]}</span>
        </div>
        <div className={styles.progress} aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
          <div
            className={styles.progressFill}
            style={{ width: `${pct}%` }}
          />
        </div>
        <div className={styles.progressPct}>{pct}%</div>
        {query && (
          <p className={styles.query}>
            <em>&ldquo;{query}&rdquo;</em>
          </p>
        )}
      </div>
    </div>
  );
}
