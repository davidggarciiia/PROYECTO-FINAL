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

  useEffect(() => {
    if (!visible) {
      setStepIdx(0);
      return;
    }
    const id = window.setInterval(() => {
      setStepIdx((prev) => (prev + 1) % STEPS.length);
    }, 1400);
    return () => window.clearInterval(id);
  }, [visible]);

  if (!visible) return null;

  return (
    <div className={styles.overlay} role="status" aria-live="polite">
      <div className={styles.card}>
        <div className={styles.spinnerBig} aria-hidden />
        <div className={styles.title}>Analizando Barcelona</div>
        <div className={styles.stepWrap} key={stepIdx}>
          <span className={styles.stepFade}>{STEPS[stepIdx]}</span>
        </div>
        <div className={styles.progress}>
          <div className={styles.progressFill} />
        </div>
        {query && (
          <p className={styles.query}>
            <em>&ldquo;{query}&rdquo;</em>
          </p>
        )}
      </div>
    </div>
  );
}
