"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import type { ZonaPreview, FinancieroResponse } from "@/lib/types";
import FinancialPanel from "../FinancialPanel";
import styles from "./FinancieroSection.module.css";

interface Props {
  zone: ZonaPreview;
  sessionId: string;
  /** Índice editorial — § XX. */
  num: string;
  /** Callback opcional para que el padre conozca el payload (para el lede). */
  onLoad?: (data: FinancieroResponse) => void;
}

/**
 * Sección Financiero del editorial. Renderiza el FinancialPanel con un header
 * de sección coherente con las DimSection.
 *
 * El cuestionario NO captura capital_inicial todavía, así que /api/financiero
 * devuelve 422 si lo llamamos en seco. Por eso NO auto-fetchamos: dejamos
 * que el propio FinancialPanel muestre su capitalGate (input "Capital inicial
 * disponible" + botón). Al pulsar "Calcular", el panel dispara su refetch
 * interno con business_context.capital_inicial y el resultado vuelve aquí
 * vía handleUpdate → onLoad para alimentar las métricas del Lede.
 */
export default function FinancieroSection({ zone, sessionId, num, onLoad }: Props) {
  const [financiero, setFinanciero] = useState<FinancieroResponse | null>(null);
  const onLoadRef = useRef(onLoad);

  useEffect(() => {
    onLoadRef.current = onLoad;
  }, [onLoad]);

  // Reset al cambiar de zona — borra el resultado calculado para la zona anterior.
  useEffect(() => {
    setFinanciero(null);
  }, [zone.zona_id]);

  const handleUpdate = useCallback((data: FinancieroResponse) => {
    setFinanciero(data);
    onLoadRef.current?.(data);
  }, []);

  return (
    <section
      id="sec-financiero"
      className={styles.section}
      aria-label="Modelo financiero"
    >
      <header className={styles.head}>
        <span className={styles.kicker}>§ {num}</span>
        <h2 className={styles.title}>
          Modelo <em>financiero</em>
        </h2>
        <p className={styles.lead}>
          Inversión inicial, P&amp;L, ROI y payback derivados de los benchmarks
          sectoriales. Indica el capital disponible para iniciar el cálculo;
          los sliders permiten simular ajustes después.
        </p>
      </header>

      <div className={styles.panelWrap}>
        <FinancialPanel
          financiero={financiero}
          loading={false}
          zonaId={zone.zona_id}
          sessionId={sessionId}
          onUpdate={handleUpdate}
        />
      </div>
    </section>
  );
}
