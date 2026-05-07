"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  ZonaPreview,
  LocalDetalleResponse,
  CompetenciaDetalle,
  FinancieroResponse,
  ScoresDimensiones,
} from "@/lib/types";
import { api } from "@/lib/api";

import Masthead from "./Masthead";
import Lede from "./Lede";
import TOC from "./TOC";
import Ledger from "./Ledger";
import DimSection from "./DimSection";
import CompetenciaSection from "./CompetenciaSection";
import FinancieroSection from "./FinancieroSection";
import LegalSection from "./LegalSection";
import Colofon from "./Colofon";
import { DIMENSIONS } from "./dimensions";
import styles from "./DossierEditorial.module.css";

interface Props {
  zone: ZonaPreview;
  detalle: LocalDetalleResponse | null;
  loading: boolean;
  onClose: () => void;
  onStop?: () => void;
  sessionId: string;
}

function isAbort(error: unknown) {
  return error instanceof DOMException && error.name === "AbortError";
}

/**
 * DossierEditorial — informe a página completa estilo editorial sobre el
 * mapa. Reemplaza el viejo dossier en modal con tabs.
 *
 * Estructura:
 *   ┌──────────────────────────────────────────────────────────┐
 *   │  Masthead (carrer + chips perfil + ref)                  │
 *   ├────────────┬─────────────────────────────────────────────┤
 *   │  TOC       │  Lede (gauge prob + métricas headline)      │
 *   │  sticky    │  Ledger (resumen 7 dims)                    │
 *   │  ──────    │  § 02 Flujo peatonal                        │
 *   │  · 01      │  § 03 Demografía                            │
 *   │  · 02      │  § 04 Competencia (+ panel grande LLM)      │
 *   │  ...       │  § 05 Transporte                            │
 *   │  · Fin     │  § 06 Seguridad                             │
 *   │  · Legal   │  § 07 Turismo                               │
 *   │            │  § 08 Dinamismo                             │
 *   │            │  § 09 Financiero                            │
 *   │            │  § 10 Legal                                 │
 *   │            │  Colofón                                    │
 *   └────────────┴─────────────────────────────────────────────┘
 */
export default function DossierEditorial({
  zone,
  detalle,
  loading,
  onClose,
  onStop,
  sessionId,
}: Props) {
  const [activeId, setActiveId] = useState<string | null>(null);
  const [competencia, setCompetencia] = useState<CompetenciaDetalle | null>(null);
  const [loadingComp, setLoadingComp] = useState(false);
  const [financiero, setFinanciero] = useState<FinancieroResponse | null>(null);

  const competenciaAbortRef = useRef<AbortController | null>(null);
  const activeZoneRef = useRef(zone.zona_id);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Reset al cambiar de zona — abort y limpiar estado.
  useEffect(() => {
    competenciaAbortRef.current?.abort();
    activeZoneRef.current = zone.zona_id;
    setCompetencia(null);
    setLoadingComp(false);
    setFinanciero(null);
    setActiveId(null);
    // Scroll arriba al cambiar de zona
    if (scrollRef.current) scrollRef.current.scrollTop = 0;
  }, [zone.zona_id]);

  // Limpieza al desmontar
  useEffect(() => () => {
    competenciaAbortRef.current?.abort();
  }, []);

  // Auto-fetch competencia: necesario porque la sección Competencia (§04)
  // se renderiza siempre y muestra el panel grande LLM.
  useEffect(() => {
    if (!sessionId || !zone.zona_id) return;
    const zoneSnapshot = zone.zona_id;
    competenciaAbortRef.current?.abort();
    const controller = new AbortController();
    competenciaAbortRef.current = controller;
    setLoadingComp(true);

    api.competencia(zoneSnapshot, sessionId, { signal: controller.signal })
      .then((data) => {
        if (activeZoneRef.current !== zoneSnapshot || controller.signal.aborted) return;
        setCompetencia(data);
      })
      .catch((e) => {
        if (isAbort(e) || controller.signal.aborted) return;
        console.error("Error competencia:", e);
      })
      .finally(() => {
        if (activeZoneRef.current !== zoneSnapshot) return;
        if (competenciaAbortRef.current === controller) {
          competenciaAbortRef.current = null;
          setLoadingComp(false);
        }
      });
  }, [zone.zona_id, sessionId]);

  // Scrollspy: detecta qué sección está en viewport al hacer scroll y
  // sincroniza el TOC.
  useEffect(() => {
    const root = scrollRef.current;
    if (!root) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const visibles = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => b.intersectionRatio - a.intersectionRatio);
        if (visibles.length > 0) {
          const top = visibles[0].target.id;
          setActiveId(top);
        }
      },
      {
        root,
        rootMargin: "-30% 0px -55% 0px",
        threshold: [0, 0.3, 0.6, 0.9],
      },
    );

    const ids = [
      ...DIMENSIONS.map((d) => `sec-${d.key}`),
      "sec-financiero",
      "sec-legal",
    ];
    const elements: HTMLElement[] = [];
    for (const id of ids) {
      const el = document.getElementById(id);
      if (el) {
        observer.observe(el);
        elements.push(el);
      }
    }

    return () => observer.disconnect();
  }, [zone.zona_id, detalle]);

  const handleStop = useCallback(() => {
    competenciaAbortRef.current?.abort();
    onStop?.();
    if (!onStop) onClose();
  }, [onClose, onStop]);

  const zona = detalle?.zona ?? null;
  const perfil = zona?.perfil_refinado ?? null;
  const sector = zona?.sector_codigo ?? null;
  const subsector = zona?.subsector_codigo ?? null;
  const explicaciones = zona?.explicaciones_dimensiones ?? {};
  const pesosBase = zona?.pesos_dimensiones ?? {};
  const pesosMod = zona?.pesos_modulados ?? {};
  const scoresRaw = zona?.scores_dimensiones ?? {};
  const probabilidad = zona?.probabilidad_supervivencia;
  const scoreGlobal = zona?.score_global ?? zone.score_global;

  // Calibración LLM de Competencia: hasta que llegue /api/competencia, el
  // score_competencia que devolvió /api/local es heurístico. Para no mostrar
  // un score_global ni una row "Competencia" basados en datos no calibrados,
  // los ocultamos en estado "calibrando…" hasta que el LLM relacional resuelva.
  const competenciaListo = competencia != null;

  const scoresEffective: ScoresDimensiones = useMemo(() => {
    if (!competenciaListo) {
      return { ...scoresRaw, competencia: undefined };
    }
    return { ...scoresRaw, competencia: competencia.score_competencia };
    // scoresRaw es un objeto literal nuevo en cada render — no podemos
    // depender de él directamente sin causar loops; basamos la memoización
    // en la referencia estable de `zona` y el objeto `competencia`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [zona, competencia, competenciaListo]);

  const scoreGlobalDisplay = competenciaListo
    ? (competencia?.score_global_calibrated ?? scoreGlobal)
    : null;
  const probabilidadDisplay = competenciaListo ? probabilidad : null;
  const headline =
    zona?.resumen_global_llm ??
    zona?.analisis_ia?.resumen_global ??
    null;
  const numDirectos = (zona as unknown as Record<string, unknown>)?.num_directos as number | undefined;

  // Métricas headline derivadas
  const puntoMuerto = financiero?.estructura_costes
    ? financiero.estructura_costes.alquiler +
      financiero.estructura_costes.personal +
      financiero.estructura_costes.otros
    : null;
  const paybackMeses = financiero?.payback_meses_base ??
    financiero?.payback_meses_conservador ??
    null;

  const tocExtras = useMemo(() => [
    { id: "sec-financiero", label: "Financiero" },
    { id: "sec-legal", label: "Legal" },
  ], []);

  const canStop = loading || loadingComp;

  return (
    <div
      className={styles.overlay}
      role="dialog"
      aria-label="Dossier editorial de la zona"
    >
      <div className={styles.scroll} ref={scrollRef}>
        <Masthead
          zone={zone}
          perfil={perfil}
          sector={sector}
          subsector={subsector}
          onClose={onClose}
        />

        <div className={styles.layout}>
          <div className={styles.tocCol}>
            <TOC
              scores={scoresEffective}
              activeId={activeId}
              onSelect={setActiveId}
              extras={tocExtras}
            />
          </div>

          <main className={styles.main}>
            {loading && !detalle && (
              <div className={styles.loadingFull}>
                <span className={styles.loadingDot} />
                Compilando el informe…
              </div>
            )}

            {!loading && !detalle && (
              <div className={styles.emptyDetail}>
                No hay datos para esta zona.
              </div>
            )}

            {detalle && (
              <>
                <Lede
                  probabilidad={probabilidadDisplay}
                  scoreGlobal={scoreGlobalDisplay}
                  headline={headline}
                  puntoMuerto={puntoMuerto}
                  flujoPotencial={zona?.vcity_flujo_peatonal ?? null}
                  huecoDirecto={numDirectos ?? null}
                  paybackMeses={paybackMeses}
                  competenciaCalibrando={!competenciaListo}
                />

                <Ledger
                  scores={scoresEffective}
                  pesosBase={pesosBase}
                  pesosMod={pesosMod}
                  activeId={activeId}
                  onSelect={setActiveId}
                  loadingKeys={competenciaListo ? undefined : ["competencia"]}
                />

                {DIMENSIONS.map((d, i) => {
                  if (d.key === "competencia") {
                    return (
                      <CompetenciaSection
                        key={d.key}
                        idx={i}
                        zone={zone}
                        sessionId={sessionId}
                        zona={zona}
                        explicacion={explicaciones[d.key]}
                        pesoBase={pesosBase[d.key]}
                        pesoMod={pesosMod[d.key]}
                        competencia={competencia}
                        competenciaLoading={loadingComp}
                      />
                    );
                  }
                  return (
                    <DimSection
                      key={d.key}
                      idx={i}
                      dim={d}
                      zona={zona}
                      explicacion={explicaciones[d.key]}
                      pesoBase={pesosBase[d.key]}
                      pesoMod={pesosMod[d.key]}
                      sessionId={sessionId}
                    />
                  );
                })}

                <FinancieroSection
                  zone={zone}
                  sessionId={sessionId}
                  num={String(DIMENSIONS.length + 2).padStart(2, "0")}
                  onLoad={setFinanciero}
                />

                <LegalSection
                  zone={zone}
                  sessionId={sessionId}
                  num={String(DIMENSIONS.length + 3).padStart(2, "0")}
                />
              </>
            )}
          </main>
        </div>

        {detalle && (
          <Colofon
            zonaId={zone.zona_id}
            modeloVersion={zona?.modelo_version}
          />
        )}
      </div>

      {canStop && (
        <button
          type="button"
          onClick={handleStop}
          aria-label="Detener carga del dossier"
          className={styles.stopBtn}
        >
          Detener
        </button>
      )}
    </div>
  );
}
