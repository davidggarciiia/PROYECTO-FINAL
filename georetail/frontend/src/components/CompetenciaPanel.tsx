"use client";

import { useState, useEffect, useRef } from "react";
import type { CompetenciaDetalle, CompetidorDetalle, ZonaPreview } from "@/lib/types";
import styles from "./CompetenciaPanel.module.css";

interface Props {
  competencia: CompetenciaDetalle | null;
  loading: boolean;
  zona: ZonaPreview;
}

type CompTab = "amenaza" | "oportunidad" | "sinergicos";

function MiniMap({ zona, amenaza, oportunidad, sinergicos }: {
  zona: ZonaPreview;
  amenaza: CompetidorDetalle[];
  oportunidad: CompetidorDetalle[];
  sinergicos: CompetidorDetalle[];
}) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<unknown>(null);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;
    // Dynamic require to avoid SSR issues
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const L = require("leaflet");
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require("leaflet/dist/leaflet.css");

    const map = L.map(mapRef.current, {
      center: [zona.lat, zona.lng],
      zoom: 16,
      zoomControl: false,
      attributionControl: false,
    });
    mapInstanceRef.current = map;

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

    // Zona objetivo — blue pulsing circle
    L.circleMarker([zona.lat, zona.lng], {
      radius: 10,
      fillColor: "#3b82f6",
      color: "#1d4ed8",
      weight: 2,
      opacity: 1,
      fillOpacity: 0.8,
    }).addTo(map).bindTooltip("Tu zona", { permanent: false });

    // NOTE: competitors don't have lat/lng in the response — they only have distancia_m.
    // Show the 500m radius circle around the zona center as a visual indicator of the analysis area.
    L.circle([zona.lat, zona.lng], {
      radius: 500,
      fillColor: "rgba(59,130,246,0.05)",
      color: "rgba(59,130,246,0.3)",
      weight: 1,
      dashArray: "4,4",
    }).addTo(map);

    // Show summary text on the map
    const summary = L.divIcon({
      className: "",
      html: `<div style="background:rgba(15,15,25,0.9);border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:6px 10px;font-size:11px;color:#fff;white-space:nowrap;line-height:1.6">
        <span style="color:#ef4444">●</span> ${amenaza.length} amenazas &nbsp;
        <span style="color:#22c55e">●</span> ${oportunidad.length} oportunidades &nbsp;
        <span style="color:#3b82f6">●</span> ${sinergicos.length} sinérgicos
      </div>`,
      iconAnchor: [0, 0],
    });
    L.marker([zona.lat - 0.003, zona.lng], { icon: summary }).addTo(map);

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, [zona.lat, zona.lng, amenaza.length, oportunidad.length, sinergicos.length]);

  return <div ref={mapRef} className={styles.miniMap} />;
}

function PrecioGap({ ps }: { ps: NonNullable<CompetenciaDetalle["precio_segmento"]> }) {
  const niveles = [1, 2, 3, 4];
  const etiquetas: Record<number, string> = { 1: "€", 2: "€€", 3: "€€€", 4: "€€€€" };
  const total = Object.values(ps.distribucion).reduce((a, b) => a + (b as number), 0) || 1;

  return (
    <div className={styles.precioGap}>
      <div className={styles.precioTitle}>Distribución de precios en zona</div>
      <div className={styles.precioBars}>
        {niveles.map(n => {
          const count = (ps.distribucion[n] as number) || 0;
          const pct = Math.round((count / total) * 100);
          const isGap = ps.tiene_gap && ps.gap_nivel === n;
          const isDominant = ps.nivel_dominante === n;
          return (
            <div key={n} className={`${styles.precioBar} ${isGap ? styles.precioBarGap : ""} ${isDominant ? styles.precioBarDominant : ""}`}>
              <div className={styles.precioBarFill} style={{ height: `${Math.max(pct, 4)}%` }} />
              <span className={styles.precioBarLabel}>{etiquetas[n]}</span>
              <span className={styles.precioBarCount}>{count}</span>
              {isGap && <span className={styles.precioBarGapTag}>gap</span>}
            </div>
          );
        })}
      </div>
      {ps.tiene_gap && (
        <p className={styles.precioGapMsg}>
          Nicho libre detectado en segmento <strong>{ps.gap_etiqueta}</strong> — pocos competidores en este rango de precio.
        </p>
      )}
    </div>
  );
}

function CompetitorCard({ c, showAmenaza = false }: { c: CompetidorDetalle; showAmenaza?: boolean }) {
  const amenazaColor = c.amenaza_score != null
    ? c.amenaza_score > 70 ? "#ef4444"
    : c.amenaza_score > 40 ? "#f59e0b"
    : "#22c55e"
    : undefined;

  return (
    <div className={styles.competitorCard}>
      <div className={styles.competitorHeader}>
        <span className={styles.competitorName}>{c.nombre}</span>
        {c.distancia_m != null && (
          <span className={styles.competitorDist}>{Math.round(c.distancia_m)}m</span>
        )}
      </div>
      <div className={styles.competitorMeta}>
        {c.rating != null && (
          <span className={styles.competitorRating}>★ {c.rating.toFixed(1)}</span>
        )}
        {c.num_resenas != null && (
          <span className={styles.competitorResenas}>{c.num_resenas} reseñas</span>
        )}
        {c.precio_nivel != null && (
          <span className={styles.competitorPrecio}>{"€".repeat(c.precio_nivel)}</span>
        )}
        {c.sector && (
          <span className={styles.competitorSector}>{c.sector}</span>
        )}
      </div>
      {showAmenaza && c.amenaza_score != null && (
        <div className={styles.amenazaBar}>
          <div className={styles.amenazaBarLabel}>Amenaza</div>
          <div className={styles.amenazaBarTrack}>
            <div
              className={styles.amenazaBarFill}
              style={{ width: `${c.amenaza_score}%`, background: amenazaColor }}
            />
          </div>
          <div className={styles.amenazaBarVal} style={{ color: amenazaColor }}>
            {Math.round(c.amenaza_score)}
          </div>
        </div>
      )}
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className={styles.emptyState}>
      <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
        <circle cx="16" cy="16" r="14" stroke="rgba(255,255,255,0.1)" strokeWidth="2"/>
        <path d="M10 16h12M16 10v12" stroke="rgba(255,255,255,0.2)" strokeWidth="2" strokeLinecap="round"/>
      </svg>
      <p>{message}</p>
    </div>
  );
}

function Skeleton({ h = 60 }: { h?: number }) {
  return <div className="skeleton" style={{ height: h, borderRadius: 8, marginBottom: 8 }} />;
}

function ScoreBar({ label, value, color, sublabel }: {
  label: string;
  value: number;
  color: string;
  sublabel?: string;
}) {
  return (
    <div className={styles.rmBar}>
      <div className={styles.rmBarHeader}>
        <span className={styles.rmBarLabel}>{label}</span>
        <span className={styles.rmBarVal} style={{ color }}>{Math.round(value)}</span>
      </div>
      <div className={styles.rmBarTrack}>
        <div
          className={styles.rmBarFill}
          style={{ width: `${Math.min(value, 100)}%`, background: color }}
        />
      </div>
      {sublabel && <span className={styles.rmBarSub}>{sublabel}</span>}
    </div>
  );
}

function ResumenMercado({ data }: { data: CompetenciaDetalle }) {
  // Score global de competencia
  const sc = data.score_competencia;
  const scLabel = sc >= 70 ? "Buena" : sc >= 45 ? "Moderada" : "Difícil";
  const scColor = sc >= 70 ? "var(--green)" : sc >= 45 ? "var(--yellow)" : "var(--red)";

  // Cluster: contexto de aglomeración
  const clusterSub =
    data.score_cluster >= 80 ? "Zona destino" :
    data.score_cluster >= 55 ? "Cluster activo" :
    data.score_cluster >= 30 ? "Poco consolidado" :
    "Sin masa crítica";
  const clusterColor =
    data.score_cluster >= 70 ? "var(--green)" :
    data.score_cluster >= 40 ? "var(--yellow)" : "var(--red)";

  // Amenaza: invertida para visualización (100 = sin amenaza)
  const amenazaInv = Math.max(0, 100 - data.amenaza_incumbentes);
  const amenazaLabel = `Presión: ${data.amenaza_incumbentes < 30 ? "baja" : data.amenaza_incumbentes < 60 ? "media" : "alta"}`;
  const amenazaColor = amenazaInv >= 70 ? "var(--green)" : amenazaInv >= 40 ? "var(--yellow)" : "var(--red)";

  // Oportunidad
  const oportunidadSub =
    data.oportunidad_mercado >= 70 ? "Gap captuable" :
    data.oportunidad_mercado >= 45 ? "Oportunidad moderada" :
    "Mercado cerrado";
  const oportunidadColor =
    data.oportunidad_mercado >= 70 ? "var(--green)" :
    data.oportunidad_mercado >= 45 ? "var(--yellow)" : "var(--red)";

  // HHI badge
  const hhiLabel = data.hhi_index < 0.15 ? "Atomizado" : data.hhi_index < 0.40 ? "Moderado" : "Concentrado";
  const hhiClass = data.hhi_index < 0.15 ? styles.hhiBadgeGreen : data.hhi_index < 0.40 ? styles.hhiBadgeYellow : styles.hhiBadgeRed;

  return (
    <div className={styles.resumenMercado}>
      {/* Header row: score global + HHI badge */}
      <div className={styles.rmHeader}>
        <div className={styles.rmTitleRow}>
          <span className={styles.rmTitle}>Resumen de mercado</span>
          <span className={`${styles.hhiBadge} ${hhiClass}`}>{hhiLabel}</span>
        </div>
        <div className={styles.rmScoreGlobal}>
          <span className={styles.rmScoreNum} style={{ color: scColor }}>{Math.round(sc)}</span>
          <div className={styles.rmScoreInfo}>
            <span className={styles.rmScoreLabel} style={{ color: scColor }}>{scLabel}</span>
            <span className={styles.rmScoreSub}>Score competencia</span>
          </div>
        </div>
      </div>

      {/* 4 score bars */}
      <div className={styles.rmBars}>
        <ScoreBar
          label="Aglomeración"
          value={data.score_cluster}
          color={clusterColor}
          sublabel={clusterSub}
        />
        <ScoreBar
          label="Zona libre"
          value={amenazaInv}
          color={amenazaColor}
          sublabel={amenazaLabel}
        />
        <ScoreBar
          label="Oportunidad"
          value={data.oportunidad_mercado}
          color={oportunidadColor}
          sublabel={oportunidadSub}
        />
        <ScoreBar
          label="Sinergias"
          value={data.score_complementarios}
          color={data.score_complementarios >= 60 ? "var(--green)" : data.score_complementarios >= 35 ? "var(--yellow)" : "var(--red)"}
          sublabel={`${data.sinergicos.length} negocios sinérgicos`}
        />
      </div>
    </div>
  );
}

export default function CompetenciaPanel({ competencia: data, loading, zona }: Props) {
  const [compTab, setCompTab] = useState<CompTab>("amenaza");

  if (loading) {
    return (
      <div className={styles.panel}>
        <Skeleton h={120} />
        <Skeleton h={60} />
        <Skeleton h={60} />
        <Skeleton h={60} />
      </div>
    );
  }

  if (!data) {
    return (
      <div className={styles.panel}>
        <EmptyState message="No hay datos de competencia disponibles." />
      </div>
    );
  }

  const tabCounts: Record<CompTab, number> = {
    amenaza:     data.amenaza.length,
    oportunidad: data.oportunidad.length,
    sinergicos:  data.sinergicos.length,
  };

  const activeList = data[compTab];

  return (
    <div className={styles.panel}>
      {/* Mini map */}
      <MiniMap
        zona={zona}
        amenaza={data.amenaza}
        oportunidad={data.oportunidad}
        sinergicos={data.sinergicos}
      />

      {/* Resumen de mercado */}
      <ResumenMercado data={data} />

      {/* Price gap */}
      {data.precio_segmento && data.precio_segmento.etiqueta !== "Sin datos" && (
        <PrecioGap ps={data.precio_segmento} />
      )}

      {/* Inner tabs */}
      <div className={styles.compTabs}>
        {(["amenaza", "oportunidad", "sinergicos"] as CompTab[]).map(t => (
          <button
            key={t}
            className={`${styles.compTab} ${compTab === t ? styles.compTabActive : ""} ${
              t === "amenaza"     ? styles.compTabAmenaza :
              t === "oportunidad" ? styles.compTabOportunidad :
              styles.compTabSinergicos
            }`}
            onClick={() => setCompTab(t)}
          >
            {t === "amenaza"     && `Amenaza (${tabCounts.amenaza})`}
            {t === "oportunidad" && `Oportunidades (${tabCounts.oportunidad})`}
            {t === "sinergicos"  && `Sinérgicos (${tabCounts.sinergicos})`}
          </button>
        ))}
      </div>

      {/* Tab description */}
      <p className={styles.tabDesc}>
        {compTab === "amenaza"     && "Competidores fuertes: alto rating, muchas reseñas, cerca."}
        {compTab === "oportunidad" && "Competidores débiles: rating bajo o pocas reseñas. Desplazables."}
        {compTab === "sinergicos"  && "Negocios complementarios que atraen tu mismo público objetivo."}
      </p>

      {/* Competitor list */}
      <div className={styles.competitorList}>
        {activeList.length === 0 ? (
          <EmptyState message={
            compTab === "amenaza"     ? "No hay competidores fuertes en 500m. Buena señal." :
            compTab === "oportunidad" ? "No hay competidores vulnerables identificados." :
            "No hay negocios sinérgicos en 500m."
          } />
        ) : (
          activeList.map((c, i) => (
            <CompetitorCard
              key={i}
              c={c}
              showAmenaza={compTab === "amenaza"}
            />
          ))
        )}
      </div>
    </div>
  );
}
