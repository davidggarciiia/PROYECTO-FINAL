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

/**
 * MiniMap — mapa Leaflet minimal y legible:
 *  - Dos buffers (150 m y 500 m) con etiqueta de radio.
 *  - Punto azul pulsante = zona objetivo.
 *  - Un círculo por competidor, color por tipo:
 *      rojo  = amenaza (directa no vulnerable)
 *      verde = oportunidad (directa desplazable)
 *      azul  = sinérgico (complementario a <200 m)
 *  - Click en un competidor abre popup con nombre, rating, distancia y botón
 *    "Ver en Google Maps".
 */
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
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const L = require("leaflet");
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require("leaflet/dist/leaflet.css");

    const map = L.map(mapRef.current, {
      center: [zona.lat, zona.lng],
      zoom: 16,
      zoomControl: true,
      attributionControl: false,
    });
    mapInstanceRef.current = map;

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

    // Buffers 150 / 500 m — sólo dos para no saturar.
    L.circle([zona.lat, zona.lng], {
      radius: 500,
      fillColor: "rgba(59,130,246,0.03)",
      color: "rgba(59,130,246,0.45)",
      weight: 1,
      fillOpacity: 0.03,
      dashArray: "6,6",
    }).addTo(map).bindTooltip("500 m", { permanent: false, direction: "top" });

    L.circle([zona.lat, zona.lng], {
      radius: 150,
      fillColor: "rgba(59,130,246,0.05)",
      color: "rgba(59,130,246,0.7)",
      weight: 1.5,
      fillOpacity: 0.05,
      dashArray: "4,4",
    }).addTo(map).bindTooltip("150 m", { permanent: false, direction: "top" });

    // Zona objetivo.
    L.circleMarker([zona.lat, zona.lng], {
      radius: 10,
      fillColor: "#3b82f6",
      color: "#1d4ed8",
      weight: 2,
      fillOpacity: 0.9,
      className: "kp-zona-target",
    }).addTo(map).bindTooltip("Tu zona", { direction: "top" });

    // Popup reusable para cada competidor.
    const popupHtml = (c: CompetidorDetalle, tipo: string, tipoColor: string) => {
      const q = encodeURIComponent(`${c.nombre} ${zona.lat},${zona.lng}`);
      const gmapsUrl = `https://www.google.com/maps/search/?api=1&query=${q}`;
      const rating = c.rating != null ? `★ ${c.rating.toFixed(1)}` : "—";
      const resenas = c.num_resenas != null ? `${c.num_resenas} reseñas` : "";
      const dist = c.distancia_m != null ? `${Math.round(c.distancia_m)} m` : "";
      const precio = c.precio_nivel != null ? "€".repeat(c.precio_nivel) : "";
      const subsector = c.subsector ?? c.sector ?? "";
      return `
        <div style="font-family:inherit;min-width:180px">
          <div style="font-weight:600;font-size:13px;margin-bottom:4px;line-height:1.25">${c.nombre}</div>
          <div style="font-size:11px;color:${tipoColor};font-weight:600;text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">${tipo} · ${subsector}</div>
          <div style="font-size:12px;color:#444;line-height:1.6">
            ${rating}${resenas ? ` · ${resenas}` : ""}<br>
            ${dist}${precio ? ` · ${precio}` : ""}
          </div>
          <a href="${gmapsUrl}" target="_blank" rel="noopener noreferrer"
             style="display:inline-block;margin-top:8px;font-size:12px;font-weight:600;color:#1d4ed8;text-decoration:none;border-bottom:1px solid currentColor">
            Ver en Google Maps ↗
          </a>
        </div>`;
    };

    const pintar = (
      list: CompetidorDetalle[],
      color: string,
      border: string,
      tipoLabel: string,
      tipoColor: string,
    ) => {
      list.forEach((c) => {
        if (c.lat == null || c.lng == null) return;
        L.circleMarker([c.lat, c.lng], {
          radius: 6,
          fillColor: color,
          color: border,
          weight: 1.5,
          fillOpacity: 0.9,
        })
          .addTo(map)
          .bindPopup(popupHtml(c, tipoLabel, tipoColor));
      });
    };
    pintar(amenaza,     "#ef4444", "#991b1b", "Amenaza",     "#b91c1c");
    pintar(oportunidad, "#22c55e", "#166534", "Oportunidad", "#15803d");
    pintar(sinergicos,  "#3b82f6", "#1e40af", "Sinérgico",   "#1d4ed8");

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, [zona.lat, zona.lng, amenaza, oportunidad, sinergicos]);

  return (
    <div className={styles.mapWrapper}>
      <div ref={mapRef} className={styles.miniMap} />
      <div className={styles.mapLegend}>
        <span><span className={styles.legendDot} style={{ background: "#3b82f6" }} /> Tu zona</span>
        <span><span className={styles.legendDot} style={{ background: "#ef4444" }} /> Amenaza</span>
        <span><span className={styles.legendDot} style={{ background: "#22c55e" }} /> Oportunidad</span>
        <span><span className={styles.legendDot} style={{ background: "#3b82f6", border: "1px solid #1e40af" }} /> Sinérgico</span>
      </div>
    </div>
  );
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

function CompetitorCard({ c, showAmenaza = false, zona }: {
  c: CompetidorDetalle;
  showAmenaza?: boolean;
  zona: ZonaPreview;
}) {
  const amenazaColor = c.amenaza_score != null
    ? c.amenaza_score > 70 ? "#ef4444"
    : c.amenaza_score > 40 ? "#f59e0b"
    : "#22c55e"
    : undefined;

  // Google Maps no soporta `center` en la URL de búsqueda, así que embebemos
  // el lat/lng en la propia query para que resuelva al negocio cercano.
  const lat = c.lat ?? zona.lat;
  const lng = c.lng ?? zona.lng;
  const gmapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${c.nombre} ${lat},${lng}`)}`;

  return (
    <a
      href={gmapsUrl}
      target="_blank"
      rel="noopener noreferrer"
      className={styles.competitorCard}
      title="Abrir en Google Maps"
    >
      <div className={styles.competitorHeader}>
        <span className={styles.competitorName}>{c.nombre}</span>
        <span className={styles.competitorHeaderRight}>
          {c.distancia_m != null && (
            <span className={styles.competitorDist}>{Math.round(c.distancia_m)}m</span>
          )}
          <span className={styles.competitorExtLink} aria-hidden>↗</span>
        </span>
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
        {(c.subsector ?? c.sector) && (
          <span className={styles.competitorSector}>{c.subsector ?? c.sector}</span>
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
    </a>
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

  // Densidad competitiva: fusiona cluster + amenaza en una sola lectura.
  // Cluster alto = hay masa crítica de público (bueno). Amenaza alta = los
  // incumbentes son fuertes (malo). Combinamos en "densidad saludable":
  //   densidad = 0.55·cluster + 0.45·(100 - amenaza)
  // Leer: alto = calle comercial viva con hueco para ti;
  //       bajo = o está muerta, o está saturada por competidores fuertes.
  const densidad = Math.round(
    0.55 * data.score_cluster + 0.45 * Math.max(0, 100 - data.amenaza_incumbentes)
  );
  const densidadColor =
    densidad >= 65 ? "var(--green)" : densidad >= 40 ? "var(--yellow)" : "var(--red)";
  // Sublabel contextual: explica el motivo real del score.
  let densidadSub: string;
  if (data.score_cluster < 30) {
    densidadSub = "Poca masa crítica comercial";
  } else if (data.amenaza_incumbentes >= 65) {
    densidadSub = "Hay público pero competidores fuertes";
  } else if (data.score_cluster >= 65 && data.amenaza_incumbentes < 45) {
    densidadSub = "Eje activo con hueco";
  } else if (data.score_cluster >= 45) {
    densidadSub = "Zona comercial moderada";
  } else {
    densidadSub = "Densidad baja";
  }

  // Oportunidad
  const oportunidadSub =
    data.oportunidad_mercado >= 70 ? "Gap captuable" :
    data.oportunidad_mercado >= 45 ? "Oportunidad moderada" :
    "Mercado cerrado";
  const oportunidadColor =
    data.oportunidad_mercado >= 70 ? "var(--green)" :
    data.oportunidad_mercado >= 45 ? "var(--yellow)" : "var(--red)";

  // Sinergias: etiqueta según negocios reales en radio 200 m
  const nSin = data.sinergicos.length;
  const sinergiasSub = nSin === 0
    ? "Sin complementarios a <200 m"
    : `${nSin} negocio${nSin === 1 ? "" : "s"} a <200 m`;

  // HHI badge
  const hhiLabel = data.hhi_index < 0.15 ? "Atomizado" : data.hhi_index < 0.40 ? "Moderado" : "Concentrado";
  const hhiClass = data.hhi_index < 0.15 ? styles.hhiBadgeGreen : data.hhi_index < 0.40 ? styles.hhiBadgeYellow : styles.hhiBadgeRed;

  return (
    <div className={styles.resumenMercado}>
      {/* Header row: score global + HHI badge */}
      <div className={styles.rmHeader}>
        <div className={styles.rmTitleRow}>
          <span className={styles.rmTitle}>Resumen de mercado</span>
          <span className={`${styles.hhiBadge} ${hhiClass}`} title="Concentración de mercado (HHI)">{hhiLabel}</span>
        </div>
        <div className={styles.rmScoreGlobal}>
          <span className={styles.rmScoreNum} style={{ color: scColor }}>{Math.round(sc)}</span>
          <div className={styles.rmScoreInfo}>
            <span className={styles.rmScoreLabel} style={{ color: scColor }}>{scLabel}</span>
            <span className={styles.rmScoreSub}>Score competencia</span>
          </div>
        </div>
      </div>

      {/* 3 score bars: densidad (cluster+amenaza) / oportunidad / sinergias */}
      <div className={styles.rmBars}>
        <ScoreBar
          label="Densidad competitiva"
          value={densidad}
          color={densidadColor}
          sublabel={densidadSub}
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
          sublabel={sinergiasSub}
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
              zona={zona}
              showAmenaza={compTab === "amenaza"}
            />
          ))
        )}
      </div>
    </div>
  );
}
