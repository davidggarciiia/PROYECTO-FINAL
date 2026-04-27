"use client";

import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import type { CompetenciaDetalle, CompetidorDetalle, ZonaPreview } from "@/lib/types";
import styles from "./CompetenciaPanel.module.css";

interface Props {
  competencia: CompetenciaDetalle | null;
  loading: boolean;
  zona: ZonaPreview;
}

type CompTab = "amenaza" | "oportunidad" | "sinergicos";
type SortKey = "amenaza" | "distancia" | "rating" | "negatividad";
const MAX_CARDS_PER_TAB = 24;

const TAB_LABELS: Record<CompTab, string> = {
  amenaza: "Amenaza",
  oportunidad: "Oportunidades",
  sinergicos: "Sinergicos",
};

function safeNumber(value: number | null | undefined, fallback = 0) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function clamp(value: number | null | undefined, min = 0, max = 100) {
  return Math.max(min, Math.min(max, safeNumber(value)));
}

function asList<T>(value: T[] | null | undefined): T[] {
  return Array.isArray(value) ? value : [];
}

function pct(value?: number | null) {
  return `${Math.round(value ?? 0)}%`;
}

function scoreTone(score: number) {
  const s = safeNumber(score, 50);
  if (s >= 70) return { label: "Mercado abierto", color: "#1f8a6c", tone: styles.toneGood };
  if (s >= 45) return { label: "Presion media", color: "#a06f20", tone: styles.toneMid };
  return { label: "Mercado duro", color: "#a04d43", tone: styles.toneBad };
}

function negativePct(c: CompetidorDetalle) {
  const r = c.resenas_resumen;
  if (!r?.total) return 0;
  return clamp((r.negativas / r.total) * 100);
}

function sentimentParts(c: CompetidorDetalle) {
  const r = c.resenas_resumen;
  const total = r?.total || 0;
  if (!r || total === 0) return { positivas: 0, neutras: 0, negativas: 0 };
  return {
    positivas: clamp((r.positivas / total) * 100),
    neutras: clamp((r.neutras / total) * 100),
    negativas: clamp((r.negativas / total) * 100),
  };
}

function sortCompetitors(list: CompetidorDetalle[], sortKey: SortKey) {
  return [...list].sort((a, b) => {
    if (sortKey === "distancia") return (a.distancia_m ?? 9999) - (b.distancia_m ?? 9999);
    if (sortKey === "rating") return (b.rating ?? 0) - (a.rating ?? 0);
    if (sortKey === "negatividad") return negativePct(b) - negativePct(a);
    return (b.amenaza_score ?? 0) - (a.amenaza_score ?? 0);
  });
}

function gmapsUrl(c: CompetidorDetalle, zona: ZonaPreview) {
  const lat = c.lat ?? zona.lat;
  const lng = c.lng ?? zona.lng;
  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(`${c.nombre} ${lat},${lng}`)}`;
}

function escapeHtml(value: string) {
  return value.replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#039;",
  }[char] || char));
}

function MiniMap({ zona, amenaza, oportunidad, sinergicos }: {
  zona: ZonaPreview;
  amenaza: CompetidorDetalle[];
  oportunidad: CompetidorDetalle[];
  sinergicos: CompetidorDetalle[];
}) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<unknown>(null);
  const [mapFailed, setMapFailed] = useState(false);

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return;
    let map: { remove: () => void } | null = null;
    try {
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const L = require("leaflet");
      setMapFailed(false);

      map = L.map(mapRef.current, {
        center: [zona.lat, zona.lng],
        zoom: 16,
        zoomControl: true,
        attributionControl: false,
      });
      mapInstanceRef.current = map;

      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

      L.circle([zona.lat, zona.lng], {
        radius: 500,
        fillColor: "rgba(37,99,235,0.04)",
        color: "rgba(37,99,235,0.45)",
        weight: 1,
        fillOpacity: 0.04,
        dashArray: "6,6",
      }).addTo(map);

      L.circle([zona.lat, zona.lng], {
        radius: 150,
        fillColor: "rgba(37,99,235,0.06)",
        color: "rgba(37,99,235,0.7)",
        weight: 1.5,
        fillOpacity: 0.06,
        dashArray: "4,4",
      }).addTo(map);

      L.circleMarker([zona.lat, zona.lng], {
        radius: 9,
        fillColor: "#2563eb",
        color: "#1d4ed8",
        weight: 2,
        fillOpacity: 0.95,
        className: "kp-zona-target",
      }).addTo(map).bindTooltip("Tu zona", { direction: "top" });

      const popupHtml = (c: CompetidorDetalle, tipo: string, tipoColor: string) => {
        const rating = c.rating != null ? `${c.rating.toFixed(1)} rating` : "Sin rating";
        const resumen = c.resenas_resumen;
        const reviews = resumen?.total ? `${resumen.total} resenas analizadas` : `${c.num_resenas ?? 0} resenas`;
        const neg = resumen?.total ? `${Math.round(negativePct(c))}% negativas` : "Sin NLP";
        const tema = resumen?.temas_top?.[0] ? `Tema: ${resumen.temas_top[0]}` : "";
        return `
        <div style="font-family:inherit;min-width:190px">
          <div style="font-weight:700;font-size:13px;margin-bottom:4px;line-height:1.25">${escapeHtml(c.nombre)}</div>
          <div style="font-size:11px;color:${tipoColor};font-weight:700;text-transform:uppercase;margin-bottom:6px">${tipo}</div>
          <div style="font-size:12px;color:#334155;line-height:1.55">
            ${rating} · ${reviews}<br>
            ${c.distancia_m != null ? `${Math.round(c.distancia_m)} m · ` : ""}${neg}<br>
            ${escapeHtml(tema)}
          </div>
          <a href="${gmapsUrl(c, zona)}" target="_blank" rel="noopener noreferrer"
             style="display:inline-block;margin-top:8px;font-size:12px;font-weight:700;color:#1d4ed8;text-decoration:none;border-bottom:1px solid currentColor">
            Abrir en Maps
          </a>
        </div>`;
      };

      const paint = (
        list: CompetidorDetalle[],
        color: string,
        border: string,
        label: string,
        labelColor: string,
      ) => {
        list.forEach((c) => {
          if (c.lat == null || c.lng == null) return;
          const radius = c.resenas_resumen?.total ? 6 + Math.min(5, Math.log10(c.resenas_resumen.total + 1) * 2) : 6;
          L.circleMarker([c.lat, c.lng], {
            radius,
            fillColor: color,
            color: border,
            weight: 1.5,
            fillOpacity: 0.92,
          })
            .addTo(map)
            .bindPopup(popupHtml(c, label, labelColor));
        });
      };

      paint(amenaza, "#d94d44", "#8f2d27", "Amenaza", "#a04d43");
      paint(oportunidad, "#2f9e78", "#17694f", "Oportunidad", "#1f8a6c");
      paint(sinergicos, "#3b82f6", "#1d4ed8", "Sinergico", "#1d4ed8");
    } catch (error) {
      console.error("Error pintando minimapa de competencia:", error);
      setMapFailed(true);
      if (map) map.remove();
      mapInstanceRef.current = null;
    }

    return () => {
      if (map) map.remove();
      mapInstanceRef.current = null;
    };
  }, [zona.lat, zona.lng, amenaza, oportunidad, sinergicos]);

  return (
    <div className={styles.mapWrapper}>
      {mapFailed ? (
        <div className={styles.mapFallback}>
          <strong>Minimapa no disponible</strong>
          <span>La lista de competidores sigue operativa.</span>
        </div>
      ) : (
        <div ref={mapRef} className={styles.miniMap} />
      )}
      <div className={styles.mapLegend}>
        <span><span className={styles.legendDot} style={{ background: "#2563eb" }} /> Tu zona</span>
        <span><span className={styles.legendDot} style={{ background: "#d94d44" }} /> Amenaza</span>
        <span><span className={styles.legendDot} style={{ background: "#2f9e78" }} /> Oportunidad</span>
        <span><span className={styles.legendDot} style={{ background: "#3b82f6" }} /> Sinergico</span>
      </div>
    </div>
  );
}

function MarketHeader({ data }: { data: CompetenciaDetalle }) {
  const score = safeNumber(data.score_competencia, 50);
  const hhi = safeNumber(data.hhi_index);
  const tone = scoreTone(score);
  const cobertura = data.resenas_cobertura;
  const ringStyle = {
    "--score-pct": `${clamp(score)}%`,
    "--score-color": tone.color,
  } as CSSProperties;

  return (
    <header className={styles.marketHeader}>
      <div className={styles.scoreBlock}>
        <div className={styles.scoreRing} style={ringStyle}>
          <span>{Math.round(score)}</span>
        </div>
        <div className={styles.scoreText}>
          <span className={`${styles.scoreLabel} ${tone.tone}`}>{tone.label}</span>
          <strong>Competencia en radio {data.radio_m ?? 500} m</strong>
          <p>
            {data.num_directos ?? 0} directos, {pct(data.pct_vulnerables)} vulnerables y HHI {hhi.toFixed(2)}.
          </p>
        </div>
      </div>

      <div className={styles.headerMetrics}>
        <Metric label="Directos" value={data.num_directos ?? 0} hint="competidores" />
        <Metric label="Vulnerables" value={pct(data.pct_vulnerables)} hint="del mercado directo" />
        <Metric label="HHI" value={hhi.toFixed(2)} hint={hhi < 0.15 ? "atomizado" : hhi < 0.4 ? "medio" : "concentrado"} />
        <Metric
          label="Cobertura"
          value={pct(cobertura?.cobertura_pct)}
          hint={`${cobertura?.resenas_total ?? 0} resenas`}
        />
      </div>
    </header>
  );
}

function Metric({ label, value, hint }: { label: string; value: string | number; hint: string }) {
  return (
    <div className={styles.metric}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{hint}</small>
    </div>
  );
}

function MarketReading({ data }: { data: CompetenciaDetalle }) {
  const scoreCluster = safeNumber(data.score_cluster, 50);
  const amenazaIncumbentes = safeNumber(data.amenaza_incumbentes, 50);
  const opportunity = clamp(data.oportunidad_mercado);
  const synergy = clamp(data.score_complementarios);
  const density = clamp(0.55 * scoreCluster + 0.45 * Math.max(0, 100 - amenazaIncumbentes));

  const message =
    amenazaIncumbentes >= 70
      ? "Hay trafico comercial, pero los incumbentes tienen mucha traccion."
      : safeNumber(data.oportunidad_mercado, 50) >= 70
        ? "La zona deja hueco para una propuesta mejor posicionada."
        : scoreCluster < 35
          ? "La competencia no presiona mucho, aunque falta masa critica alrededor."
          : "El mercado es equilibrado: suficiente actividad con presion controlada.";

  return (
    <section className={styles.reading}>
      <div>
        <span className={styles.sectionLabel}>Lectura de mercado</span>
        <p>{message}</p>
      </div>
      <div className={styles.readingBars}>
        <ScoreBar label="Densidad sana" value={density} />
        <ScoreBar label="Oportunidad" value={opportunity} />
        <ScoreBar label="Sinergias" value={synergy} />
      </div>
    </section>
  );
}

function ScoreBar({ label, value }: { label: string; value: number }) {
  const color = value >= 65 ? "#1f8a6c" : value >= 40 ? "#a06f20" : "#a04d43";
  return (
    <div className={styles.scoreBar}>
      <div className={styles.scoreBarTop}>
        <span>{label}</span>
        <strong style={{ color }}>{Math.round(value)}</strong>
      </div>
      <div className={styles.scoreBarTrack}>
        <div className={styles.scoreBarFill} style={{ width: `${clamp(value)}%`, background: color }} />
      </div>
    </div>
  );
}

function PrecioGap({ ps }: { ps: NonNullable<CompetenciaDetalle["precio_segmento"]> }) {
  const levels = [1, 2, 3, 4];
  const labels: Record<number, string> = { 1: "€", 2: "€€", 3: "€€€", 4: "€€€€" };
  const distribucion = ps.distribucion ?? {};
  const total = Object.values(distribucion).reduce((sum, value) => sum + safeNumber(value as number), 0) || 1;

  return (
    <section className={styles.priceBand}>
      <div className={styles.priceCopy}>
        <span className={styles.sectionLabel}>Precio del segmento</span>
        <strong>{ps.etiqueta || "Sin datos"}</strong>
        <p>
          {ps.tiene_gap && ps.gap_etiqueta
            ? `Hueco visible en ${ps.gap_etiqueta}: pocos rivales cubren ese rango.`
            : "Distribucion de tickets declarados por los competidores visibles."}
        </p>
      </div>
      <div className={styles.priceBars}>
        {levels.map((level) => {
          const count = safeNumber(distribucion[level] as number);
          const height = Math.max(8, Math.round((count / total) * 100));
          const isGap = ps.tiene_gap && ps.gap_nivel === level;
          const isDominant = ps.nivel_dominante === level;
          return (
            <div key={level} className={styles.priceBar}>
              <div className={styles.priceTrack}>
                <div
                  className={`${styles.priceFill} ${isGap ? styles.priceGapFill : ""} ${isDominant ? styles.priceDominantFill : ""}`}
                  style={{ height: `${height}%` }}
                />
              </div>
              <span>{labels[level]}</span>
              <small>{count}</small>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function ReviewStrip({ c }: { c: CompetidorDetalle }) {
  const parts = sentimentParts(c);
  const total = c.resenas_resumen?.total || 0;
  if (!total) {
    return (
      <div className={styles.noReviews}>
        Sin reseñas scrapeadas todavia
      </div>
    );
  }

  return (
    <div className={styles.reviewStrip}>
      <div className={styles.reviewStripTop}>
        <span>{total} reseñas analizadas</span>
        <strong>{Math.round(parts.negativas)}% negativas</strong>
      </div>
      <div className={styles.sentimentTrack} aria-label="Distribucion de sentimiento">
        <span className={styles.sentimentPositive} style={{ width: `${parts.positivas}%` }} />
        <span className={styles.sentimentNeutral} style={{ width: `${parts.neutras}%` }} />
        <span className={styles.sentimentNegative} style={{ width: `${parts.negativas}%` }} />
      </div>
    </div>
  );
}

function CompetitorCard({ c, zona, showThreat }: {
  c: CompetidorDetalle;
  zona: ZonaPreview;
  showThreat: boolean;
}) {
  const amenaza = clamp(c.amenaza_score ?? 0);
  const amenazaColor = amenaza >= 70 ? "#a04d43" : amenaza >= 40 ? "#a06f20" : "#1f8a6c";
  const snippets = c.resenas_destacadas ?? [];
  const topics = c.resenas_resumen?.temas_top ?? [];

  return (
    <article className={styles.competitorCard}>
      <div className={styles.competitorHeader}>
        <div>
          <h4>{c.nombre}</h4>
          <div className={styles.competitorMeta}>
            {c.distancia_m != null && <span>{Math.round(c.distancia_m)} m</span>}
            {c.rating != null && <span>{c.rating.toFixed(1)} rating</span>}
            {c.num_resenas != null && <span>{c.num_resenas} reseñas</span>}
            {c.precio_nivel != null && <span>{"€".repeat(c.precio_nivel)}</span>}
          </div>
        </div>
        <a href={gmapsUrl(c, zona)} target="_blank" rel="noopener noreferrer" className={styles.mapLink}>
          Maps
        </a>
      </div>

      <div className={styles.badges}>
        {(c.subsector ?? c.sector) && <span>{c.subsector ?? c.sector}</span>}
        {c.es_competencia_directa_subsector && <span className={styles.badgeRisk}>mismo subsector</span>}
        {c.es_vulnerable && <span className={styles.badgeOpportunity}>vulnerable</span>}
        {topics.slice(0, 4).map((topic) => <span key={topic}>{topic}</span>)}
      </div>

      {showThreat && c.amenaza_score != null && (
        <div className={styles.threatLine}>
          <span>Amenaza</span>
          <div className={styles.threatTrack}>
            <div className={styles.threatFill} style={{ width: `${amenaza}%`, background: amenazaColor }} />
          </div>
          <strong style={{ color: amenazaColor }}>{Math.round(amenaza)}</strong>
        </div>
      )}

      <ReviewStrip c={c} />

      {snippets.length > 0 && (
        <div className={styles.snippets}>
          {snippets.slice(0, 3).map((review, index) => (
            <blockquote key={`${review.fecha ?? "sin-fecha"}-${index}`}>
              <p>{review.texto}</p>
              <footer>
                {review.rating != null && <span>{review.rating.toFixed(1)} rating</span>}
                {review.sentimiento && <span>{review.sentimiento}</span>}
                {review.categoria && <span>{review.categoria}</span>}
              </footer>
            </blockquote>
          ))}
        </div>
      )}
    </article>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className={styles.emptyState}>
      <strong>Sin datos</strong>
      <p>{message}</p>
    </div>
  );
}

function Skeleton({ h = 60 }: { h?: number }) {
  return <div className="skeleton" style={{ height: h, borderRadius: 8, marginBottom: 8 }} />;
}

export default function CompetenciaPanel({ competencia: data, loading, zona }: Props) {
  const [compTab, setCompTab] = useState<CompTab>("amenaza");
  const [sortKey, setSortKey] = useState<SortKey>("amenaza");

  const activeList = useMemo(() => {
    if (!data) return [];
    return sortCompetitors(asList(data[compTab]), sortKey).slice(0, MAX_CARDS_PER_TAB);
  }, [data, compTab, sortKey]);

  if (loading) {
    return (
      <div className={styles.panel}>
        <Skeleton h={180} />
        <Skeleton h={96} />
        <Skeleton h={96} />
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

  const amenaza = asList(data.amenaza);
  const oportunidad = asList(data.oportunidad);
  const sinergicos = asList(data.sinergicos);
  const tabCounts: Record<CompTab, number> = {
    amenaza: amenaza.length,
    oportunidad: oportunidad.length,
    sinergicos: sinergicos.length,
  };

  return (
    <div className={styles.panel}>
      <MarketHeader data={data} />

      <div className={styles.mapBand}>
        <MiniMap zona={zona} amenaza={amenaza} oportunidad={oportunidad} sinergicos={sinergicos} />
        <MarketReading data={data} />
      </div>

      {data.precio_segmento && data.precio_segmento.etiqueta !== "Sin datos" && (
        <PrecioGap ps={data.precio_segmento} />
      )}

      <div className={styles.toolbar}>
        <div className={styles.compTabs}>
          {(["amenaza", "oportunidad", "sinergicos"] as CompTab[]).map((tab) => (
            <button
              key={tab}
              className={`${styles.compTab} ${compTab === tab ? styles.compTabActive : ""}`}
              onClick={() => setCompTab(tab)}
              type="button"
            >
              <span>{TAB_LABELS[tab]}</span>
              <strong>{tabCounts[tab]}</strong>
            </button>
          ))}
        </div>

        <label className={styles.sorter}>
          <span>Orden</span>
          <select value={sortKey} onChange={(event) => setSortKey(event.target.value as SortKey)}>
            <option value="amenaza">Amenaza</option>
            <option value="distancia">Distancia</option>
            <option value="rating">Rating</option>
            <option value="negatividad">Negatividad</option>
          </select>
        </label>
      </div>

      <div className={styles.tabContext}>
        {compTab === "amenaza" && "Rivales fuertes: cercania, reputacion, volumen de reseñas y mismo sector."}
        {compTab === "oportunidad" && "Locales desplazables: rating bajo, pocas reseñas o señales negativas recientes."}
        {compTab === "sinergicos" && "Negocios que atraen publico compatible sin competir directamente."}
      </div>

      <div className={styles.competitorList}>
        {activeList.length === 0 ? (
          <EmptyState message={
            compTab === "amenaza"
              ? "No hay competidores fuertes en el radio analizado."
              : compTab === "oportunidad"
                ? "No hay competidores vulnerables identificados."
                : "No hay negocios sinergicos visibles en esta zona."
          } />
        ) : (
          activeList.map((c, index) => (
            <CompetitorCard key={c.id ?? `${c.nombre}-${index}`} c={c} zona={zona} showThreat={compTab === "amenaza"} />
          ))
        )}
      </div>
    </div>
  );
}
