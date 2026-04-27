"use client";

import styles from "./TurismoPanel.module.css";

interface TurismoPanelProps {
  // Score global
  score_turismo?: number | null;
  // Sub-scores (nuevos)
  score_turismo_airbnb?: number | null;
  score_turismo_hut?: number | null;
  // Stocks crudos
  airbnb_density_500m?: number | null;
  airbnb_occupancy_est?: number | null;
  booking_hoteles_500m?: number | null;
  booking_rating_medio?: number | null;
  eventos_culturales_500m?: number | null;
  venues_musicales_500m?: number | null;
  // Distancias
  dist_playa_m?: number | null;
  dist_landmark_top3_m?: number | null;
  // Moduladores
  vcity_tourist_rate?: number | null;
  seasonality_summer_lift?: number | null;
  // Contexto
  sector_codigo?: string;
  explicacion_bullets?: string[];
}

type PerfilTuristico =
  | "playa"
  | "hotelero"
  | "cultural"
  | "mixto"
  | "residencial"
  | "sin_datos";

type Confianza = "alta" | "media" | "baja";

// ── TODO: pull from backend (calibración v2 BCN Eixample) ──────────────────
const AIRBNB_REF = 180;

// ── Saturadores log (mirror backend constants) ─────────────────────────────
const SAT_AIRBNB = 60;
const SAT_HOTELES = 15;
const SAT_POIS = 8;

function logNorm(v: number, sat: number): number {
  if (v <= 0 || sat <= 0) return 0;
  return Math.min(1, Math.log1p(v) / Math.log1p(sat));
}

/**
 * Replica de scoring/dimensiones/turismo.py::_perfil_dominante (líneas 66-81).
 * El backend NO persiste perfil_turistico — lo derivamos client-side desde stocks.
 */
function derivePerfil(
  airbnbCount: number,
  hoteles: number,
  pois: number,
  distPlaya: number | null,
): PerfilTuristico {
  const nA = logNorm(airbnbCount, SAT_AIRBNB);
  const nH = logNorm(hoteles, SAT_HOTELES);
  const nP = logNorm(pois, SAT_POIS);

  if (nA < 0.05 && nH < 0.05 && nP < 0.05) {
    if (distPlaya != null && distPlaya < 700) return "playa";
    return "residencial";
  }
  if (distPlaya != null && distPlaya < 300 && (nH > 0.3 || nA > 0.3)) {
    return "playa";
  }
  const senales: Array<[PerfilTuristico | "hotelero_informal", number]> = [
    ["hotelero", nH],
    ["cultural", nP],
    ["hotelero_informal", nA],
  ];
  senales.sort((a, b) => b[1] - a[1]);
  const top = senales[0];
  const segundo = senales[1];
  if (segundo[1] > 0.7 * top[1]) return "mixto";
  if (top[0] === "hotelero_informal") return "hotelero";
  return top[0] as PerfilTuristico;
}

function deriveConfianza(
  stocksNotNull: number,
  sumaPonderada: number,
  distPlaya: number | null,
): Confianza {
  if (stocksNotNull >= 2 && sumaPonderada > 20) return "alta";
  if (stocksNotNull >= 1 && distPlaya != null) return "media";
  return "baja";
}

interface PerfilMeta {
  emoji: string;
  label: string;
  desc: string;
}

function perfilMeta(perfil: PerfilTuristico): PerfilMeta {
  const map: Record<PerfilTuristico, PerfilMeta> = {
    playa: {
      emoji: "🏖",
      label: "Zona de playa",
      desc: "Zona litoral con afluencia turística estacional.",
    },
    hotelero: {
      emoji: "🏨",
      label: "Zona hotelera",
      desc: "Concentración de alojamiento turístico (hoteles + pisos).",
    },
    cultural: {
      emoji: "🎭",
      label: "Zona cultural",
      desc: "Atractivos culturales y eventos turísticos cercanos.",
    },
    mixto: {
      emoji: "🔀",
      label: "Zona turística mixta",
      desc: "Diversificación entre alojamiento y cultura.",
    },
    residencial: {
      emoji: "🏘",
      label: "Zona residencial",
      desc: "Baja presencia turística, predomina la clientela local.",
    },
    sin_datos: {
      emoji: "❓",
      label: "Sin datos turísticos",
      desc: "Cobertura insuficiente para clasificar el perfil.",
    },
  };
  return map[perfil];
}

function formatDist(m: number): string {
  if (m < 1000) return `${Math.round(m).toLocaleString("es-ES")} m`;
  return `${(m / 1000).toLocaleString("es-ES", { maximumFractionDigits: 1 })} km`;
}

type DependenciaLevel = "alta" | "media" | "baja";

function calcDependencia(
  density: number,
  hoteles: number,
): DependenciaLevel {
  if (density > 200 && hoteles > 3) return "alta";
  if (density > 50 || hoteles > 1) return "media";
  return "baja";
}

function getImplicacion(
  sector_codigo: string | undefined,
  level: DependenciaLevel,
  perfil: PerfilTuristico,
): string {
  const sector = sector_codigo ?? "";

  // Modificador por perfil — añade matiz estacional
  const matizPerfil =
    perfil === "playa"
      ? " La temporada de verano (mayo-septiembre) marcará tu calendario."
      : perfil === "cultural"
      ? " La afluencia es más estable, con menor estacionalidad."
      : perfil === "hotelero"
      ? " El flujo es continuo gracias al alojamiento, pero con peaks en festivos."
      : "";

  if (sector === "restauracion") {
    if (level === "alta") {
      return (
        "Para restauración, el turismo puede suponer hasta el 60% del ticket en verano." +
        matizPerfil
      );
    }
    return (
      "La clientela turística complementa la base local. Adapta carta y horarios según temporada." +
      matizPerfil
    );
  }

  if (sector === "moda") {
    if (level === "alta") {
      return (
        "Turistas buscan souvenirs y fast fashion. Diferénciate con producto local." +
        matizPerfil
      );
    }
    return "El mix de clientela local y visitantes da estabilidad anual al negocio.";
  }

  if (sector === "salud" || sector === "estetica") {
    if (level === "alta" || level === "media") {
      return "La clientela turística es baja para este tipo de negocio — el score refleja una clientela estable de residentes.";
    }
    return "Zona de residentes: clientela fiel y predecible. Bajo efecto estacional.";
  }

  if (sector === "ocio") {
    if (level === "alta") {
      return (
        "Zonas turísticas de alta densidad son ideales para ocio nocturno y experiencias." +
        matizPerfil
      );
    }
    return "Zona con clientela mayoritariamente local — apuesta por programación recurrente y fidelización.";
  }

  if (sector === "alimentacion") {
    if (level === "alta") {
      return (
        "Picos estacionales de demanda — ajusta stock y horarios. Riesgo de pasar desapercibido entre la oferta turística." +
        matizPerfil
      );
    }
    return "Clientela local fija, demanda estable. Bajo riesgo estacional.";
  }

  if (sector === "inmobiliario") {
    if (level === "alta") {
      return "Alta demanda de alquiler turístico, pero presión normativa creciente (HUT, Airbnb).";
    }
    return "Mercado dominado por residentes; demanda estable y normativa más previsible.";
  }

  if (sector === "servicios_personales" || sector === "tecnologia") {
    if (level === "alta") {
      return "La clientela turística es escasa para este sector — el score refleja la base residente.";
    }
    return "Clientela local de proximidad: fideliza y construye relación a largo plazo.";
  }

  return "La intensidad turística afecta directamente a la estacionalidad de tus ingresos." + matizPerfil;
}

interface ScoreRingProps {
  score: number;
}

function ScoreRing({ score }: ScoreRingProps) {
  const pct = Math.max(0, Math.min(100, Math.round(score)));
  const accent = "var(--accent, #7C3AED)";
  const track = "var(--surface-3, #231742)";
  return (
    <div
      className={styles.scoreRing}
      style={{
        background: `conic-gradient(${accent} ${pct * 3.6}deg, ${track} ${pct * 3.6}deg)`,
      }}
      role="img"
      aria-label={`Score turístico: ${pct} sobre 100`}
    >
      <div className={styles.scoreRingInner}>
        <span className={styles.scoreRingNum}>{pct}</span>
      </div>
    </div>
  );
}

export default function TurismoPanel({
  score_turismo,
  score_turismo_airbnb,
  score_turismo_hut,
  airbnb_density_500m,
  airbnb_occupancy_est,
  booking_hoteles_500m,
  booking_rating_medio,
  eventos_culturales_500m,
  venues_musicales_500m,
  dist_playa_m,
  dist_landmark_top3_m,
  vcity_tourist_rate,
  seasonality_summer_lift,
  sector_codigo,
  explicacion_bullets,
}: TurismoPanelProps) {
  const hasAnyData =
    airbnb_density_500m != null ||
    airbnb_occupancy_est != null ||
    booking_hoteles_500m != null ||
    eventos_culturales_500m != null ||
    venues_musicales_500m != null ||
    dist_playa_m != null ||
    dist_landmark_top3_m != null ||
    vcity_tourist_rate != null ||
    score_turismo != null;

  if (!hasAnyData) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Sin datos turísticos disponibles para esta zona.
        </div>
      </div>
    );
  }

  // ── Derivación perfil + confianza (mirror backend) ─────────────────────
  const airbnbCount = airbnb_density_500m ?? 0;
  const hoteles = booking_hoteles_500m ?? 0;
  const eventosCult = eventos_culturales_500m ?? 0;
  const venuesMus = venues_musicales_500m ?? 0;
  const poisAggregated = eventosCult + 0.5 * venuesMus;

  const stocksNotNull =
    (airbnb_density_500m != null ? 1 : 0) +
    (booking_hoteles_500m != null ? 1 : 0) +
    (eventos_culturales_500m != null ? 1 : 0);
  const sumaPonderada = airbnbCount + 5 * hoteles + 3 * eventosCult;

  const perfil = derivePerfil(
    airbnbCount,
    hoteles,
    poisAggregated,
    dist_playa_m ?? null,
  );
  const confianza = deriveConfianza(stocksNotNull, sumaPonderada, dist_playa_m ?? null);
  const meta = perfilMeta(perfil);

  // ── Dependencia (para implicación de negocio) ──────────────────────────
  const dependencia = calcDependencia(airbnbCount, hoteles);
  const implicacion = getImplicacion(sector_codigo, dependencia, perfil);

  // ── Densidad Airbnb visualization ──────────────────────────────────────
  const density = airbnbCount;
  const airbnbFillPct = Math.min((density / AIRBNB_REF) * 100, 200);
  const airbnbAboveRef = density > AIRBNB_REF;
  const refMarkerPct = Math.min(
    (AIRBNB_REF / Math.max(density, AIRBNB_REF + 1)) * 100,
    100,
  );

  // ── Sub-scores (composición turística) ─────────────────────────────────
  const scoreCultural = poisAggregated > 0 ? logNorm(poisAggregated, SAT_POIS) * 100 : null;

  // ── Ocupación Airbnb chip ──────────────────────────────────────────────
  const ocupPct =
    airbnb_occupancy_est != null ? Math.round(airbnb_occupancy_est * 100) : null;
  let ocupChipClass = styles.chipMuted;
  let ocupLabel = "";
  if (airbnb_occupancy_est != null && ocupPct != null) {
    if (airbnb_occupancy_est > 0.75) {
      ocupChipClass = styles.chipRed;
      ocupLabel = `Alta presión (${ocupPct}%)`;
    } else if (airbnb_occupancy_est >= 0.30) {
      ocupChipClass = styles.chipYellow;
      ocupLabel = `Ocupación media (${ocupPct}%)`;
    } else {
      ocupChipClass = styles.chipGreen;
      ocupLabel = `Baja ocupación (${ocupPct}%)`;
    }
  }

  // ── Hiperestacional verano ─────────────────────────────────────────────
  const showSeasonChip =
    seasonality_summer_lift != null && seasonality_summer_lift > 1.20;
  const seasonLiftPct =
    seasonality_summer_lift != null
      ? Math.round((seasonality_summer_lift - 1) * 100)
      : 0;

  // ── Hoteles render — cap 5 con +N ──────────────────────────────────────
  const HOTEL_CAP = 5;
  const hotelesShown = Math.min(hoteles, HOTEL_CAP);
  const hotelesExtra = Math.max(0, hoteles - HOTEL_CAP);

  // ── Booking rating chip ────────────────────────────────────────────────
  let ratingChipClass = styles.chipMuted;
  if (booking_rating_medio != null) {
    if (booking_rating_medio >= 8) ratingChipClass = styles.chipGreen;
    else if (booking_rating_medio >= 6) ratingChipClass = styles.chipYellow;
    else ratingChipClass = styles.chipRed;
  }

  // ── Atractivos culturales: visible si hay POIs o landmark cercano ──────
  const totalPois =
    eventos_culturales_500m != null
      ? eventos_culturales_500m + (venues_musicales_500m != null ? Math.round(venues_musicales_500m / 2) : 0)
      : null;
  const showCulturalSection =
    (eventos_culturales_500m != null && eventos_culturales_500m > 0) ||
    dist_landmark_top3_m != null;

  let landmarkChipClass = styles.landmarkChipFar;
  let landmarkLabel = "";
  if (dist_landmark_top3_m != null) {
    const distRound = Math.round(dist_landmark_top3_m);
    if (dist_landmark_top3_m < 300) {
      landmarkChipClass = styles.landmarkChipNear;
      landmarkLabel = `🏛 Landmark icónico a ${distRound} m`;
    } else if (dist_landmark_top3_m <= 700) {
      landmarkChipClass = styles.landmarkChipMid;
      landmarkLabel = `🏛 Landmark a ${distRound} m`;
    } else {
      landmarkChipClass = styles.landmarkChipFar;
      landmarkLabel = `🏛 Landmark más cercano a ${distRound} m`;
    }
  }

  // ── % peatones turistas ────────────────────────────────────────────────
  const showTouristRate = vcity_tourist_rate != null;
  const touristRatePct =
    vcity_tourist_rate != null
      ? Math.min(100, Math.max(0, Math.round(vcity_tourist_rate * 100)))
      : 0;
  let touristFillColor = "var(--green, #10B981)";
  if (vcity_tourist_rate != null) {
    if (vcity_tourist_rate > 0.40) touristFillColor = "var(--red, #EF4444)";
    else if (vcity_tourist_rate >= 0.20) touristFillColor = "var(--yellow, #F59E0B)";
  }
  const touristEvery10 =
    vcity_tourist_rate != null
      ? (vcity_tourist_rate * 10).toLocaleString("es-ES", {
          minimumFractionDigits: 1,
          maximumFractionDigits: 1,
        })
      : "0";

  // ── Confianza chip class ──────────────────────────────────────────────
  const confianzaClass =
    confianza === "alta"
      ? styles.confianzaAlta
      : confianza === "media"
      ? styles.confianzaMedia
      : styles.confianzaBaja;

  return (
    <div className={styles.panel}>

      {/* ── Section 1: Hero "Perfil turístico de la zona" ── */}
      <section className={styles.heroCard}>
        <span className={styles.sectionKicker}>Perfil turístico de la zona</span>
        <div className={styles.perfilHero}>
          <div className={styles.perfilLeft}>
            <span className={styles.perfilEmoji} role="img" aria-label={meta.label}>
              {meta.emoji}
            </span>
            <div className={styles.perfilCopy}>
              <p className={styles.perfilLabel}>{meta.label}</p>
              <p className={styles.perfilDesc}>{meta.desc}</p>
              <span className={`${styles.confianzaChip} ${confianzaClass}`}>
                Confianza {confianza}
              </span>
            </div>
          </div>

          {score_turismo != null && (
            <div className={styles.perfilRight}>
              <ScoreRing score={score_turismo} />
            </div>
          )}
        </div>
      </section>

      {/* ── Section 2: Composición turística (sub-scores) ── */}
      {(score_turismo_airbnb != null ||
        score_turismo_hut != null ||
        scoreCultural != null) && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>Composición turística</span>
          <div className={styles.subscoreList}>
            {/* Airbnb */}
            <div className={styles.subscoreRow}>
              <span className={styles.subscoreLabel}>Alojamiento informal (Airbnb)</span>
              <div
                className={styles.subscoreTrack}
                role="img"
                aria-label={
                  score_turismo_airbnb != null
                    ? `Sub-score Airbnb: ${Math.round(score_turismo_airbnb)} sobre 100`
                    : "Sub-score Airbnb sin datos"
                }
              >
                <div
                  className={`${styles.subscoreFill} ${
                    score_turismo_airbnb != null ? styles.subscoreFillAirbnb : styles.subscoreFillEmpty
                  }`}
                  style={{
                    width: `${score_turismo_airbnb != null ? Math.min(100, Math.max(0, score_turismo_airbnb)) : 0}%`,
                  }}
                />
              </div>
              <span className={styles.subscoreValue}>
                {score_turismo_airbnb != null
                  ? `${Math.round(score_turismo_airbnb)}/100`
                  : "—"}
              </span>
            </div>

            {/* Hoteles */}
            <div className={styles.subscoreRow}>
              <span className={styles.subscoreLabel}>Alojamiento formal (hoteles)</span>
              <div
                className={styles.subscoreTrack}
                role="img"
                aria-label={
                  score_turismo_hut != null
                    ? `Sub-score hoteles: ${Math.round(score_turismo_hut)} sobre 100`
                    : "Sub-score hoteles sin datos"
                }
              >
                <div
                  className={`${styles.subscoreFill} ${
                    score_turismo_hut != null ? styles.subscoreFillHotel : styles.subscoreFillEmpty
                  }`}
                  style={{
                    width: `${score_turismo_hut != null ? Math.min(100, Math.max(0, score_turismo_hut)) : 0}%`,
                  }}
                />
              </div>
              <span className={styles.subscoreValue}>
                {score_turismo_hut != null
                  ? `${Math.round(score_turismo_hut)}/100`
                  : "—"}
              </span>
            </div>

            {/* Cultural */}
            <div className={styles.subscoreRow}>
              <span className={styles.subscoreLabel}>Atractivos culturales</span>
              <div
                className={styles.subscoreTrack}
                role="img"
                aria-label={
                  scoreCultural != null
                    ? `Sub-score cultural: ${Math.round(scoreCultural)} sobre 100`
                    : "Sub-score cultural sin datos"
                }
              >
                <div
                  className={`${styles.subscoreFill} ${
                    scoreCultural != null ? styles.subscoreFillCult : styles.subscoreFillEmpty
                  }`}
                  style={{
                    width: `${scoreCultural != null ? Math.min(100, Math.max(0, scoreCultural)) : 0}%`,
                  }}
                />
              </div>
              <span className={styles.subscoreValue}>
                {scoreCultural != null ? `${Math.round(scoreCultural)}/100` : "—"}
              </span>
            </div>
          </div>
        </section>
      )}

      {/* ── Section 3: Densidad Airbnb ── */}
      {airbnb_density_500m != null && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>Densidad Airbnb</span>
          <div className={styles.airbnbBarWrap}>
            <div
              className={styles.airbnbBarTrack}
              role="img"
              aria-label={`Densidad Airbnb: ${Math.round(density)} pisos por km², referencia BCN ${AIRBNB_REF}`}
            >
              <div
                className={styles.airbnbBarFill}
                style={{
                  width: `${Math.min(airbnbFillPct, 100)}%`,
                  background: airbnbAboveRef
                    ? "var(--red, #EF4444)"
                    : "var(--green, #10B981)",
                }}
              />
              <div
                className={styles.airbnbRefMarker}
                style={{ left: `${airbnbAboveRef ? refMarkerPct : 100}%` }}
                aria-hidden="true"
              />
            </div>
            <div className={styles.airbnbMeta}>
              <span className={styles.airbnbValue}>
                {density.toLocaleString("es-ES", { maximumFractionDigits: 0 })} pis/km²
              </span>
              <div className={styles.airbnbChips}>
                {ocupLabel && (
                  <span className={`${styles.chip} ${ocupChipClass}`}>{ocupLabel}</span>
                )}
                {showSeasonChip && (
                  <span className={`${styles.chip} ${styles.seasonChip}`}>
                    🌞 Hiperestacional verano +{seasonLiftPct}%
                  </span>
                )}
              </div>
            </div>
            <p className={styles.airbnbRef}>
              Ref. BCN Eixample: {AIRBNB_REF} pis/km²
              {airbnbAboveRef ? " — por encima de la media" : " — por debajo de la media"}
            </p>
          </div>
        </section>
      )}

      {/* ── Section 4: Hoteles en 500 m ── */}
      <section className={styles.section}>
        <span className={styles.sectionKicker}>Hoteles en 500 m</span>
        {hoteles > 0 ? (
          <>
            <div className={styles.hotelBadgeRow}>
              {Array.from({ length: hotelesShown }).map((_, i) => (
                <span key={i} className={styles.hotelBadge} aria-hidden="true">
                  🏨
                </span>
              ))}
              {hotelesExtra > 0 && (
                <span className={`${styles.chip} ${styles.chipAccent}`}>+{hotelesExtra}</span>
              )}
              <span className={styles.hotelCount}>
                {hoteles.toLocaleString("es-ES")} hotel{hoteles !== 1 ? "es" : ""}
              </span>
            </div>
            {booking_rating_medio != null && (
              <div className={styles.hotelRatingRow}>
                <span className={styles.hotelRatingLabel}>Rating medio:</span>
                <span className={`${styles.chip} ${ratingChipClass}`}>
                  {booking_rating_medio.toFixed(1)} ★
                </span>
              </div>
            )}
          </>
        ) : (
          <span className={`${styles.chip} ${styles.chipMuted}`}>Sin hoteles en 500 m</span>
        )}
      </section>

      {/* ── Section 5: Atractivos culturales ── */}
      {showCulturalSection && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>Atractivos culturales</span>
          {totalPois != null && totalPois > 0 && (
            <div className={styles.culturalHero}>
              <span className={styles.culturalHeroNum}>{totalPois}</span>
              <span className={styles.culturalHeroLabel}>POIs culturales en 500 m</span>
            </div>
          )}
          {dist_landmark_top3_m != null && (
            <span className={`${styles.chip} ${styles.landmarkChip} ${landmarkChipClass}`}>
              {landmarkLabel}
            </span>
          )}
        </section>
      )}

      {/* ── Section 6: % peatones turistas ── */}
      {showTouristRate && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>% peatones turistas</span>
          <div
            className={styles.touristRateBar}
            role="img"
            aria-label={`Peatones turistas: ${touristRatePct}%, media BCN 25%`}
          >
            <div
              className={styles.touristRateFill}
              style={{
                width: `${touristRatePct}%`,
                background: touristFillColor,
              }}
            />
            <div
              className={styles.touristRateMarker}
              style={{ left: "25%" }}
              aria-hidden="true"
            />
          </div>
          <p className={styles.touristRateText}>
            {touristEvery10} de cada 10 peatones son turistas
            <span className={styles.touristRateRef}> (vs 2,5 de media en BCN)</span>
          </p>
        </section>
      )}

      {/* ── Section 7: Proximidad al mar ── */}
      {dist_playa_m != null && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>Proximidad al mar</span>
          {dist_playa_m < 1500 ? (
            <span className={`${styles.chip} ${styles.chipGreen}`}>
              A {formatDist(dist_playa_m)} de la playa
            </span>
          ) : dist_playa_m <= 3000 ? (
            <span className={`${styles.chip} ${styles.chipYellow}`}>
              {formatDist(dist_playa_m)} al mar
            </span>
          ) : (
            <span className={`${styles.chip} ${styles.chipMuted}`}>Sin factor playa</span>
          )}
        </section>
      )}

      {/* ── Section 8: Implicación para tu negocio ── */}
      <section className={styles.section}>
        <span className={styles.sectionKicker}>Implicación para tu negocio</span>
        <div className={styles.implicacionBox}>
          <p className={styles.implicacionText}>{implicacion}</p>
        </div>
      </section>

      {/* ── Section 9: Bullets ── */}
      {explicacion_bullets && explicacion_bullets.length > 0 && (
        <section className={styles.section}>
          <span className={styles.sectionKicker}>Factores detectados</span>
          <div className={styles.pillList}>
            {explicacion_bullets.map((bullet, i) => (
              <span key={i} className={styles.signalPill}>{bullet}</span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
