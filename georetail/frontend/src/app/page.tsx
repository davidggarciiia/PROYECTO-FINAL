import type { CSSProperties, ReactNode } from "react";
import Link from "next/link";
import LandingNav from "@/components/landing/LandingNav";
import styles from "./page.module.css";

/* ════════════════════════════════════════════════════════════════
   Inline SVGs — all hand-rolled, no photos, Feather-style strokes.
   ════════════════════════════════════════════════════════════════ */

function ArrowRight() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden
    >
      <path d="M5 12h14" />
      <path d="m13 6 6 6-6 6" />
    </svg>
  );
}

function BarcelonaSkyline() {
  // Rows of twinkling windows — deterministic delays, varied durations.
  const windows: Array<{ x: number; y: number; d: number; dur: number }> = [
    { x: 62, y: 215, d: 0.2, dur: 5.5 },
    { x: 62, y: 232, d: 1.1, dur: 6.5 },
    { x: 62, y: 249, d: 2.3, dur: 7 },
    { x: 80, y: 215, d: 0.6, dur: 5 },
    { x: 80, y: 232, d: 1.9, dur: 8 },
    { x: 140, y: 175, d: 0.4, dur: 6 },
    { x: 158, y: 175, d: 1.5, dur: 7.2 },
    { x: 140, y: 195, d: 2.1, dur: 5.8 },
    { x: 158, y: 195, d: 0.9, dur: 9 },
    { x: 140, y: 215, d: 2.8, dur: 6.2 },
    { x: 158, y: 215, d: 1.2, dur: 7.5 },
    { x: 225, y: 135, d: 0.1, dur: 6.6 },
    { x: 243, y: 135, d: 1.7, dur: 5.3 },
    { x: 225, y: 155, d: 2.6, dur: 8.1 },
    { x: 243, y: 155, d: 0.8, dur: 5.9 },
    { x: 225, y: 175, d: 3.2, dur: 7 },
    { x: 243, y: 175, d: 1.4, dur: 6.3 },
    { x: 225, y: 195, d: 2.2, dur: 8.8 },
    { x: 243, y: 195, d: 0.3, dur: 5.4 },
    { x: 310, y: 160, d: 1.0, dur: 6.7 },
    { x: 328, y: 160, d: 2.4, dur: 5.2 },
    { x: 310, y: 180, d: 0.7, dur: 7.4 },
    { x: 328, y: 180, d: 1.8, dur: 8.5 },
    { x: 310, y: 200, d: 2.9, dur: 6 },
    { x: 396, y: 210, d: 0.5, dur: 7.1 },
    { x: 414, y: 210, d: 1.6, dur: 5.7 },
    { x: 396, y: 228, d: 2.7, dur: 8.3 },
    { x: 414, y: 228, d: 1.3, dur: 6.4 },
  ];

  return (
    <svg
      className={styles.skyline}
      width="520"
      height="360"
      viewBox="0 0 520 360"
      fill="none"
      aria-hidden
    >
      <defs>
        <linearGradient id="bldGrad1" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#2A1656" stopOpacity="0.9" />
          <stop offset="100%" stopColor="#0C0720" stopOpacity="0.95" />
        </linearGradient>
        <linearGradient id="bldGrad2" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#3B1D7A" stopOpacity="0.85" />
          <stop offset="100%" stopColor="#120A2A" stopOpacity="0.95" />
        </linearGradient>
        <linearGradient id="bldGrad3" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#1E1140" stopOpacity="0.9" />
          <stop offset="100%" stopColor="#0A0618" stopOpacity="0.95" />
        </linearGradient>
        <linearGradient id="bldAccent" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#C026D3" stopOpacity="0.35" />
          <stop offset="100%" stopColor="#6B21A8" stopOpacity="0.25" />
        </linearGradient>
        <linearGradient id="skylineFadeL" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor="#09060F" stopOpacity="1" />
          <stop offset="30%" stopColor="#09060F" stopOpacity="0" />
        </linearGradient>
      </defs>

      {/* Torre Agbar (stylized oval) */}
      <ellipse cx="465" cy="195" rx="18" ry="60" fill="url(#bldGrad2)" stroke="rgba(192,38,211,0.35)" strokeWidth="1" />
      <ellipse cx="465" cy="145" rx="14" ry="12" fill="url(#bldAccent)" />

      {/* Sagrada Familia — four tapered spires */}
      <path d="M210 360 L210 130 L216 118 L222 130 L222 360 Z" fill="url(#bldGrad2)" stroke="rgba(168,85,247,0.3)" strokeWidth="1" />
      <path d="M228 360 L228 110 L234 96 L240 110 L240 360 Z" fill="url(#bldGrad1)" stroke="rgba(168,85,247,0.35)" strokeWidth="1" />
      <path d="M246 360 L246 82 L252 66 L258 82 L258 360 Z" fill="url(#bldGrad2)" stroke="rgba(192,38,211,0.4)" strokeWidth="1.1" />
      <path d="M264 360 L264 108 L270 94 L276 108 L276 360 Z" fill="url(#bldGrad1)" stroke="rgba(168,85,247,0.3)" strokeWidth="1" />

      {/* Small spires accent dots */}
      <circle cx="216" cy="116" r="1.8" fill="#C026D3" opacity="0.8" />
      <circle cx="234" cy="94" r="1.8" fill="#C026D3" opacity="0.8" />
      <circle cx="252" cy="64" r="2.2" fill="#C026D3" opacity="0.9" />
      <circle cx="270" cy="92" r="1.8" fill="#C026D3" opacity="0.8" />

      {/* Residential blocks foreground */}
      <rect x="20" y="240" width="78" height="120" fill="url(#bldGrad3)" stroke="rgba(168,85,247,0.22)" strokeWidth="1" />
      <rect x="105" y="205" width="82" height="155" fill="url(#bldGrad1)" stroke="rgba(168,85,247,0.28)" strokeWidth="1" />
      <rect x="298" y="195" width="72" height="165" fill="url(#bldGrad3)" stroke="rgba(168,85,247,0.22)" strokeWidth="1" />
      <rect x="384" y="235" width="62" height="125" fill="url(#bldGrad1)" stroke="rgba(168,85,247,0.26)" strokeWidth="1" />

      {/* Chimneys / antennas */}
      <line x1="56" y1="240" x2="56" y2="220" stroke="rgba(168,85,247,0.35)" strokeWidth="1.2" />
      <line x1="148" y1="205" x2="148" y2="180" stroke="rgba(168,85,247,0.35)" strokeWidth="1.2" />
      <line x1="340" y1="195" x2="340" y2="165" stroke="rgba(168,85,247,0.35)" strokeWidth="1.2" />
      <line x1="410" y1="235" x2="410" y2="212" stroke="rgba(168,85,247,0.35)" strokeWidth="1.2" />

      {/* Distant buildings (muted) */}
      <rect x="360" y="260" width="22" height="100" fill="url(#bldGrad2)" opacity="0.6" />
      <rect x="450" y="250" width="28" height="110" fill="url(#bldGrad2)" opacity="0.6" />
      <rect x="485" y="240" width="26" height="120" fill="url(#bldGrad3)" opacity="0.55" />

      {/* Ground line */}
      <line x1="0" y1="360" x2="520" y2="360" stroke="rgba(168,85,247,0.18)" strokeWidth="1" />

      {/* Windows — twinkling */}
      {windows.map((w, i) => (
        <rect
          key={i}
          x={w.x}
          y={w.y}
          width="6"
          height="7"
          rx="0.8"
          fill="#C4B5FD"
          style={
            {
              animation: `kp-twinkle ${w.dur}s ease-in-out ${w.d}s infinite`,
              "--tw-min": "0.12",
              "--tw-max": "0.7",
            } as CSSProperties
          }
        />
      ))}

      {/* Left-fade mask to protect text legibility */}
      <rect x="0" y="0" width="520" height="360" fill="url(#skylineFadeL)" />
    </svg>
  );
}

function IllustrationModel() {
  return (
    <svg viewBox="0 0 480 280" width="100%" height="100%" aria-hidden>
      <defs>
        <linearGradient id="mBar" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#A855F7" />
          <stop offset="100%" stopColor="#6B21A8" />
        </linearGradient>
        <linearGradient id="mBarMuted" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#3B1D7A" />
          <stop offset="100%" stopColor="#1A1230" />
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="480" height="280" fill="transparent" />
      {/* Axes */}
      <line x1="48" y1="232" x2="448" y2="232" stroke="rgba(255,255,255,0.10)" strokeWidth="1" />
      <line x1="48" y1="40" x2="48" y2="232" stroke="rgba(255,255,255,0.10)" strokeWidth="1" />
      {/* Gridlines */}
      {[80, 128, 176].map((y) => (
        <line key={y} x1="48" y1={y} x2="448" y2={y} stroke="rgba(255,255,255,0.05)" strokeWidth="1" strokeDasharray="3 4" />
      ))}
      {/* Feature importance bars */}
      {[
        { label: "Flujo peatonal", v: 0.92 },
        { label: "Renta mediana", v: 0.78 },
        { label: "Competencia 200m", v: 0.64 },
        { label: "Accesibilidad", v: 0.51 },
        { label: "Dinamismo", v: 0.42 },
        { label: "Entorno comercial", v: 0.33 },
      ].map((f, i) => {
        const barW = 320 * f.v;
        const y = 52 + i * 30;
        return (
          <g key={f.label}>
            <text x="42" y={y + 12} textAnchor="end" fontFamily="Inter, sans-serif" fontSize="10" fill="#A78BC8" fontWeight="500">
              {f.label}
            </text>
            <rect x="60" y={y} width={barW} height="16" rx="3" fill="url(#mBar)" />
            <rect x={60 + barW} y={y} width={320 - barW} height="16" rx="3" fill="url(#mBarMuted)" opacity="0.6" />
            <text x={60 + barW + 8} y={y + 12} fontFamily="ui-monospace, monospace" fontSize="9" fill="#A855F7" letterSpacing="0.1em">
              {(f.v * 100).toFixed(0)}%
            </text>
          </g>
        );
      })}
      {/* Mono caption */}
      <text x="48" y="258" fontFamily="ui-monospace, monospace" fontSize="9" fill="#6B5A8A" letterSpacing="0.15em">
        FEATURE IMPORTANCE · XGBOOST v13
      </text>
    </svg>
  );
}

function IllustrationDimensions() {
  // Radar-like polygon across 8 axes.
  const axes = [
    "Flujo peatonal",
    "Demografía",
    "Competencia",
    "Transporte",
    "Entorno",
    "Dinamismo",
    "Seguridad",
    "Turismo",
  ];
  const cx = 240;
  const cy = 140;
  const r = 96;
  const values = [0.88, 0.76, 0.62, 0.71, 0.58, 0.5, 0.82, 0.66];
  const angleFor = (i: number) => -Math.PI / 2 + (i * 2 * Math.PI) / axes.length;
  const pt = (i: number, scale: number) => {
    const a = angleFor(i);
    return `${cx + Math.cos(a) * r * scale},${cy + Math.sin(a) * r * scale}`;
  };
  const poly = values.map((v, i) => pt(i, v)).join(" ");

  return (
    <svg viewBox="0 0 480 280" width="100%" height="100%" aria-hidden>
      <defs>
        <radialGradient id="dRad" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="#C026D3" stopOpacity="0.45" />
          <stop offset="100%" stopColor="#6B21A8" stopOpacity="0.15" />
        </radialGradient>
      </defs>
      {/* Rings */}
      {[0.25, 0.5, 0.75, 1].map((s) => (
        <polygon
          key={s}
          points={axes.map((_, i) => pt(i, s)).join(" ")}
          fill="none"
          stroke="rgba(255,255,255,0.07)"
          strokeWidth="1"
        />
      ))}
      {/* Axes */}
      {axes.map((_, i) => (
        <line
          key={i}
          x1={cx}
          y1={cy}
          x2={cx + Math.cos(angleFor(i)) * r}
          y2={cy + Math.sin(angleFor(i)) * r}
          stroke="rgba(255,255,255,0.07)"
          strokeWidth="1"
        />
      ))}
      {/* Polygon */}
      <polygon points={poly} fill="url(#dRad)" stroke="#A855F7" strokeWidth="1.4" />
      {/* Vertices */}
      {values.map((v, i) => {
        const a = angleFor(i);
        return (
          <circle
            key={i}
            cx={cx + Math.cos(a) * r * v}
            cy={cy + Math.sin(a) * r * v}
            r="3"
            fill="#C026D3"
          />
        );
      })}
      {/* Labels */}
      {axes.map((label, i) => {
        const a = angleFor(i);
        const lx = cx + Math.cos(a) * (r + 22);
        const ly = cy + Math.sin(a) * (r + 22);
        return (
          <text
            key={label}
            x={lx}
            y={ly}
            fontFamily="Inter, sans-serif"
            fontSize="10"
            fontWeight="600"
            fill="#A78BC8"
            textAnchor="middle"
            dominantBaseline="middle"
          >
            {label}
          </text>
        );
      })}
      <text x="48" y="258" fontFamily="ui-monospace, monospace" fontSize="9" fill="#6B5A8A" letterSpacing="0.15em">
        8 DIMENSIONES · SCORE COMPUESTO
      </text>
    </svg>
  );
}

function IllustrationFinance() {
  // 36-month revenue vs cost with break-even marker.
  const pts = (fn: (m: number) => number) => {
    const out: string[] = [];
    for (let m = 0; m < 36; m++) {
      const x = 50 + (m / 35) * 390;
      const y = 220 - fn(m) * 170;
      out.push(`${x},${y}`);
    }
    return out.join(" ");
  };
  const revenue = (m: number) => Math.min(1, 0.12 + 0.025 * m + 0.35 * (1 - Math.exp(-m / 12)));
  const cost = (m: number) => Math.max(0.18, 0.55 - 0.005 * m);
  const breakEvenM = 11;
  const breX = 50 + (breakEvenM / 35) * 390;
  return (
    <svg viewBox="0 0 480 280" width="100%" height="100%" aria-hidden>
      <defs>
        <linearGradient id="fRev" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#A855F7" stopOpacity="0.5" />
          <stop offset="100%" stopColor="#6B21A8" stopOpacity="0" />
        </linearGradient>
      </defs>
      {/* Axes */}
      <line x1="50" y1="220" x2="440" y2="220" stroke="rgba(255,255,255,0.10)" strokeWidth="1" />
      <line x1="50" y1="40" x2="50" y2="220" stroke="rgba(255,255,255,0.10)" strokeWidth="1" />
      {/* Gridlines */}
      {[80, 130, 180].map((y) => (
        <line key={y} x1="50" y1={y} x2="440" y2={y} stroke="rgba(255,255,255,0.05)" strokeWidth="1" strokeDasharray="3 4" />
      ))}
      {/* Revenue area */}
      <polygon
        points={`50,220 ${pts(revenue)} 440,220`}
        fill="url(#fRev)"
      />
      {/* Revenue line */}
      <polyline points={pts(revenue)} fill="none" stroke="#C026D3" strokeWidth="2" strokeLinecap="round" />
      {/* Cost line */}
      <polyline points={pts(cost)} fill="none" stroke="#F59E0B" strokeWidth="1.6" strokeDasharray="4 3" strokeLinecap="round" />
      {/* Break-even marker */}
      <line x1={breX} y1="40" x2={breX} y2="220" stroke="#10B981" strokeWidth="1" strokeDasharray="3 3" opacity="0.75" />
      <circle cx={breX} cy="112" r="4" fill="#10B981" />
      <text x={breX + 8} y="108" fontFamily="Inter, sans-serif" fontSize="10" fontWeight="700" fill="#10B981">
        Break-even · mes 11
      </text>
      {/* Axis labels */}
      <text x="50" y="238" fontFamily="ui-monospace, monospace" fontSize="8" fill="#6B5A8A" letterSpacing="0.15em">
        M1
      </text>
      <text x="240" y="238" fontFamily="ui-monospace, monospace" fontSize="8" fill="#6B5A8A" letterSpacing="0.15em">
        M18
      </text>
      <text x="430" y="238" fontFamily="ui-monospace, monospace" fontSize="8" fill="#6B5A8A" letterSpacing="0.15em">
        M36
      </text>
      {/* Legend */}
      <g transform="translate(300, 52)">
        <rect x="0" y="0" width="10" height="3" fill="#C026D3" />
        <text x="16" y="4" fontFamily="Inter, sans-serif" fontSize="10" fill="#A78BC8" fontWeight="500">Ingresos</text>
        <rect x="82" y="0" width="10" height="3" fill="#F59E0B" />
        <text x="98" y="4" fontFamily="Inter, sans-serif" fontSize="10" fill="#A78BC8" fontWeight="500">Costes</text>
      </g>
      <text x="50" y="258" fontFamily="ui-monospace, monospace" fontSize="9" fill="#6B5A8A" letterSpacing="0.15em">
        PROYECCIÓN 36 MESES · P&amp;L
      </text>
    </svg>
  );
}

/* ════════════════════════════════════════════════════════════════
   Small render helpers
   ════════════════════════════════════════════════════════════════ */

type Feature = {
  eyebrow: string;
  title: string;
  body: string;
  source: string;
  illustration: ReactNode;
  reverse?: boolean;
};

const FEATURES: Feature[] = [
  {
    eyebrow: "MODELO ENTRENADO",
    title: "Modelo entrenado con historia real",
    body: "XGBoost sobre 12.000 aperturas y cierres de negocios en Barcelona. No es una heurística: es supervivencia comercial aprendida zona por zona.",
    source: "Datos: Cens Comercial BCN",
    illustration: <IllustrationModel />,
  },
  {
    eyebrow: "DIMENSIONES QUE IMPORTAN",
    title: "Ocho ejes, una sola decisión",
    body: "Flujo peatonal, demografía, competencia, transporte, entorno, dinamismo, seguridad y turismo — pesados según lo que predice cierre en tu sector.",
    source: "Datos: Ajuntament de Barcelona",
    illustration: <IllustrationDimensions />,
    reverse: true,
  },
  {
    eyebrow: "FINANZAS AUTOMÁTICAS",
    title: "Del mapa a la cuenta de resultados",
    body: "Proyección a 36 meses con ingresos, costes, ROI y payback calculados desde el primer clic. Ajusta alquiler, ticket o personal con sliders.",
    source: "Datos: Idealista + Foursquare",
    illustration: <IllustrationFinance />,
  },
];

const METRICS = [
  { value: "12.000", label: "Aperturas entrenadas" },
  { value: "47.000", label: "Locales cubiertos" },
  { value: "48", label: "Distritos + barrios" },
  { value: "Semanal", label: "Actualización de datos" },
];

const STEPS = [
  {
    n: "01",
    title: "Describe tu negocio",
    body: "Tipo, público, presupuesto y tamaño — en lenguaje natural. Un cuestionario breve cubre lo que falte.",
  },
  {
    n: "02",
    title: "Analiza el mapa",
    body: "Zonas coloreadas por score (verde ≥ 75, ámbar 50–74, rojo < 50). Abre cualquiera para ver el desglose.",
  },
  {
    n: "03",
    title: "Exporta tu informe",
    body: "Score, explicabilidad, proyección financiera y checklist legal — en un PDF listo para socio o banco.",
  },
];

const TESTIMONIOS = [
  {
    chip: "CAFETERÍA · GRÀCIA",
    quote:
      "Lo que me habría llevado seis semanas de consultoría lo tuve en 45 segundos. Y acerté el barrio.",
    name: "Elena Martí",
    role: "Fundadora · Espai Cafè",
  },
  {
    chip: "RETAIL · EIXAMPLE",
    quote:
      "La proyección a 36 meses fue clave para el banco. Cerré la financiación en dos semanas.",
    name: "Raúl Domènech",
    role: "Óptica Domènech",
  },
  {
    chip: "RESTAURANTE · SANTS",
    quote:
      "El score me ahorró un mal local. La competencia a 150 metros no la había visto.",
    name: "Marta Puig",
    role: "Cuina Puig · Sants-Montjuïc",
  },
];

/* ════════════════════════════════════════════════════════════════
   Page
   ════════════════════════════════════════════════════════════════ */

export default function LandingPage() {
  return (
    <>
      <LandingNav />

      <main className={styles.main}>
        {/* ══════════════════════════════════════════════════════
             HERO
             ══════════════════════════════════════════════════════ */}
        <section className={styles.hero} id="producto">
          <div className={styles.heroBg}>
            <div className={`${styles.glow} ${styles.glowA}`} />
            <div className={`${styles.glow} ${styles.glowB}`} />
            <div className={styles.grid} />
          </div>

          <div className={styles.heroInner}>
            <div className={styles.heroLeft}>
              <span
                className={`eyebrow ${styles.heroEyebrow}`}
                style={{ animationDelay: "0.1s" }}
              >
                ANÁLISIS DE PRECISIÓN · BARCELONA
              </span>
              <h1
                className={styles.heroTitle}
                style={{ animationDelay: "0.2s" }}
              >
                Encuentra el lugar perfecto para{" "}
                <span className="gradient-text">tu negocio</span>
              </h1>
              <p
                className={`lead ${styles.heroLead}`}
                style={{ animationDelay: "0.35s" }}
              >
                El análisis que haría una consultora de retail — en tiempo real, desde el navegador.
                Score de viabilidad, proyección financiera y checklist legal, por barrio.
              </p>
              <div
                className={styles.heroCtas}
                style={{ animationDelay: "0.5s" }}
              >
                <Link href="/app" className={`${styles.cta} ${styles.ctaPrimary}`}>
                  Probar gratis <ArrowRight />
                </Link>
                <a href="#como-funciona" className={`${styles.cta} ${styles.ctaGhost}`}>
                  Ver demostración
                </a>
              </div>
              <div
                className={styles.heroMeta}
                style={{ animationDelay: "0.65s" }}
              >
                <span className={styles.dot} />
                <span>Sin registro · Datos públicos BCN · Modelo XGBoost v13</span>
              </div>
            </div>

            <div className={styles.heroRight} aria-hidden>
              <BarcelonaSkyline />
            </div>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             TRUST / METRICS STRIP
             ══════════════════════════════════════════════════════ */}
        <section className={styles.metricsStrip} aria-label="Métricas clave">
          <div className={styles.metricsInner}>
            {METRICS.map((m, i) => (
              <div key={m.label} className={styles.metric}>
                <div className={`${styles.metricValue} mono`}>{m.value}</div>
                <div className={styles.metricLabel}>{m.label}</div>
                {i < METRICS.length - 1 && <span className={styles.metricDivider} aria-hidden />}
              </div>
            ))}
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             FEATURE ROWS
             ══════════════════════════════════════════════════════ */}
        <section className={styles.features}>
          <div className={styles.sectionInner}>
            <div className={styles.sectionHeader}>
              <span className="eyebrow">DATOS REALES, INTELIGENCIA REAL</span>
              <h2>El análisis que haría una consultora — automatizado</h2>
              <p className="lead">
                Tres capas que convierten datos públicos de Barcelona en una decisión de ubicación.
              </p>
            </div>

            {FEATURES.map((f) => (
              <div
                key={f.title}
                className={`${styles.featureRow} ${f.reverse ? styles.featureRowReverse : ""}`}
              >
                <div className={styles.featureCopy}>
                  <span className="eyebrow">{f.eyebrow}</span>
                  <h3>{f.title}</h3>
                  <p>{f.body}</p>
                  <span className={`badge badge-purple ${styles.sourcePill}`}>{f.source}</span>
                </div>
                <div className={styles.featureCard}>
                  <div className={styles.illustrationWrap}>{f.illustration}</div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             HOW IT WORKS
             ══════════════════════════════════════════════════════ */}
        <section className={styles.how} id="como-funciona">
          <div className={styles.sectionInner}>
            <div className={styles.sectionHeader}>
              <span className="eyebrow">EN TRES PASOS</span>
              <h2>De la idea al mapa en menos de un minuto</h2>
            </div>
            <div className={styles.stepsGrid}>
              {STEPS.map((s) => (
                <div key={s.n} className={`card ${styles.stepCard}`}>
                  <span className={`eyebrow ${styles.stepNumber}`}>{s.n}</span>
                  <h4 className={styles.stepTitle}>{s.title}</h4>
                  <p>{s.body}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             TESTIMONIALS
             ══════════════════════════════════════════════════════ */}
        <section className={styles.testimonials} id="testimonios">
          <div className={styles.sectionInner}>
            <div className={styles.sectionHeader}>
              <span className="eyebrow">EMPRENDEDORES</span>
              <h2>Elegidos con datos, no con corazonadas</h2>
            </div>
            <div className={styles.testiGrid}>
              {TESTIMONIOS.map((t) => (
                <figure key={t.name} className={`card ${styles.testiCard}`}>
                  <span className={`badge badge-purple ${styles.testiChip}`}>{t.chip}</span>
                  <blockquote className={styles.testiQuote}>“{t.quote}”</blockquote>
                  <figcaption className={styles.testiAuthor}>
                    <div className={styles.testiName}>{t.name}</div>
                    <div className={styles.testiRole}>{t.role}</div>
                  </figcaption>
                </figure>
              ))}
            </div>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             FINAL CTA
             ══════════════════════════════════════════════════════ */}
        <section className={styles.finalCta}>
          <div className={styles.finalCtaGlow} aria-hidden />
          <div className={styles.finalCtaInner}>
            <span className="eyebrow">EMPIEZA AHORA</span>
            <h2 className={styles.finalCtaTitle}>
              Encuentra tu sitio en{" "}
              <span className="gradient-text">Barcelona</span>
            </h2>
            <p className="lead">
              Describe tu idea, recibe un mapa con score por zona y un informe listo para socio o banco.
            </p>
            <Link href="/app" className={`${styles.cta} ${styles.ctaPrimary} ${styles.ctaFinal}`}>
              Probar gratis <ArrowRight />
            </Link>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════
             FOOTER
             ══════════════════════════════════════════════════════ */}
        <footer className={styles.footer}>
          <div className={styles.footerInner}>
            <div className={styles.footerBrand}>
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src="/logo-wordmark.svg" alt="KnowPoints" height={28} />
              <p className={styles.footerTagline}>
                Análisis de ubicación comercial para Barcelona.
              </p>
            </div>
            <div className={styles.footerCols}>
              <div className={styles.footerCol}>
                <div className={styles.footerColTitle}>Producto</div>
                <a href="#producto">Características</a>
                <a href="#como-funciona">Cómo funciona</a>
                <Link href="/app">Abrir la app</Link>
              </div>
              <div className={styles.footerCol}>
                <div className={styles.footerColTitle}>Empresa</div>
                <a href="#testimonios">Testimonios</a>
                <a href="mailto:hola@knowpoints.app">Contacto</a>
              </div>
              <div className={styles.footerCol}>
                <div className={styles.footerColTitle}>Legal</div>
                <a href="#">Privacidad</a>
                <a href="#">Términos</a>
                <a href="#">Cookies</a>
              </div>
            </div>
          </div>
          <div className={styles.footerBottom}>
            <span>© {new Date().getFullYear()} KnowPoints</span>
            <span className={styles.footerDot}>·</span>
            <span>Hecho en Barcelona con datos abiertos</span>
          </div>
        </footer>
      </main>
    </>
  );
}
