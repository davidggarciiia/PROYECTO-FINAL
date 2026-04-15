"use client";

import Link from "next/link";
import styles from "./page.module.css";

/* ── Deterministic pseudo-random (no hydration mismatch) ── */
function pr(seed: number) { return ((seed * 9301 + 49297) % 233280) / 233280; }
function winFill(seed: number, threshold: number, litColor: string, dimColor: string) {
  return pr(seed) > threshold ? litColor : dimColor;
}
function winOpacity(seed: number) { return 0.5 + pr(seed * 7) * 0.5; }

/* ── Inline KnowPoints logo SVG ── */
function KPLogo({ size = 36 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 80 80" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="kpGrad" x1="0" y1="0" x2="80" y2="80" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#6B21A8" />
          <stop offset="100%" stopColor="#C026D3" />
        </linearGradient>
      </defs>
      {/* Building — vertical bar of K */}
      <rect x="4" y="8" width="26" height="58" rx="4" fill="url(#kpGrad)" />
      {/* Window grid */}
      <rect x="10" y="15" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="19" y="15" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="10" y="25" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="19" y="25" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="10" y="35" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="19" y="35" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="10" y="45" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      <rect x="19" y="45" width="6" height="6" rx="1" fill="white" opacity="0.75" />
      {/* K diagonal up */}
      <path d="M30 37 L56 10" stroke="url(#kpGrad)" strokeWidth="9" strokeLinecap="round" />
      {/* K diagonal down */}
      <path d="M30 43 L50 62" stroke="url(#kpGrad)" strokeWidth="9" strokeLinecap="round" />
      {/* Location pin circle (the "o") */}
      <circle cx="62" cy="46" r="13" fill="url(#kpGrad)" />
      <circle cx="62" cy="46" r="6" fill="#09060F" />
      {/* Pin tail */}
      <path d="M62 59 L58 72" stroke="url(#kpGrad)" strokeWidth="7" strokeLinecap="round" />
    </svg>
  );
}

const FEATURES = [
  {
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" />
        <path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" />
      </svg>
    ),
    label: "Tráfico Peatonal",
    title: "Datos reales de afluencia",
    desc: "Analiza el flujo de personas a lo largo del día con datos de sensores municipales. Identifica las horas pico y elige la calle que más te conviene.",
    tag: "Aforadors BCN",
    tagColor: "purple",
  },
  {
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="12" cy="12" r="10" /><line x1="2" y1="12" x2="22" y2="12" />
        <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
      </svg>
    ),
    label: "Competencia",
    title: "Proximidad a competidores",
    desc: "Detecta negocios similares en un radio de 1 km. Identifica zonas saturadas y nichos sin explotar antes de comprometerte con un local.",
    tag: "Google Places + Foursquare",
    tagColor: "blue",
  },
  {
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
      </svg>
    ),
    label: "Score IA",
    title: "Potencial de crecimiento",
    desc: "Score de viabilidad 0-100 calculado con XGBoost sobre 21 variables. Compara barrios al instante y filtra por tu presupuesto de alquiler.",
    tag: "XGBoost + SHAP",
    tagColor: "green",
  },
  {
    icon: (
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
        <line x1="12" y1="1" x2="12" y2="23" /><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
      </svg>
    ),
    label: "Financiero",
    title: "Proyección financiera automática",
    desc: "Estimación de ROI a 36 meses sin introducir datos manualmente. Ajusta ticket medio, alquiler y nº de empleados con sliders para explorar escenarios.",
    tag: "36 meses · ROI · Payback",
    tagColor: "amber",
  },
];

const STEPS = [
  {
    num: "01",
    title: "Define tu negocio",
    desc: "Describe tu idea en lenguaje natural: tipo de negocio, tamaño, presupuesto, preferencias de barrio. KnowPoints entiende el contexto sin formularios complejos.",
  },
  {
    num: "02",
    title: "Analiza las métricas",
    desc: "Revisamos cientos de ubicaciones en tiempo real. Tráfico peatonal, competidores cercanos, demografía, precios de alquiler y datos de supervivencia de negocios similares.",
  },
  {
    num: "03",
    title: "Encuentra tu lugar",
    desc: "Recibe un mapa interactivo con zonas rankeadas por viabilidad. Explora el análisis detallado y exporta el informe completo en PDF para presentarlo a inversores.",
  },
];

const TESTIMONIALS = [
  {
    quote: "El análisis de tráfico peatonal de KnowPoints nos ahorró meses de investigación. Encontramos nuestra ubicación en una semana y la proyección financiera fue casi exacta.",
    name: "Marta Vidal",
    role: "Fundadora, Café Saló — Gràcia",
    initials: "MV",
  },
  {
    quote: "Usamos KnowPoints para comparar 8 locales en el Eixample. El score de competidores fue clave: descubrimos un nicho en una calle secundaria que ninguna consultora nos habría señalado.",
    name: "Jordi Puigdomènech",
    role: "CEO, Studio Ink BCN",
    initials: "JP",
  },
  {
    quote: "La proyección financiera automática fue lo que nos convenció al banco. En 40 minutos teníamos un informe PDF profesional con ROI y payback. Increíble.",
    name: "Elena Ros",
    role: "Directora, Espai Wellness Diagonal",
    initials: "ER",
  },
];

export default function LandingPage() {
  return (
    <div className={styles.landing}>

      {/* ══ NAV ══ */}
      <nav className={styles.nav}>
        <div className={styles.navInner}>
          <Link href="/" className={styles.navBrand}>
            <KPLogo size={32} />
            <span className={styles.navBrandName}>KnowPoints</span>
          </Link>

          <div className={styles.navLinks}>
            <a href="#features" className={styles.navLink}>Características</a>
            <a href="#process" className={styles.navLink}>Cómo funciona</a>
            <a href="#testimonials" className={styles.navLink}>Testimonios</a>
          </div>

          <Link href="/app" className={styles.navCta}>
            Empezar gratis
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M5 12h14M12 5l7 7-7 7" />
            </svg>
          </Link>
        </div>
      </nav>

      {/* ══ HERO ══ */}
      <section className={styles.hero}>
        {/* Background decorations */}
        <div className={styles.heroBg}>
          <div className={styles.heroGlow1} />
          <div className={styles.heroGlow2} />
          <div className={styles.heroGrid} />
          {/* City skyline SVG */}
          <svg
            className={styles.heroSkyline}
            viewBox="0 0 950 600"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            preserveAspectRatio="xMaxYMax slice"
          >
            <defs>
              <linearGradient id="bldGrad1" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#2A1848" />
                <stop offset="100%" stopColor="#120C1C" />
              </linearGradient>
              <linearGradient id="bldGrad2" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#1E1238" />
                <stop offset="100%" stopColor="#0F0820" />
              </linearGradient>
              <linearGradient id="bldGrad3" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#231545" />
                <stop offset="100%" stopColor="#160E2A" />
              </linearGradient>
              <linearGradient id="bldAccent" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#3D1A70" />
                <stop offset="100%" stopColor="#1A0F38" />
              </linearGradient>
              <filter id="bldGlow">
                <feGaussianBlur stdDeviation="2" result="blur" />
                <feComposite in="SourceGraphic" in2="blur" operator="over" />
              </filter>
              <linearGradient id="groundFade" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#09060F" stopOpacity="0" />
                <stop offset="100%" stopColor="#09060F" stopOpacity="1" />
              </linearGradient>
            </defs>

            {/* ── Background buildings (far / dark) ── */}
            <rect x="510" y="180" width="100" height="420" fill="url(#bldGrad2)" />
            <rect x="620" y="230" width="85"  height="370" fill="url(#bldGrad2)" />
            <rect x="660" y="150" width="115" height="450" fill="url(#bldGrad2)" />
            <rect x="440" y="270" width="90"  height="330" fill="url(#bldGrad2)" />

            {/* ── Mid buildings ── */}
            {/* Tall left */}
            <rect x="380" y="100" width="130" height="500" fill="url(#bldGrad1)" />
            {/* Windows left — 5 cols */}
            {[130,160,190,220,250,280,310,340,370,400,430,460].map((y, i) =>
              [395,415,435,455,475].map((x, j) => (
                <rect key={`wL${i}${j}`} x={x} y={y} width="11" height="14" rx="1"
                  fill={winFill(i*5+j+1, 0.35, "rgba(192,130,255,0.55)", "rgba(124,58,237,0.15)")}
                  opacity={winOpacity(i*5+j+10)}
                />
              ))
            )}

            {/* Tall center-right */}
            <rect x="530" y="60" width="150" height="540" fill="url(#bldGrad3)" />
            {/* Windows center — 6 cols */}
            {[90,120,150,180,210,240,270,300,330,360,390,420,450,480].map((y, i) =>
              [542,563,584,605,626,647].map((x, j) => (
                <rect key={`wC${i}${j}`} x={x} y={y} width="11" height="14" rx="1"
                  fill={winFill(i*6+j+100, 0.30, "rgba(168,85,247,0.60)", "rgba(88,28,135,0.20)")}
                  opacity={winOpacity(i*6+j+200)}
                />
              ))
            )}

            {/* Right tower */}
            <rect x="690" y="40" width="130" height="560" fill="url(#bldAccent)" />
            {/* Windows right — 5 cols */}
            {[70,100,130,160,190,220,250,280,310,340,370,400,430,460,490].map((y, i) =>
              [702,722,742,762,782].map((x, j) => (
                <rect key={`wR${i}${j}`} x={x} y={y} width="11" height="14" rx="1"
                  fill={winFill(i*5+j+300, 0.25, "rgba(216,180,254,0.65)", "rgba(124,58,237,0.20)")}
                  opacity={winOpacity(i*5+j+400)}
                />
              ))
            )}

            {/* Far right wide slab */}
            <rect x="825" y="110" width="90" height="490" fill="url(#bldGrad1)" />
            {[138,166,194,222,250,278,306,334,362,390].map((y, i) =>
              [836,856,876].map((x, j) => (
                <rect key={`wFR${i}${j}`} x={x} y={y} width="10" height="13" rx="1"
                  fill="rgba(192,130,255,0.45)"
                  opacity={winOpacity(i*3+j+500)}
                />
              ))
            )}

            {/* ── Foreground building (nearest, left of cluster) ── */}
            <rect x="330" y="210" width="100" height="390" fill="#1A0F38" />
            {[235,262,289,316,343,370,397,424,451,478].map((y, i) =>
              [342,362,382,402].map((x, j) => (
                <rect key={`wFG${i}${j}`} x={x} y={y} width="10" height="13" rx="1"
                  fill={winFill(i*4+j+600, 0.4, "rgba(216,180,254,0.70)", "rgba(88,28,135,0.15)")}
                  opacity={winOpacity(i*4+j+700)}
                />
              ))
            )}

            {/* ── Antenna / spire on tallest ── */}
            <line x1="605" y1="60" x2="605" y2="10" stroke="rgba(192,130,255,0.40)" strokeWidth="2" />
            <circle cx="605" cy="10" r="3" fill="rgba(216,180,254,0.80)" />
            <circle cx="605" cy="10" r="7" fill="rgba(168,85,247,0.20)" />

            <line x1="755" y1="40" x2="755" y2="-10" stroke="rgba(192,130,255,0.35)" strokeWidth="2" />
            <circle cx="755" cy="-10" r="3" fill="rgba(216,180,254,0.70)" />

            {/* ── Ground / base fade ── */}
            <rect x="0" y="540" width="950" height="60" fill="url(#groundFade)" />

            {/* ── Subtle purple haze at building tops ── */}
            <ellipse cx="660" cy="180" rx="280" ry="70"
              fill="rgba(124,58,237,0.07)" />
          </svg>
        </div>

        <div className={styles.heroContent}>
          <div className={styles.heroBadge}>
            <span className={styles.heroBadgeDot} />
            IA + Datos en tiempo real · Barcelona
          </div>

          <h1 className={styles.heroTitle}>
            Encuentra el Lugar<br />
            Perfecto para tu<br />
            <span className={styles.heroTitleAccent}>Negocio.</span>
          </h1>

          <p className={styles.heroSubtitle}>
            KnowPoints analiza tráfico peatonal, competidores y viabilidad financiera
            para recomendarte las mejores ubicaciones comerciales de Barcelona — en tiempo real,
            desde el navegador.
          </p>

          <div className={styles.heroCtas}>
            <Link href="/app" className={styles.heroCtaPrimary}>
              Analizar ubicaciones
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M5 12h14M12 5l7 7-7 7" />
              </svg>
            </Link>
            <a href="#process" className={styles.heroCtaSecondary}>
              Ver cómo funciona
            </a>
          </div>

          <div className={styles.heroStats}>
            <div className={styles.heroStat}>
              <span className={styles.heroStatNum}>21</span>
              <span className={styles.heroStatLabel}>variables por zona</span>
            </div>
            <div className={styles.heroStatDivider} />
            <div className={styles.heroStat}>
              <span className={styles.heroStatNum}>36m</span>
              <span className={styles.heroStatLabel}>proyección financiera</span>
            </div>
            <div className={styles.heroStatDivider} />
            <div className={styles.heroStat}>
              <span className={styles.heroStatNum}>BCN</span>
              <span className={styles.heroStatLabel}>datos municipales</span>
            </div>
          </div>
        </div>

      </section>

      {/* ══ FEATURES ══ */}
      <section id="features" className={styles.features}>
        <div className={styles.sectionInner}>
          <div className={styles.sectionHeader}>
            <p className={styles.sectionEyebrow}>Análisis de Precisión</p>
            <h2 className={styles.sectionTitle}>Datos reales del mundo,<br />inteligencia del mundo real.</h2>
            <p className={styles.sectionDesc}>
              Integramos más de diez fuentes de datos públicas y privadas para darte
              la visión más completa de cada ubicación comercial en Barcelona.
            </p>
          </div>

          <div className={styles.featuresGrid}>
            {FEATURES.map((f) => (
              <div key={f.label} className={styles.featureCard}>
                <div className={styles.featureIcon}>{f.icon}</div>
                <div className={styles.featureEyebrow}>{f.label}</div>
                <h3 className={styles.featureTitle}>{f.title}</h3>
                <p className={styles.featureDesc}>{f.desc}</p>
                <span className={`${styles.featureTag} ${styles[`featureTag_${f.tagColor}`]}`}>
                  {f.tag}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ══ PROCESS ══ */}
      <section id="process" className={styles.process}>
        <div className={styles.sectionInner}>
          <div className={styles.sectionHeader}>
            <p className={styles.sectionEyebrow}>El Proceso de Precisión</p>
            <h2 className={styles.sectionTitle}>Tres pasos para asegurar<br />el futuro de tu negocio.</h2>
            <p className={styles.sectionDesc}>
              Desde la idea hasta el informe PDF listo para inversores, todo en menos de cinco minutos.
            </p>
          </div>

          <div className={styles.stepsGrid}>
            {STEPS.map((s) => (
              <div key={s.num} className={styles.stepCard}>
                <div className={styles.stepNum}>{s.num}</div>
                <h3 className={styles.stepTitle}>{s.title}</h3>
                <p className={styles.stepDesc}>{s.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ══ TESTIMONIALS ══ */}
      <section id="testimonials" className={styles.testimonials}>
        <div className={styles.sectionInner}>
          <div className={styles.sectionHeader}>
            <p className={styles.sectionEyebrow}>Construido por datos</p>
            <h2 className={styles.sectionTitle}>Validado por el éxito.</h2>
          </div>

          <div className={styles.testimonialsGrid}>
            {TESTIMONIALS.map((t) => (
              <div key={t.name} className={styles.testimonialCard}>
                <div className={styles.testimonialQuote}>
                  <svg width="20" height="16" viewBox="0 0 20 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                    <path d="M0 16V9.6C0 4.267 2.933 1.067 8.8 0L9.6 1.6C7.2 2.133 5.467 3.2 4.4 4.8C3.333 6.4 2.933 8 3.2 9.6H6.4V16H0ZM11.2 16V9.6C11.2 4.267 14.133 1.067 20 0L20.8 1.6C18.4 2.133 16.667 3.2 15.6 4.8C14.533 6.4 14.133 8 14.4 9.6H17.6V16H11.2Z" fill="currentColor" opacity="0.4" />
                  </svg>
                  <p>{t.quote}</p>
                </div>
                <div className={styles.testimonialAuthor}>
                  <div className={styles.testimonialAvatar}>{t.initials}</div>
                  <div>
                    <div className={styles.testimonialName}>{t.name}</div>
                    <div className={styles.testimonialRole}>{t.role}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ══ CTA ══ */}
      <section className={styles.cta}>
        <div className={styles.ctaGlow} />
        <div className={styles.ctaInner}>
          <h2 className={styles.ctaTitle}>
            ¿Listo para encontrar<br />tu base de operaciones?
          </h2>
          <p className={styles.ctaDesc}>
            Únete a cientos de emprendedores que ya encontraron su ubicación ideal con KnowPoints.
            Sin tarjeta de crédito. Sin compromiso.
          </p>
          <div className={styles.ctaBtns}>
            <Link href="/app" className={styles.ctaBtnPrimary}>
              Empieza tu análisis gratuito
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M5 12h14M12 5l7 7-7 7" />
              </svg>
            </Link>
            <a href="mailto:hola@knowpoints.app" className={styles.ctaBtnSecondary}>
              Hablar con el equipo
            </a>
          </div>
        </div>
      </section>

      {/* ══ FOOTER ══ */}
      <footer className={styles.footer}>
        <div className={styles.footerInner}>
          <div className={styles.footerBrand}>
            <div className={styles.footerLogo}>
              <KPLogo size={28} />
              <span>KnowPoints</span>
            </div>
            <p className={styles.footerTagline}>
              Inteligencia de ubicación comercial para Barcelona.
            </p>
          </div>

          <div className={styles.footerLinks}>
            <div className={styles.footerCol}>
              <div className={styles.footerColTitle}>Producto</div>
              <Link href="/app" className={styles.footerLink}>Analizador</Link>
              <a href="#features" className={styles.footerLink}>Características</a>
              <a href="#process" className={styles.footerLink}>Cómo funciona</a>
            </div>
            <div className={styles.footerCol}>
              <div className={styles.footerColTitle}>Empresa</div>
              <a href="#" className={styles.footerLink}>Sobre nosotros</a>
              <a href="mailto:hola@knowpoints.app" className={styles.footerLink}>Contacto</a>
            </div>
            <div className={styles.footerCol}>
              <div className={styles.footerColTitle}>Legal</div>
              <a href="#" className={styles.footerLink}>Privacidad</a>
              <a href="#" className={styles.footerLink}>Términos</a>
              <a href="#" className={styles.footerLink}>Cookies</a>
            </div>
          </div>
        </div>

        <div className={styles.footerBottom}>
          <span>© 2026 KnowPoints Technologies. Todos los derechos reservados.</span>
          <span>Datos: Open Data BCN · TMB · Idealista · Google Places</span>
        </div>
      </footer>
    </div>
  );
}
