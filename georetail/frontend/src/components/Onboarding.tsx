"use client";

/**
 * Onboarding.tsx — Pre-app "Signal" stage for KnowPoints.
 *
 * Port of prototype_v1/onboarding.jsx usando tokens KnowPoints y
 * CSS Modules. Entrada de usuario en una sola pantalla: describe el
 * negocio, o elige un escenario pre-cargado, y pasa al mapa.
 *
 * Integración sugerida en app/app/page.tsx:
 *
 *   const [started, setStarted] = useState(false);
 *   const [initialQuery, setInitialQuery] = useState("");
 *
 *   if (!started) {
 *     return (
 *       <Onboarding onSubmit={(q) => {
 *         setInitialQuery(q);
 *         setStarted(true);
 *         // luego dispara la búsqueda con q
 *       }} />
 *     );
 *   }
 *   // ... resto del shell del mapa
 */
import { useState, FormEvent } from "react";
import styles from "./Onboarding.module.css";

interface Props {
  onSubmit: (query: string) => void;
}

const SUGGESTIONS = [
  { id: "01", text: "Cafetería de especialidad · público joven · 80 m²" },
  { id: "02", text: "Óptica boutique · ticket medio-alto · 60 m²" },
  { id: "03", text: "Restaurante italiano informal · 40 cubiertos" },
  { id: "04", text: "Tienda de ropa vintage · barrio con carácter" },
];

export default function Onboarding({ onSubmit }: Props) {
  const [q, setQ] = useState("");
  const [leaving, setLeaving] = useState(false);

  const submit = (text?: string) => {
    const value = (text ?? q).trim();
    if (value.length < 10) return;
    setLeaving(true);
    setTimeout(() => onSubmit(value), 320);
  };

  const handleFormSubmit = (e: FormEvent) => {
    e.preventDefault();
    submit();
  };

  return (
    <div className={`${styles.ob} ${leaving ? styles.isLeaving : ""}`}>
      <div className={styles.grid} />

      <div className={styles.topbar}>
        <div className={styles.mark}>
          <span className={styles.markDot} />
          KnowPoints
        </div>
        <span className={styles.chip}>BETA · BARCELONA</span>
        <div className={styles.spacer} />
        <span className="eyebrow">UNIDAD 02 / SIGNAL</span>
        <span className="eyebrow">
          <span className={styles.tick}>●</span>&nbsp;EN LÍNEA
        </span>
      </div>

      <div className={styles.main}>
        <div className={styles.left}>
          <div>
            <div className="eyebrow" style={{ marginBottom: 18 }}>
              INTELIGENCIA DE UBICACIÓN · V 1.2
            </div>
            <h1 className={styles.headline}>
              Deja de <span className={styles.strike}>suponer</span>
              <br />
              dónde montar tu
              <br />
              <span className={styles.emph}>negocio</span>.
            </h1>
          </div>

          <p className={styles.lede}>
            Cargamos 12 bases de datos de Barcelona — aforadors, censo,
            normativa, competencia — y te devolvemos un mapa ordenado por
            viabilidad real, con modelo financiero y trámites ya trazados.
          </p>

          <form className={styles.form} onSubmit={handleFormSubmit}>
            <div className={styles.promptShell}>
              <textarea
                autoFocus
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="describe tu negocio, público, presupuesto, tamaño…"
                className={styles.textarea}
                maxLength={500}
                rows={3}
              />
              <div className={styles.promptFoot}>
                <span className={styles.meta}>
                  {q.length}/500 · ENTER para analizar
                </span>
                <button
                  type="submit"
                  className={`btn ${styles.btnSignal}`}
                  disabled={q.trim().length < 4}
                >
                  Escanear Barcelona →
                </button>
              </div>
            </div>

            <div className={styles.suggWrap}>
              <div className={styles.suggTitle}>
                O lanza un escenario pre-cargado
              </div>
              <div className={styles.suggList}>
                {SUGGESTIONS.map((s) => (
                  <button
                    key={s.id}
                    type="button"
                    className={styles.sugg}
                    onClick={() => submit(s.text)}
                  >
                    <span className={styles.suggIco}>{s.id} ›</span>
                    <span>{s.text}</span>
                  </button>
                ))}
              </div>
            </div>
          </form>
        </div>

        <div className={styles.right}>
          <div className={styles.rightBg} aria-hidden="true">
            <svg
              viewBox="0 0 400 600"
              width="100%"
              height="100%"
              preserveAspectRatio="xMidYMid slice"
            >
              <defs>
                <pattern
                  id="ob-dots"
                  width="18"
                  height="18"
                  patternUnits="userSpaceOnUse"
                >
                  <circle cx="1" cy="1" r="0.8" fill="rgba(243,238,255,0.06)" />
                </pattern>
              </defs>
              <rect width="400" height="600" fill="url(#ob-dots)" />
              <path
                d="M40 360 Q110 280 180 310 Q240 340 280 290 Q330 240 370 280 L360 440 L280 500 L160 510 L80 460 Z"
                fill="none"
                stroke="rgba(124,58,237,0.30)"
                strokeWidth="1"
                strokeDasharray="3 4"
              />
              <path
                d="M120 220 Q180 180 240 200 Q300 220 330 260 L310 330 L220 350 L160 330 L120 280 Z"
                fill="rgba(124,58,237,0.05)"
                stroke="rgba(124,58,237,0.22)"
                strokeWidth="1"
              />
              {/* Score markers */}
              <g>
                <circle cx="220" cy="250" r="14" fill="none" stroke="#10B981" strokeWidth="1.5" />
                <circle cx="220" cy="250" r="4" fill="#10B981" />
                <text x="240" y="246" fontFamily="var(--font-mono)" fontSize="10" fill="#10B981">
                  87 · PG. GRÀCIA
                </text>
              </g>
              <g>
                <circle cx="180" cy="310" r="10" fill="none" stroke="#10B981" strokeWidth="1.5" />
                <circle cx="180" cy="310" r="3" fill="#10B981" />
                <text x="50" y="312" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.5)">
                  82 · GRÀCIA
                </text>
              </g>
              <g>
                <circle cx="290" cy="390" r="8" fill="none" stroke="#F59E0B" strokeWidth="1.5" />
                <circle cx="290" cy="390" r="2.5" fill="#F59E0B" />
                <text x="305" y="392" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.4)">
                  71 · RAVAL
                </text>
              </g>
              <g>
                <circle cx="130" cy="450" r="6" fill="none" stroke="#EF4444" strokeWidth="1" />
                <circle cx="130" cy="450" r="2" fill="#EF4444" />
                <text x="50" y="470" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.35)">
                  44 · HORTA
                </text>
              </g>
            </svg>
          </div>

          <div className={styles.rightContent}>
            <div className="eyebrow">DATOS EN VIVO · BARCELONA</div>
            <div className={styles.statRow}>
              <div className={styles.stat}>
                <div className={styles.statLabel}>Zonas indexadas</div>
                <div className={styles.statValue}>2.847</div>
                <div className={styles.statMeta}>73 distritos</div>
              </div>
              <div className={styles.stat}>
                <div className={styles.statLabel}>Fuentes cruzadas</div>
                <div className={styles.statValue}>
                  <span className={styles.emph}>12</span>
                </div>
                <div className={styles.statMeta}>INE · Aforadors · Idescat</div>
              </div>
              <div className={styles.stat}>
                <div className={styles.statLabel}>Tiempo análisis</div>
                <div className={styles.statValue}>
                  42<span style={{ fontSize: 16 }}>s</span>
                </div>
                <div className={styles.statMeta}>tu prompt → reporte</div>
              </div>
            </div>
            <div className={styles.dash} />
            <div className={styles.quote}>
              La diferencia entre un buen local y{" "}
              <span className={styles.emph}>el mejor local</span> son 3 calles —
              y muchos datos.
            </div>
            <div className={styles.quoteSig}>— KNOWPOINTS // MANIFIESTO</div>
          </div>

          <div className={styles.rightFoot}>
            <span>FIG. 02 · Cartografía de viabilidad</span>
            <span className={styles.signal}>● 41.39 N / 2.17 E</span>
          </div>
        </div>
      </div>
    </div>
  );
}
