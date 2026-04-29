"use client";

import { useState } from "react";
import styles from "./Onboarding.module.css";

interface Props {
  onStart: () => void;
}

export default function Onboarding({ onStart }: Props) {
  const [leaving, setLeaving] = useState(false);

  const handleStart = () => {
    setLeaving(true);
    setTimeout(onStart, 320);
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
        {/* ── Left column ──────────────────────────────────────── */}
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

          <div className={styles.ctaSection}>
            <p className={styles.lede}>
              Cargamos 12 bases de datos de Barcelona — aforadors, censo,
              normativa, competencia — y te devolvemos un mapa ordenado por
              viabilidad real, con modelo financiero y trámites ya trazados.
            </p>
            <button
              type="button"
              className={styles.btnStart}
              onClick={handleStart}
            >
              Empezar a crear tu negocio →
            </button>
          </div>
        </div>

        {/* ── Right column — stats panel ────────────────────────── */}
        <div className={styles.right}>
          <div className={styles.rightBg} aria-hidden="true">
            <svg
              viewBox="0 0 400 600"
              width="100%"
              height="100%"
              preserveAspectRatio="xMidYMid slice"
            >
              <defs>
                <pattern id="ob-dots" width="18" height="18" patternUnits="userSpaceOnUse">
                  <circle cx="1" cy="1" r="0.8" fill="rgba(243,238,255,0.06)" />
                </pattern>
              </defs>
              <rect width="400" height="600" fill="url(#ob-dots)" />
              <path
                d="M40 360 Q110 280 180 310 Q240 340 280 290 Q330 240 370 280 L360 440 L280 500 L160 510 L80 460 Z"
                fill="none" stroke="rgba(124,58,237,0.30)" strokeWidth="1" strokeDasharray="3 4"
              />
              <path
                d="M120 220 Q180 180 240 200 Q300 220 330 260 L310 330 L220 350 L160 330 L120 280 Z"
                fill="rgba(124,58,237,0.05)" stroke="rgba(124,58,237,0.22)" strokeWidth="1"
              />
              <g>
                <circle cx="220" cy="250" r="14" fill="none" stroke="#10B981" strokeWidth="1.5" />
                <circle cx="220" cy="250" r="4" fill="#10B981" />
                <text x="240" y="246" fontFamily="var(--font-mono)" fontSize="10" fill="#10B981">87 · PG. GRÀCIA</text>
              </g>
              <g>
                <circle cx="180" cy="310" r="10" fill="none" stroke="#10B981" strokeWidth="1.5" />
                <circle cx="180" cy="310" r="3" fill="#10B981" />
                <text x="50" y="312" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.5)">82 · GRÀCIA</text>
              </g>
              <g>
                <circle cx="290" cy="390" r="8" fill="none" stroke="#F59E0B" strokeWidth="1.5" />
                <circle cx="290" cy="390" r="2.5" fill="#F59E0B" />
                <text x="305" y="392" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.4)">71 · RAVAL</text>
              </g>
              <g>
                <circle cx="130" cy="450" r="6" fill="none" stroke="#EF4444" strokeWidth="1" />
                <circle cx="130" cy="450" r="2" fill="#EF4444" />
                <text x="50" y="470" fontFamily="var(--font-mono)" fontSize="9" fill="rgba(243,238,255,0.35)">44 · HORTA</text>
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
                <div className={styles.statValue}><span className={styles.emph}>12</span></div>
                <div className={styles.statMeta}>INE · Aforadors · Idescat</div>
              </div>
              <div className={styles.stat}>
                <div className={styles.statLabel}>Tiempo análisis</div>
                <div className={styles.statValue}>42<span style={{ fontSize: 16 }}>s</span></div>
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
