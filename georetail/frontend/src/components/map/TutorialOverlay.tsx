"use client";

import { useState } from "react";
import styles from "./TutorialOverlay.module.css";

interface Props {
  onClose: () => void;
}

const SLIDES = [
  {
    title: "Bienvenido a KnowPoints",
    description:
      "KnowPoints analiza Barcelona para encontrarte las mejores ubicaciones donde abrir tu negocio. El mapa muestra zonas coloreadas según su puntuación de viabilidad comercial. Este tutorial te explica cada elemento de la interfaz.",
    highlight: "Vista general de la aplicación",
  },
  {
    title: "Barra superior",
    description:
      "La barra superior muestra tu búsqueda activa y el número de zonas encontradas. El botón «Nueva búsqueda» te permite volver al inicio para explorar un tipo de negocio diferente.",
    highlight: "Barra de estado y navegación",
  },
  {
    title: "Zonas del mapa",
    description:
      "Cada marcador circular representa una zona analizada de Barcelona. El color indica la puntuación de viabilidad: verde (>75 puntos), amarillo (50–75) o rojo (<50). Haz clic en cualquier zona para seleccionarla.",
    highlight: "Marcadores de zona coloreados por puntuación",
  },
  {
    title: "Panel de zona activa",
    description:
      "En la parte inferior izquierda aparece el resumen de la zona seleccionada: score global, barrio, precio estimado de alquiler y barras de puntuación por dimensión — flujo peatonal, demografía, competencia, transporte y más.",
    highlight: "Dock inferior con puntuación y dimensiones",
  },
  {
    title: "Lista de zonas",
    description:
      "El panel lateral derecho muestra todas las zonas encontradas ordenadas por puntuación. Pulsa sobre cualquiera para centrar el mapa en ella. Las flechas del panel inferior también permiten navegar entre zonas.",
    highlight: "Índice de zonas — panel lateral derecho",
  },
  {
    title: "Refinar la búsqueda",
    description:
      "El buscador flotante central permite añadir contexto o cambiar los criterios dentro de la misma sesión: «con terraza», «sin competencia directa», «cerca del metro»… El motor recalculará las zonas al instante.",
    highlight: "Caja de búsqueda flotante para refinar resultados",
  },
  {
    title: "Análisis completo",
    description:
      "Pulsa «Ver análisis completo» en el panel inferior para abrir el dossier de la zona: puntuación por cada dimensión, mapa de competencia, perfil de clientela, proyección financiera a 36 meses y requisitos legales del sector.",
    highlight: "Dossier de análisis — panel lateral completo",
  },
];

export default function TutorialOverlay({ onClose }: Props) {
  const [step, setStep] = useState(0);
  const total = SLIDES.length;
  const slide = SLIDES[step];
  const isLast = step === total - 1;

  const handleNext = () => {
    if (!isLast) setStep((s) => s + 1);
    else onClose();
  };

  const handlePrev = () => {
    if (step > 0) setStep((s) => s - 1);
  };

  return (
    <div
      className={styles.overlay}
      role="dialog"
      aria-modal="true"
      aria-label="Tutorial de KnowPoints"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modal}>
        {/* Header */}
        <div className={styles.header}>
          <span className={styles.eyebrow}>
            Tutorial &middot; {step + 1} / {total}
          </span>
          <button
            className={styles.skipBtn}
            onClick={onClose}
            aria-label="Saltar tutorial"
          >
            Saltar tutorial
          </button>
        </div>

        {/* Progress pills */}
        <div className={styles.progressRow} role="tablist" aria-label="Pasos del tutorial">
          {SLIDES.map((_, i) => (
            <button
              key={i}
              role="tab"
              aria-selected={i === step}
              className={`${styles.dot} ${i === step ? styles.dotActive : ""} ${i < step ? styles.dotDone : ""}`}
              onClick={() => setStep(i)}
              aria-label={`Ir al paso ${i + 1}`}
            />
          ))}
        </div>

        {/* Screenshot placeholder */}
        <div className={styles.screenshotWrap} key={`sc-${step}`}>
          <div className={styles.screenshot} aria-label="Espacio para captura de pantalla">
            <svg
              className={styles.screenshotIcon}
              viewBox="0 0 24 24"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
            >
              <rect x="1" y="5" width="22" height="15" rx="2" stroke="currentColor" strokeWidth="1.5" />
              <circle cx="12" cy="12.5" r="3.5" stroke="currentColor" strokeWidth="1.5" />
              <path
                d="M8 5L9.5 3h5L16 5"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <path d="M20 8h1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
            </svg>
            <span className={styles.screenshotCaption}>Captura de pantalla</span>
            <span className={styles.screenshotHighlight}>{slide.highlight}</span>
          </div>
        </div>

        {/* Content */}
        <div className={styles.content} key={`ct-${step}`}>
          <h2 className={styles.title}>{slide.title}</h2>
          <p className={styles.description}>{slide.description}</p>
        </div>

        {/* Navigation */}
        <div className={styles.nav}>
          <button
            className={styles.btnSecondary}
            onClick={handlePrev}
            disabled={step === 0}
          >
            Anterior
          </button>
          <button className={styles.btnPrimary} onClick={handleNext}>
            {isLast ? "Empezar a explorar" : "Siguiente"}
          </button>
        </div>
      </div>
    </div>
  );
}
