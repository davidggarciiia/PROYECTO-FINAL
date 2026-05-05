"use client";

/**
 * QuickQuestionnaire.tsx — Wizard completo de 5-6 pasos.
 *
 * Pasos 0-2 (nuevos): sector (nueva taxonomía UI) → subsector → input libre.
 * Pasos 3-5 (existentes): público objetivo → zona BCN → presupuesto alquiler.
 *
 * Todo el wizard usa el mismo layout centrado y las mismas clases CSS.
 */
import { useState } from "react";
import styles from "./QuickQuestionnaire.module.css";
import {
  SECTORES,
  getSubsectorList,
  getBackendSector,
  getBackendSubsector,
  getSubsectorPlaceholder,
} from "@/lib/sectorMap";
import { buildBusinessContext } from "@/lib/buildBusinessContext";
import type { PerfilEstructurado } from "@/lib/types";

interface Props {
  onComplete: (perfil: PerfilEstructurado) => void;
  onBack:     () => void;
}

type Tile = { codigo: string; label: string; desc?: string };

// ── Paso 3 — Público objetivo ──────────────────────────────────────────────
const PUBLICO_TILES: Tile[] = [
  { codigo: "jovenes",       label: "Jóvenes / estudiantes",   desc: "Perfil 18-30, poder adquisitivo medio." },
  { codigo: "familias",      label: "Familias",                 desc: "Con niños, consumo recurrente." },
  { codigo: "profesionales", label: "Profesionales y oficinas", desc: "Oficinas cercanas, tickets medios-altos." },
  { codigo: "turistas",      label: "Turistas / visitantes",    desc: "Zonas con paso turístico." },
  { codigo: "residentes",    label: "Mixto / residentes",       desc: "Sin foco específico, público variado." },
];

// ── Paso 4 — Zona de BCN ───────────────────────────────────────────────────
const ZONA_TILES: Array<Tile & { distritos?: string[] }> = [
  { codigo: "centro",      label: "Centro histórico",             desc: "Ciutat Vella, Raval, Gòtic.",            distritos: ["Ciutat Vella"] },
  { codigo: "comercial",   label: "Barrios comerciales clásicos", desc: "Eixample (Passeig de Gràcia, Consell).", distritos: ["Eixample"] },
  { codigo: "emergente",   label: "Zonas emergentes",             desc: "Gràcia, Poblenou, Sant Martí.",          distritos: ["Gràcia", "Sant Martí"] },
  { codigo: "tranquilo",   label: "Barrios tranquilos",           desc: "Sarrià, Les Corts, Horta-Guinardó.",    distritos: ["Sarrià-Sant Gervasi", "Les Corts", "Horta-Guinardó"] },
  { codigo: "indiferente", label: "Sin preferencia",              desc: "Analizar toda Barcelona.",              distritos: undefined },
];

// ── Paso 5 — Presupuesto alquiler ──────────────────────────────────────────
const PRESUPUESTO_TILES: Array<Tile & { max?: number }> = [
  { codigo: "lt1500",    label: "Hasta 1.500 €",   desc: "Barrios periféricos o locales pequeños.", max: 1500 },
  { codigo: "1500_2500", label: "1.500 – 2.500 €", desc: "Equilibrio precio / flujo.",              max: 2500 },
  { codigo: "2500_4000", label: "2.500 – 4.000 €", desc: "Ejes comerciales consolidados.",          max: 4000 },
  { codigo: "gt4000",    label: "4.000 € o más",   desc: "Prime retail, Passeig de Gràcia.",        max: undefined },
  { codigo: "sin_limite",label: "Sin preferencia",  desc: "Analizar sin filtro de alquiler.",        max: undefined },
];

export default function QuickQuestionnaire({ onComplete, onBack }: Props) {
  const [step, setStep]               = useState<number>(0);
  const [sectorUI, setSectorUI]       = useState<string>("");
  const [subsectorLabel, setSubsector]= useState<string>("");
  const [descripcion, setDescripcion] = useState<string>("");
  const [publico, setPublico]         = useState<string>("");
  const [zona, setZona]               = useState<string>("");
  const [presupuesto, setPresupuesto] = useState<string>("");
  const [leaving, setLeaving]         = useState(false);
  const [submitting, setSubmitting]   = useState(false);

  const sectorDef        = SECTORES.find((s) => s.codigo === sectorUI);
  const subsectorList    = getSubsectorList(sectorUI);
  const hasSubsectorStep = subsectorList.length > 0;
  const inputPlaceholder = getSubsectorPlaceholder(sectorUI, subsectorLabel);

  // Sector tiles: usa la nueva taxonomía UI con desc incluida
  const sectorTiles: Tile[] = SECTORES.map((s) => ({
    codigo: s.codigo,
    label:  s.label,
    desc:   s.desc,
  }));

  // Subsector tiles: etiquetas del sectorMap (sin desc para tiles más compactos)
  const subsectorTiles: Tile[] = subsectorList.map((sub) => ({
    codigo: sub.label,
    label:  sub.label,
  }));

  // Pasos totales: sector(1) + subsector?(1) + input(1) + publico(1) + zona(1) + presupuesto(1)
  const totalSteps = 3 + (hasSubsectorStep ? 3 : 2); // 6 con subsector, 5 sin

  // Paso visual (1-based)
  const visualStep = (() => {
    if (step === 0) return 1;
    if (step === 1) return 2;
    if (step === 2) return hasSubsectorStep ? 3 : 2;
    if (step === 3) return hasSubsectorStep ? 4 : 3;
    if (step === 4) return hasSubsectorStep ? 5 : 4;
    return hasSubsectorStep ? 6 : 5;
  })();

  const canNext = (() => {
    if (step === 0) return sectorUI !== "";
    if (step === 1) return subsectorLabel !== "";
    if (step === 2) return true;          // input libre, siempre puede continuar
    if (step === 3) return publico !== "";
    if (step === 4) return zona !== "";
    if (step === 5) return presupuesto !== "";
    return false;
  })();

  const goNext = () => {
    if (!canNext) return;
    // Saltar paso subsector si no hay subsectores para este sector
    if (step === 0 && !hasSubsectorStep) { setStep(2); return; }
    if (step < 5) { setStep(step + 1); return; }
    submit();
  };

  const goBack = () => {
    if (step === 0) { onBack(); return; }
    // Saltar paso subsector al volver también
    if (step === 2 && !hasSubsectorStep) { setStep(0); return; }
    setStep(step - 1);
  };

  // Auto-avance en sector → subsector (o input) tras selección
  const pickSector = (codigo: string) => {
    setSectorUI(codigo);
    setSubsector("");
    const hasSubs = getSubsectorList(codigo).length > 0;
    setStep(hasSubs ? 1 : 2);
  };

  // Auto-avance en subsector → input tras selección
  const pickSubsector = (label: string) => {
    setSubsector(label);
    setStep(2);
  };

  const submit = () => {
    if (submitting) return;
    setSubmitting(true);

    const ctx             = buildBusinessContext(sectorUI, subsectorLabel, descripcion);
    const backendSector   = getBackendSector(sectorUI, subsectorLabel);
    const backendSubsector = getBackendSubsector(sectorUI, subsectorLabel);

    const pe: PerfilEstructurado = {
      sector:    backendSector,
      subsector: backendSubsector || undefined,
      matices:   ctx.matices_enriquecidos,
    };

    if (publico) {
      pe.publico_objetivo = { estilo_vida: [publico], horarios_pico: [] };
    }

    const zonaTile = ZONA_TILES.find((t) => t.codigo === zona);
    if (zonaTile?.distritos?.length) pe.distritos = zonaTile.distritos;

    const presTile = PRESUPUESTO_TILES.find((t) => t.codigo === presupuesto);
    if (presTile?.max) pe.presupuesto_max = presTile.max;

    setLeaving(true);
    setTimeout(() => onComplete(pe), 260);
  };

  // ── Render helpers ────────────────────────────────────────────────────────

  const renderTiles = (tiles: Tile[], selected: string, onPick: (c: string) => void) => (
    <div className={styles.tileGrid}>
      {tiles.map((t) => (
        <button
          key={t.codigo}
          type="button"
          className={`${styles.tile} ${selected === t.codigo ? styles.tileActive : ""}`}
          onClick={() => onPick(t.codigo)}
        >
          <div className={styles.tileTop}>
            <div className={styles.tileLabel}>{t.label}</div>
            {t.desc && <span className={styles.tileInfoIcon}>i</span>}
          </div>
          {t.desc && <div className={styles.tileDesc}>{t.desc}</div>}
        </button>
      ))}
    </div>
  );

  const TITLES: Record<number, { h: string; sub?: string }> = {
    0: { h: "¿En qué sector está tu negocio?",        sub: "Elige la categoría que mejor lo describe." },
    1: { h: `¿Qué tipo de ${sectorDef?.label.toLowerCase() ?? "negocio"} exactamente?`, sub: "Elige el que más se parezca a tu idea." },
    2: { h: "¿Quieres especificar más tu negocio?",   sub: "Cuanto más detalles des, más preciso será el análisis." },
    3: { h: "¿Quién es tu cliente ideal?",             sub: "El público al que apunta tu negocio." },
    4: { h: "¿Qué zona de Barcelona te encaja?",       sub: `Si no sabes, elige "Sin preferencia".` },
    5: { h: "¿Alquiler mensual que puedes asumir?",    sub: "Filtramos zonas dentro de ese rango." },
  };

  const currentTitle = TITLES[step];

  return (
    <div className={`${styles.wizard} ${leaving ? styles.isLeaving : ""}`}>
      <div className={styles.grid} />

      <div className={styles.topbar}>
        <div className={styles.mark}>
          <span className={styles.markDot} />
          KnowPoints
        </div>
        <span className={styles.chip}>CUESTIONARIO GUIADO</span>
        <div className={styles.spacer} />
        <span className="eyebrow">
          PASO {visualStep} / {totalSteps}
        </span>
      </div>

      <div className={styles.progressTrack}>
        <div
          className={styles.progressFill}
          style={{ width: `${(visualStep / totalSteps) * 100}%` }}
        />
      </div>

      <main className={styles.main}>
        <div className={styles.header}>
          <h1 className={styles.question}>
            {currentTitle.h}
            {step === 2 && <span className={styles.badgeOpcional}>Opcional</span>}
          </h1>
          {currentTitle.sub && <p className={styles.lede}>{currentTitle.sub}</p>}
        </div>

        <div className={styles.body}>
          {step === 0 && renderTiles(sectorTiles, sectorUI, pickSector)}

          {step === 1 && renderTiles(subsectorTiles, subsectorLabel, pickSubsector)}

          {step === 2 && (
            <div className={styles.inputStep}>
              <div className={styles.promptShell}>
                <textarea
                  autoFocus
                  value={descripcion}
                  onChange={(e) => setDescripcion(e.target.value)}
                  placeholder={`Ej: ${inputPlaceholder}`}
                  className={styles.promptTextarea}
                  maxLength={300}
                  rows={4}
                />
                <div className={styles.promptFoot}>
                  <span className={styles.promptMeta}>
                    💡 Cuanto más detalles des, más preciso será el análisis · {descripcion.length}/300
                  </span>
                </div>
              </div>
            </div>
          )}

          {step === 3 && renderTiles(PUBLICO_TILES, publico, setPublico)}

          {step === 4 && renderTiles(ZONA_TILES, zona, setZona)}

          {step === 5 && renderTiles(PRESUPUESTO_TILES, presupuesto, setPresupuesto)}
        </div>
      </main>

      <footer className={styles.footer}>
        <button type="button" onClick={goBack} className={styles.btnGhost} disabled={submitting}>
          ← {step === 0 ? "Volver" : "Atrás"}
        </button>
        <button
          type="button"
          onClick={goNext}
          className={`btn btn-primary ${styles.btnNext}`}
          disabled={!canNext || submitting}
        >
          {submitting
            ? <span className="spinner" />
            : step === 5 ? "Analizar Barcelona →" : "Siguiente →"}
        </button>
      </footer>
    </div>
  );
}
