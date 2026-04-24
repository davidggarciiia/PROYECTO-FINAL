"use client";

/**
 * QuickQuestionnaire.tsx — Wizard full-screen de 5 preguntas.
 *
 * Alternativa al texto libre del Onboarding y al formulario detallado del
 * SearchBox. Cinco decisiones con 4-5 opciones tipo tile grande: sector,
 * concepto exacto, cliente ideal, zona preferida y presupuesto de alquiler.
 *
 * Output: PerfilEstructurado parcial (solo `sector` es obligatorio; el
 * backend acepta el resto como undefined — ver georetail/backend/api/buscar.py).
 */
import { useEffect, useRef, useState } from "react";
import styles from "./QuickQuestionnaire.module.css";
import { api } from "@/lib/api";
import type {
  OpcionesCuestionarioResponse,
  PerfilEstructurado,
  SubsectorOpcion,
} from "@/lib/types";

interface Props {
  onComplete: (perfil: PerfilEstructurado) => void;
  onBack: () => void;
}

type Tile = { codigo: string; label: string; desc?: string };

// ── Paso 1 — Sectores principales ──────────────────────────────────────────
const SECTORES_PRIMARIOS: Tile[] = [
  { codigo: "restauracion", label: "Restauración",          desc: "Bar, cafetería, restaurante, brunch, sushi…" },
  { codigo: "moda",         label: "Moda y tiendas",        desc: "Boutique, vintage, streetwear, zapatillas…" },
  { codigo: "estetica",     label: "Estética y belleza",    desc: "Peluquería, barbería, uñas, spa, masajes…" },
  { codigo: "salud",        label: "Salud y bienestar",     desc: "Dental, fisio, óptica, nutricionista…" },
  { codigo: "deporte",      label: "Deporte y fitness",     desc: "Gym boutique, yoga, pilates, pádel…" },
  { codigo: "alimentacion", label: "Alimentación",          desc: "Super barrio, panadería, tienda ecológica…" },
  { codigo: "hogar",        label: "Hogar y decoración",    desc: "Muebles, textil, interiorismo, iluminación…" },
  { codigo: "mascotas",     label: "Mascotas",              desc: "Tienda, veterinaria, guardería, peluquería…" },
  { codigo: "cultura",      label: "Cultura y ocio",        desc: "Librería, galería de arte, discos, juguetes…" },
  { codigo: "educacion",    label: "Educación y formación", desc: "Idiomas, academia, música, cocina, talleres…" },
  { codigo: "servicios",    label: "Servicios de barrio",   desc: "Coworking, tintorería, copistería, viajes…" },
  { codigo: "otro",         label: "Otro",                  desc: "Tatuajes, shisha, cualquier otro concepto…" },
];

// ── Paso 2 — Subsectores curados (subset de BASE_VARIANTS_BY_SECTOR) ───────
// Los labels finales se sustituyen por el que devuelve /api/cuestionario/opciones
// cuando está disponible, para garantizar consistencia con la taxonomía.
const SUBSECTORES_CURADOS: Record<string, string[]> = {
  restauracion: ["neighborhood_cafe", "specialty_coffee", "vermut_bar", "tapas_bar", "brunch_house", "street_food_counter", "sushi_counter", "smoothie_bar", "fine_dining", "cocktail_bar", "vegan_bistro"],
  moda:         ["premium_boutique", "vintage_store", "streetwear_store", "sneaker_shop", "concept_store", "kids_fashion", "sportswear_store", "accessories_store", "jewelry_store"],
  estetica:     ["hair_salon", "barber_shop", "nail_studio", "day_spa", "beauty_clinic", "brow_lash_bar", "massage_center", "skin_clinic"],
  salud:        ["dental_clinic", "physio_clinic", "psychology_center", "nutritionist", "osteopathy", "optician", "pharmacy", "aesthetic_medicine", "veterinary_clinic"],
  deporte:      ["boutique_gym", "yoga_studio", "pilates_reformer", "padel_club", "cycling_studio", "crossfit_box", "dance_fitness", "martial_arts", "swimming_club"],
  alimentacion: ["neighborhood_cafe", "bakery_takeaway", "greengrocer", "organic_market", "delicatessen", "cheese_shop", "coffee_roaster", "asian_grocery", "wine_shop", "butcher_shop"],
  hogar:        ["furniture_store", "home_textiles", "interior_design", "lighting_store", "kitchen_store", "second_hand_furniture"],
  mascotas:     ["pet_shop", "vet_clinic", "pet_grooming_salon", "pet_daycare", "aquarium_shop"],
  cultura:      ["bookstore", "art_gallery", "record_shop", "toy_store", "craft_shop", "cultural_space"],
  educacion:    ["language_academy", "tutoring_center", "music_school", "art_school", "cooking_school", "creative_workshop", "coding_academy", "driving_school"],
  servicios:    ["coworking_office", "laundromat", "dry_cleaning", "photo_studio", "pet_grooming", "mobile_repair", "florist", "travel_agency"],
};

// ── Paso 3 — Cliente ideal (mapea a publico_objetivo.estilo_vida) ──────────
const PUBLICO_TILES: Tile[] = [
  { codigo: "jovenes",       label: "Jóvenes / estudiantes",    desc: "Perfil 18-30, poder adquisitivo medio." },
  { codigo: "familias",      label: "Familias",                  desc: "Con niños, consumo recurrente." },
  { codigo: "profesionales", label: "Profesionales y oficinas",  desc: "Oficinas cercanas, tickets medios-altos." },
  { codigo: "turistas",      label: "Turistas / visitantes",     desc: "Zonas con paso turístico." },
  { codigo: "residentes",    label: "Mixto / residentes",        desc: "Sin foco específico, público variado." },
];

// ── Paso 4 — Zona de BCN ────────────────────────────────────────────────────
// Agrupaciones de distritos. "Sin preferencia" deja `distritos` sin setear.
const ZONA_TILES: Array<Tile & { distritos?: string[] }> = [
  { codigo: "centro",       label: "Centro histórico",           desc: "Ciutat Vella, Raval, Gòtic.",            distritos: ["Ciutat Vella"] },
  { codigo: "comercial",    label: "Barrios comerciales clásicos", desc: "Eixample (Passeig de Gràcia, Consell).", distritos: ["Eixample"] },
  { codigo: "emergente",    label: "Zonas emergentes",           desc: "Gràcia, Poblenou, Sant Martí.",          distritos: ["Gràcia", "Sant Martí"] },
  { codigo: "tranquilo",    label: "Barrios tranquilos",         desc: "Sarrià, Les Corts, Horta-Guinardó.",     distritos: ["Sarrià-Sant Gervasi", "Les Corts", "Horta-Guinardó"] },
  { codigo: "indiferente",  label: "Sin preferencia",            desc: "Analizar toda Barcelona.",               distritos: undefined },
];

// ── Paso 5 — Presupuesto alquiler ──────────────────────────────────────────
const PRESUPUESTO_TILES: Array<Tile & { max?: number }> = [
  { codigo: "lt1500",  label: "Hasta 1.500 €",     desc: "Barrios periféricos o locales pequeños.", max: 1500 },
  { codigo: "1500_2500", label: "1.500 – 2.500 €", desc: "Equilibrio precio / flujo.",              max: 2500 },
  { codigo: "2500_4000", label: "2.500 – 4.000 €", desc: "Ejes comerciales consolidados.",          max: 4000 },
  { codigo: "gt4000", label: "4.000 € o más",     desc: "Prime retail, Passeig de Gràcia.",        max: undefined },
  { codigo: "sin_limite", label: "Sin preferencia", desc: "Analizar sin filtro de alquiler.",       max: undefined },
];

export default function QuickQuestionnaire({ onComplete, onBack }: Props) {
  const [step, setStep] = useState<number>(1);
  const [opciones, setOpciones] = useState<OpcionesCuestionarioResponse | null>(null);

  const [sector, setSector] = useState<string>("");
  const [subsector, setSubsector] = useState<string>("");
  const [publico, setPublico] = useState<string>("");
  const [zona, setZona] = useState<string>("");
  const [presupuesto, setPresupuesto] = useState<string>("");

  const [leaving, setLeaving] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // Precargar el catálogo (labels ES de subsectores). Falla silencioso.
  useEffect(() => {
    let cancelled = false;
    api.opcionesCuestionario()
      .then((r) => { if (!cancelled) setOpciones(r); })
      .catch(() => { /* silent: usamos fallback local */ });
    return () => { cancelled = true; };
  }, []);

  // "otro" no tiene lista curada de subsectores — lo saltamos.
  const hasSubsectorStep = sector !== "" && sector !== "otro" && SUBSECTORES_CURADOS[sector];

  // Total de pasos (4 si sector=otro, 5 si no).
  const totalSteps = hasSubsectorStep ? 5 : 4;
  // Paso visual (cuando se salta subsector, el paso 3 lógico es el 2 visual, etc.).
  const visualStep =
    step === 1 ? 1 :
    step === 2 ? 2 :
    hasSubsectorStep ? step : step - 1;

  const canNext = (() => {
    if (step === 1) return sector !== "";
    if (step === 2) return !hasSubsectorStep || subsector !== "";
    if (step === 3) return publico !== "";
    if (step === 4) return zona !== "";
    if (step === 5) return presupuesto !== "";
    return false;
  })();

  const goNext = () => {
    if (!canNext) return;
    if (step === 1 && !hasSubsectorStep) { setStep(3); return; }
    if (step < 5) { setStep(step + 1); return; }
    // Último paso → armamos payload y completamos
    submit();
  };

  const goBack = () => {
    if (step === 1) { onBack(); return; }
    if (step === 3 && !hasSubsectorStep) { setStep(1); return; }
    setStep(step - 1);
  };

  const submit = () => {
    if (submitting) return;
    setSubmitting(true);

    const pe: PerfilEstructurado = { sector };
    if (subsector) pe.subsector = subsector;

    if (publico) {
      pe.publico_objetivo = {
        estilo_vida: [publico],
        horarios_pico: [],
      };
    }

    const zonaTile = ZONA_TILES.find((t) => t.codigo === zona);
    if (zonaTile?.distritos && zonaTile.distritos.length > 0) {
      pe.distritos = zonaTile.distritos;
    }

    const presTile = PRESUPUESTO_TILES.find((t) => t.codigo === presupuesto);
    if (presTile?.max) pe.presupuesto_max = presTile.max;

    setLeaving(true);
    exitTimerRef.current = window.setTimeout(() => onComplete(pe), 260);
  };

  const exitTimerRef = useRef<number | undefined>(undefined);
  useEffect(() => () => { if (exitTimerRef.current) clearTimeout(exitTimerRef.current); }, []);

  // ── Subsector tiles con labels del backend si están disponibles ─────────
  const subsectorTiles: Tile[] = (() => {
    if (!sector || !SUBSECTORES_CURADOS[sector]) return [];
    const codigos = SUBSECTORES_CURADOS[sector];
    const source: SubsectorOpcion[] = opciones?.subsectores_por_sector?.[sector] ?? [];
    const byCodigo = new Map(source.map((s) => [s.codigo, s.label]));
    return codigos.map((c) => ({
      codigo: c,
      label: byCodigo.get(c) ?? c,
    }));
  })();

  // ── Render helpers ──────────────────────────────────────────────────────
  const renderTiles = <T extends Tile>(
    tiles: T[],
    selected: string,
    onPick: (codigo: string) => void,
  ) => (
    <div className={styles.tileGrid}>
      {tiles.map((t) => (
        <button
          key={t.codigo}
          type="button"
          className={`${styles.tile} ${selected === t.codigo ? styles.tileActive : ""}`}
          onClick={() => onPick(t.codigo)}
        >
          <div className={styles.tileLabel}>{t.label}</div>
          {t.desc && <div className={styles.tileDesc}>{t.desc}</div>}
        </button>
      ))}
    </div>
  );

  const titleByStep: Record<number, { h: string; sub?: string }> = {
    1: { h: "¿Qué tipo de negocio vas a abrir?",       sub: "Elige la categoría principal." },
    2: { h: "¿Qué concepto exacto?",                    sub: "Elige el que más se parezca a tu idea." },
    3: { h: "¿Quién es tu cliente ideal?",              sub: "El público al que apunta tu negocio." },
    4: { h: "¿Qué zona de Barcelona te encaja?",        sub: "Si no sabes, elige \"Sin preferencia\"." },
    5: { h: "¿Alquiler mensual que puedes asumir?",     sub: "Filtramos zonas dentro de ese rango." },
  };

  const currentTitle = titleByStep[step];

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
          <h1 className={styles.question}>{currentTitle.h}</h1>
          {currentTitle.sub && <p className={styles.lede}>{currentTitle.sub}</p>}
        </div>

        <div className={styles.body}>
          {step === 1 && renderTiles(SECTORES_PRIMARIOS, sector, (c) => {
            setSector(c);
            setSubsector("");
          })}

          {step === 2 && hasSubsectorStep && renderTiles(subsectorTiles, subsector, setSubsector)}

          {step === 3 && renderTiles(PUBLICO_TILES, publico, setPublico)}

          {step === 4 && renderTiles(ZONA_TILES, zona, setZona)}

          {step === 5 && renderTiles(PRESUPUESTO_TILES, presupuesto, setPresupuesto)}
        </div>
      </main>

      <footer className={styles.footer}>
        <button type="button" onClick={goBack} className={styles.btnGhost} disabled={submitting}>
          ← {step === 1 ? "Volver" : "Atrás"}
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
