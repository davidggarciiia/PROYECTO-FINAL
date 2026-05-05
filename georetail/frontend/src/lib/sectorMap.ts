/**
 * sectorMap.ts — Fuente única de verdad para la taxonomía sector/subsector del frontend.
 *
 * Mapea sectores UI → códigos backend, infiere streams económicos,
 * y expone placeholders dinámicos por subsector.
 *
 * Backend slugs válidos (taxonomy.py): restauracion, moda, estetica, tatuajes,
 * shisha_lounge, salud, deporte, educacion, alimentacion, servicios, otro
 */

export type StreamType = "space" | "traffic" | "labor" | "asset";

export interface SectorDef {
  codigo:      string;
  label:       string;
  desc:        string;
  backendCode: string;
  streams:     StreamType[];
}

export interface SubsectorDef {
  label:       string;
  placeholder: string;
  /** Código canónico en scoring/taxonomia.py SUBSECTORES. undefined si no hay equivalente exacto. */
  backendCode?: string;
}

export const SECTORES: SectorDef[] = [
  {
    codigo:      "hosteleria",
    label:       "Hostelería",
    desc:        "Negocios de comida y bebida para consumo en local o para llevar",
    backendCode: "restauracion",
    streams:     ["space", "traffic"],
  },
  {
    codigo:      "retail",
    label:       "Retail",
    desc:        "Venta directa de productos al cliente final (ej: supermercado, tienda de ropa, electrónica)",
    backendCode: "otro",          // overridden per subsector via getBackendSector()
    streams:     ["traffic", "space"],
  },
  {
    codigo:      "servicios_personales",
    label:       "Servicios personales",
    desc:        "Servicios basados en el trabajo directo con clientes (ej: peluquería, fisioterapia)",
    backendCode: "estetica",
    streams:     ["labor", "space"],
  },
  {
    codigo:      "servicios_b2b",
    label:       "Servicios B2B",
    desc:        "Servicios dirigidos a otras empresas (consultoría, marketing, software)",
    backendCode: "servicios",
    streams:     ["labor"],
  },
  {
    codigo:      "industria",
    label:       "Industria",
    desc:        "Producción o fabricación de productos",
    backendCode: "otro",
    streams:     ["space", "labor"],
  },
  {
    codigo:      "activos",
    label:       "Activos",
    desc:        "Negocios que generan ingresos mediante máquinas o unidades físicas",
    backendCode: "otro",
    streams:     ["asset"],
  },
  {
    codigo:      "inmobiliario",
    label:       "Inmobiliario",
    desc:        "Negocios basados en el uso o alquiler de espacios",
    backendCode: "otro",
    streams:     ["asset", "space"],
  },
  {
    codigo:      "ecommerce",
    label:       "Ecommerce",
    desc:        "Venta de productos o servicios a través de internet",
    backendCode: "otro",
    streams:     ["traffic", "labor"],
  },
  {
    codigo:      "logistica",
    label:       "Logística",
    desc:        "Transporte, distribución o entrega de productos",
    backendCode: "otro",
    streams:     ["traffic", "asset"],
  },
  {
    codigo:      "ocio",
    label:       "Ocio",
    desc:        "Negocios enfocados en entretenimiento y experiencias",
    backendCode: "otro",
    streams:     ["space", "traffic"],
  },
];

// Subsectores con placeholder dinámico por tipo de negocio.
// backendCode mapea al código canónico de scoring/taxonomia.py SUBSECTORES.
// undefined significa que no hay equivalente exacto en la taxonomía cerrada.
export const SUBSECTORES: Record<string, SubsectorDef[]> = {
  hosteleria: [
    { label: "Restaurante",  placeholder: "japonés, de tapas, vegano, de autor, italiano..." },
    { label: "Cafetería",    placeholder: "café de especialidad, brunch, pastelería artesanal...", backendCode: "cafeteria" },
    { label: "Fast food",    placeholder: "burger, pizza, kebab, poke bowl, pollo...",            backendCode: "fast_food" },
    { label: "Takeaway",     placeholder: "asiático, sushi, bocadillos gourmet, wraps...",         backendCode: "fast_food" },
    { label: "Panadería",    placeholder: "artesana, ecológica, con obrador, masa madre...",       backendCode: "panaderia" },
    { label: "Dark kitchen", placeholder: "delivery italiano, asiático, hamburguesas premium...",  backendCode: "fast_food" },
    { label: "Buffet",       placeholder: "asiático, mediterráneo, all-you-can-eat, precio fijo..." },
  ],
  retail: [
    { label: "Alimentación",  placeholder: "ecológica, gourmet, productos locales, zero waste..." },
    { label: "Moda",          placeholder: "streetwear, vintage, lujo asequible, moda infantil..." },
    { label: "Electrónica",   placeholder: "gaming, Apple reseller, reparación, segunda mano..." },
    { label: "Hogar",         placeholder: "decoración nórdica, vintage, sostenible, interiorismo..." },
    { label: "Especializado", placeholder: "deporte, mascotas, papelería premium, instrumentos..." },
    { label: "Segunda mano",  placeholder: "ropa, muebles, tecnología, libros, coleccionismo...",  backendCode: "vintage" },
    { label: "Farmacia",      placeholder: "con parafarmacia, nutrición deportiva, homeopatía..." },
    { label: "Nicho",         placeholder: "magic cards, discos de vinilo, figuras, comics..." },
  ],
  servicios_personales: [
    { label: "Peluquería",        placeholder: "unisex, coloristas, extensiones, premium, de barrio...",    backendCode: "hair_salon" },
    { label: "Barbería",          placeholder: "hipster, fade, barba, premium, afeitado clásico...",         backendCode: "barber_shop" },
    { label: "Estética y belleza",placeholder: "micropigmentación, láser, uñas, spa, lashes..." },
    { label: "Peluquería canina", placeholder: "aseo de mascotas, dog grooming, baño y corte...",            backendCode: "pet_grooming" },
    { label: "Fisioterapia",      placeholder: "deportiva, neurológica, osteopatía, pilates terapéutico..." },
    { label: "Psicología",        placeholder: "cognitiva, infantil, de pareja, EMDR, online..." },
    { label: "Clínica dental",    placeholder: "estética, ortodoncia invisible, implantes, infantil..." },
    { label: "Yoga / pilates",    placeholder: "hot yoga, reformer, meditación, pre-natal, ashtanga...",     backendCode: "spa" },
    { label: "Otro",              placeholder: "describe el servicio personalizado que ofreces..." },
  ],
  servicios_b2b: [
    { label: "Consultoría",            placeholder: "estratégica, financiera, operacional, digital..." },
    { label: "Marketing y publicidad", placeholder: "digital, branding, performance, redes, SEO..." },
    { label: "Software / Tech",        placeholder: "SaaS, apps móviles, IA, automatización, web..." },
    { label: "Diseño",                 placeholder: "gráfico, UX/UI, arquitectura de interiores, industrial..." },
    { label: "Recursos humanos",       placeholder: "selección, headhunting, formación, employer branding..." },
    { label: "Legal / asesoría",       placeholder: "mercantil, laboral, fiscal, inmobiliario, startup..." },
    { label: "Otro",                   placeholder: "describe el servicio B2B que ofreces..." },
  ],
  industria: [
    { label: "Manufactura",  placeholder: "electrónica, plástico, metal, carpintería, cerámica..." },
    { label: "Artesanía",    placeholder: "joyería, cerámica, textil artesanal, cuero, vidrio..." },
    { label: "Alimentaria",  placeholder: "conservas, bebidas artesanales, repostería, quesos..." },
    { label: "Textil",       placeholder: "patronaje, confección, moda técnica, uniformes..." },
    { label: "Laboratorio",  placeholder: "química, biotecnología, cosmética, suplementos..." },
    { label: "Otro",         placeholder: "describe qué produces o fabricas..." },
  ],
  activos: [
    { label: "Vending / máquinas", placeholder: "snacks, café, lavado de coches, recarga eléctrica..." },
    { label: "Lavandería",         placeholder: "autoservicio 24h, tintorería exprés, industrial..." },
    { label: "Parking / almacén",  placeholder: "motos, bicicletas, trasteros, mini almacenes..." },
    { label: "Energía / paneles",  placeholder: "comunidades energéticas, instalación fotovoltaica..." },
    { label: "Otro",               placeholder: "describe el activo físico que genera ingresos..." },
  ],
  inmobiliario: [
    { label: "Alquiler residencial", placeholder: "larga estancia, media estancia, estudiantes, reubicados..." },
    { label: "Alquiler comercial",   placeholder: "oficinas flex, locales comerciales, naves ligeras..." },
    { label: "Coworking",            placeholder: "creativo, tecnológico, flexible, por horas, community..." },
    { label: "Turístico / Airbnb",   placeholder: "apartamentos turísticos, rooms, boutique hotel..." },
    { label: "Naves industriales",   placeholder: "logística, dark kitchen, taller, e-commerce..." },
    { label: "Otro",                 placeholder: "describe el tipo de espacio que gestionas..." },
  ],
  ecommerce: [
    { label: "Productos físicos",  placeholder: "ropa, electrónica, hogar, deporte, alimentación..." },
    { label: "Dropshipping",       placeholder: "nicho de producto, marca propia, print on demand..." },
    { label: "Marketplace",        placeholder: "artesanía local, segunda mano, servicios profesionales..." },
    { label: "Suscripción",        placeholder: "cajas temáticas, software, contenido digital, comunidad..." },
    { label: "Digital / Software", placeholder: "cursos online, apps, plantillas, herramientas IA..." },
    { label: "Otro",               placeholder: "describe qué vendes por internet..." },
  ],
  logistica: [
    { label: "Mensajería / paquetería",  placeholder: "última milla, urgente, para e-commerce, frío..." },
    { label: "Transporte de mercancías", placeholder: "nacional, frigorífico, paletizado, ADR..." },
    { label: "Última milla",             placeholder: "bici cargo, moto eléctrica, cero emisiones, zonas bcn..." },
    { label: "Almacenamiento",           placeholder: "fulfillment, cross-docking, 3PL, pick & pack..." },
    { label: "Otro",                     placeholder: "describe el servicio logístico..." },
  ],
  ocio: [
    { label: "Gimnasio / fitness",   placeholder: "funcional, boxeo, orientado a mujer, 24h, low cost..." },
    { label: "Centro deportivo",     placeholder: "pádel, climbing, natación, multideporte, padel indoor..." },
    { label: "Academia / escuela",   placeholder: "idiomas, música, danza, programación, cocina..." },
    { label: "Sala de eventos",      placeholder: "bodas, corporativos, conciertos, microteatro, fiestas..." },
    { label: "Escape room / gaming", placeholder: "realidad virtual, laser tag, arcade, board games..." },
    { label: "Otro",                 placeholder: "describe la experiencia o entretenimiento..." },
  ],
};

// Subsectors que usan un sector backend distinto al default del sector UI padre.
const RETAIL_BACKEND: Record<string, string> = {
  "Alimentación": "alimentacion",
  "Moda":         "moda",
  "Farmacia":     "salud",
};

const SERVICIOS_PERSONALES_BACKEND: Record<string, string> = {
  "Peluquería canina": "servicios",
  "Fisioterapia":      "salud",
  "Psicología":        "salud",
  "Clínica dental":    "salud",
};

// ── Public helpers ────────────────────────────────────────────────────────────

export function getSectorDef(codigo: string): SectorDef | undefined {
  return SECTORES.find((s) => s.codigo === codigo);
}

export function getSubsectorList(sectorCodigo: string): SubsectorDef[] {
  return SUBSECTORES[sectorCodigo] ?? [];
}

export function getBackendSector(sectorCodigo: string, subsectorLabel: string): string {
  if (sectorCodigo === "retail") {
    return RETAIL_BACKEND[subsectorLabel] ?? "otro";
  }
  if (sectorCodigo === "servicios_personales" && subsectorLabel in SERVICIOS_PERSONALES_BACKEND) {
    return SERVICIOS_PERSONALES_BACKEND[subsectorLabel];
  }
  return getSectorDef(sectorCodigo)?.backendCode ?? "otro";
}

export function inferStreams(sectorCodigo: string): StreamType[] {
  return getSectorDef(sectorCodigo)?.streams ?? ["traffic"];
}

export function getSubsectorPlaceholder(sectorCodigo: string, subsectorLabel: string): string {
  const subs = SUBSECTORES[sectorCodigo] ?? [];
  return (
    subs.find((s) => s.label === subsectorLabel)?.placeholder ??
    "Ej: restaurante japonés, bar de tapas, tienda de informática gaming..."
  );
}

/**
 * Devuelve el código canónico de taxonomia.py para el subsector seleccionado.
 * undefined si no hay equivalente exacto en la taxonomía cerrada del backend.
 */
export function getBackendSubsector(sectorCodigo: string, subsectorLabel: string): string | undefined {
  const subs = SUBSECTORES[sectorCodigo] ?? [];
  return subs.find((s) => s.label === subsectorLabel)?.backendCode;
}
