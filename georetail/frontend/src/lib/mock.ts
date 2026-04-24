import type {
  BuscarResponse,
  CompetenciaDetalle,
  ExplicacionDimension,
  FinancieroResponse,
  ImpactoModeloDimension,
  LegalRoadmapResponse,
  LocalDetalleResponse,
} from "./types";

export const MOCK_SESSION_ID = "mock-session-001";

type MockZona = NonNullable<BuscarResponse["zonas"]>[number];

const FEATURED_ZONES: MockZona[] = [
  {
    zona_id: "gracia-diagonal",
    nombre: "C/ Diagonal Gràcia",
    barrio: "El Camp de Gràcia",
    distrito: "Gràcia",
    score_global: 60,
    probabilidad_supervivencia_3a: 0.69,
    alquiler_estimado: 1900,
    m2_disponibles: 70,
    color: "amarillo",
    lat: 41.3985,
    lng: 2.1587,
    resumen_ia: "Zona con buen entorno comercial pero flujo peatonal bajo.",
  },
  {
    zona_id: "eixample-consell-cent",
    nombre: "Consell de Cent",
    barrio: "L'Antiga Esquerra de l'Eixample",
    distrito: "Eixample",
    score_global: 82,
    probabilidad_supervivencia_3a: 0.84,
    alquiler_estimado: 2800,
    m2_disponibles: 95,
    color: "verde",
    lat: 41.3888,
    lng: 2.1551,
    resumen_ia: "Alta densidad comercial y excelente tráfico peatonal.",
  },
  {
    zona_id: "poble-nou-rambla",
    nombre: "Rambla del Poblenou",
    barrio: "El Poblenou",
    distrito: "Sant Martí",
    score_global: 77,
    probabilidad_supervivencia_3a: 0.79,
    alquiler_estimado: 1650,
    m2_disponibles: 80,
    color: "verde",
    lat: 41.4015,
    lng: 2.1978,
    resumen_ia: "Zona emergente con perfil joven y menor saturación directa.",
  },
];

// Semilla determinista para generar ~100 zonas de relleno por Barcelona.
// No pretende ser real — los nombres de calle/barrio/distrito vienen de una
// lista acotada y el resto (score, alquiler, m2, color, lat/lng) se deriva.
const STREET_SEEDS: Array<{
  nombre: string;
  barrio: string;
  distrito: string;
  lat: number;
  lng: number;
}> = [
  { nombre: "Passeig de Gràcia",        barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3933, lng: 2.1649 },
  { nombre: "Rambla de Catalunya",      barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3917, lng: 2.1641 },
  { nombre: "Gran de Gràcia",           barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4022, lng: 2.1566 },
  { nombre: "Verdi",                    barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4056, lng: 2.1582 },
  { nombre: "Travessera de Gràcia",     barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4010, lng: 2.1560 },
  { nombre: "Torrent de l'Olla",        barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4035, lng: 2.1592 },
  { nombre: "Avinguda Diagonal",        barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.3948, lng: 2.1400 },
  { nombre: "Muntaner",                 barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.3933, lng: 2.1430 },
  { nombre: "Balmes",                   barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.3945, lng: 2.1460 },
  { nombre: "Aribau",                   barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3890, lng: 2.1550 },
  { nombre: "Enric Granados",           barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3888, lng: 2.1586 },
  { nombre: "Rocafort",                 barrio: "La Nova Esquerra Eixample",  distrito: "Eixample",         lat: 41.3820, lng: 2.1507 },
  { nombre: "Comte d'Urgell",           barrio: "La Nova Esquerra Eixample",  distrito: "Eixample",         lat: 41.3832, lng: 2.1510 },
  { nombre: "Diputació",                barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3907, lng: 2.1680 },
  { nombre: "Girona",                   barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3944, lng: 2.1728 },
  { nombre: "Avinguda Meridiana",       barrio: "La Sagrera",                 distrito: "Sant Andreu",      lat: 41.4222, lng: 2.1860 },
  { nombre: "Carrer Gran de Sant Andreu", barrio: "Sant Andreu de Palomar",   distrito: "Sant Andreu",      lat: 41.4351, lng: 2.1903 },
  { nombre: "Nou Barris · Fabra i Puig",barrio: "Porta",                      distrito: "Nou Barris",       lat: 41.4345, lng: 2.1766 },
  { nombre: "Via Júlia",                barrio: "Les Roquetes",               distrito: "Nou Barris",       lat: 41.4416, lng: 2.1723 },
  { nombre: "Passeig Maragall",         barrio: "El Congrés i els Indians",   distrito: "Sant Andreu",      lat: 41.4213, lng: 2.1776 },
  { nombre: "Travessera de les Corts",  barrio: "Les Corts",                  distrito: "Les Corts",        lat: 41.3858, lng: 2.1276 },
  { nombre: "Gran Via de Carles III",   barrio: "Pedralbes",                  distrito: "Les Corts",        lat: 41.3870, lng: 2.1175 },
  { nombre: "Carrer de Sants",          barrio: "Sants",                      distrito: "Sants-Montjuïc",   lat: 41.3744, lng: 2.1390 },
  { nombre: "Creu Coberta",             barrio: "Hostafrancs",                distrito: "Sants-Montjuïc",   lat: 41.3755, lng: 2.1435 },
  { nombre: "Paral·lel",                barrio: "El Poble-sec",               distrito: "Sants-Montjuïc",   lat: 41.3741, lng: 2.1611 },
  { nombre: "Blai",                     barrio: "El Poble-sec",               distrito: "Sants-Montjuïc",   lat: 41.3727, lng: 2.1647 },
  { nombre: "Ronda Sant Antoni",        barrio: "Sant Antoni",                distrito: "Eixample",         lat: 41.3803, lng: 2.1634 },
  { nombre: "Comte Borrell",            barrio: "Sant Antoni",                distrito: "Eixample",         lat: 41.3815, lng: 2.1575 },
  { nombre: "Tamarit",                  barrio: "Sant Antoni",                distrito: "Eixample",         lat: 41.3789, lng: 2.1580 },
  { nombre: "Rambla del Raval",         barrio: "El Raval",                   distrito: "Ciutat Vella",     lat: 41.3795, lng: 2.1695 },
  { nombre: "Hospital",                 barrio: "El Raval",                   distrito: "Ciutat Vella",     lat: 41.3805, lng: 2.1710 },
  { nombre: "La Rambla",                barrio: "El Gòtic",                   distrito: "Ciutat Vella",     lat: 41.3809, lng: 2.1734 },
  { nombre: "Ferran",                   barrio: "El Gòtic",                   distrito: "Ciutat Vella",     lat: 41.3810, lng: 2.1754 },
  { nombre: "Portal de l'Àngel",        barrio: "El Gòtic",                   distrito: "Ciutat Vella",     lat: 41.3846, lng: 2.1732 },
  { nombre: "Via Laietana",             barrio: "Sant Pere · Santa Caterina", distrito: "Ciutat Vella",     lat: 41.3847, lng: 2.1788 },
  { nombre: "Argenteria",               barrio: "Sant Pere · Santa Caterina", distrito: "Ciutat Vella",     lat: 41.3838, lng: 2.1808 },
  { nombre: "Passeig del Born",         barrio: "La Ribera",                  distrito: "Ciutat Vella",     lat: 41.3839, lng: 2.1836 },
  { nombre: "Princesa",                 barrio: "La Ribera",                  distrito: "Ciutat Vella",     lat: 41.3848, lng: 2.1813 },
  { nombre: "Passeig Joan de Borbó",    barrio: "La Barceloneta",             distrito: "Ciutat Vella",     lat: 41.3811, lng: 2.1876 },
  { nombre: "Passeig Marítim",          barrio: "La Barceloneta",             distrito: "Ciutat Vella",     lat: 41.3775, lng: 2.1950 },
  { nombre: "Avinguda Icària",          barrio: "La Vila Olímpica",           distrito: "Sant Martí",       lat: 41.3898, lng: 2.1987 },
  { nombre: "Rambla del Poblenou",      barrio: "El Poblenou",                distrito: "Sant Martí",       lat: 41.4024, lng: 2.2000 },
  { nombre: "Pere IV",                  barrio: "El Poblenou",                distrito: "Sant Martí",       lat: 41.4052, lng: 2.1953 },
  { nombre: "Marina",                   barrio: "El Parc i la Llacuna",       distrito: "Sant Martí",       lat: 41.4015, lng: 2.1902 },
  { nombre: "Bac de Roda",              barrio: "El Poblenou",                distrito: "Sant Martí",       lat: 41.4112, lng: 2.2013 },
  { nombre: "Guipúscoa",                barrio: "El Besòs i el Maresme",      distrito: "Sant Martí",       lat: 41.4250, lng: 2.2088 },
  { nombre: "Rambla de Prim",           barrio: "El Besòs i el Maresme",      distrito: "Sant Martí",       lat: 41.4180, lng: 2.2116 },
  { nombre: "Mallorca",                 barrio: "La Sagrada Família",         distrito: "Eixample",         lat: 41.4031, lng: 2.1760 },
  { nombre: "Provença",                 barrio: "La Sagrada Família",         distrito: "Eixample",         lat: 41.4022, lng: 2.1739 },
  { nombre: "Sardenya",                 barrio: "La Sagrada Família",         distrito: "Eixample",         lat: 41.4060, lng: 2.1790 },
  { nombre: "Lesseps",                  barrio: "Vallcarca i els Penitents",  distrito: "Gràcia",           lat: 41.4100, lng: 2.1510 },
  { nombre: "República Argentina",      barrio: "El Coll",                    distrito: "Gràcia",           lat: 41.4103, lng: 2.1485 },
  { nombre: "Passeig de Sant Joan",     barrio: "Camp d'en Grassot",          distrito: "Gràcia",           lat: 41.4028, lng: 2.1709 },
  { nombre: "Pau Claris",               barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3922, lng: 2.1690 },
  { nombre: "Roger de Llúria",          barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3927, lng: 2.1714 },
  { nombre: "Bruc",                     barrio: "Dreta de l'Eixample",        distrito: "Eixample",         lat: 41.3950, lng: 2.1727 },
  { nombre: "Marià Cubí",               barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.3999, lng: 2.1466 },
  { nombre: "Via Augusta",              barrio: "Sant Gervasi – la Bonanova", distrito: "Sarrià-Sant Gervasi", lat: 41.4025, lng: 2.1375 },
  { nombre: "Bonanova",                 barrio: "Sant Gervasi – la Bonanova", distrito: "Sarrià-Sant Gervasi", lat: 41.4058, lng: 2.1338 },
  { nombre: "Major de Sarrià",          barrio: "Sarrià",                     distrito: "Sarrià-Sant Gervasi", lat: 41.4006, lng: 2.1199 },
  { nombre: "Numància",                 barrio: "Les Corts",                  distrito: "Les Corts",        lat: 41.3888, lng: 2.1322 },
  { nombre: "Josep Tarradellas",        barrio: "Sants · Badal",              distrito: "Sants-Montjuïc",   lat: 41.3895, lng: 2.1383 },
  { nombre: "Entença",                  barrio: "La Nova Esquerra Eixample",  distrito: "Eixample",         lat: 41.3838, lng: 2.1415 },
  { nombre: "Villarroel",               barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3868, lng: 2.1530 },
  { nombre: "Casanova",                 barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3877, lng: 2.1513 },
  { nombre: "València",                 barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3900, lng: 2.1568 },
  { nombre: "Londres",                  barrio: "La Nova Esquerra Eixample",  distrito: "Eixample",         lat: 41.3870, lng: 2.1452 },
  { nombre: "Av. Mistral",              barrio: "Sant Antoni",                distrito: "Eixample",         lat: 41.3772, lng: 2.1540 },
  { nombre: "Av. Roma",                 barrio: "Sant Antoni",                distrito: "Eixample",         lat: 41.3811, lng: 2.1485 },
  { nombre: "Rambla Brasil",            barrio: "Sants",                      distrito: "Sants-Montjuïc",   lat: 41.3775, lng: 2.1332 },
  { nombre: "Riera Blanca",             barrio: "Sants · Badal",              distrito: "Sants-Montjuïc",   lat: 41.3781, lng: 2.1258 },
  { nombre: "Plaça del Sol",            barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4028, lng: 2.1551 },
  { nombre: "Plaça de la Virreina",     barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4050, lng: 2.1579 },
  { nombre: "Asturies",                 barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4040, lng: 2.1539 },
  { nombre: "Ros de Olano",             barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4037, lng: 2.1560 },
  { nombre: "Pi i Margall",             barrio: "La Salut",                   distrito: "Gràcia",           lat: 41.4093, lng: 2.1620 },
  { nombre: "Dante Alighieri",          barrio: "El Carmel",                  distrito: "Horta-Guinardó",   lat: 41.4276, lng: 2.1561 },
  { nombre: "Ronda del Guinardó",       barrio: "El Guinardó",                distrito: "Horta-Guinardó",   lat: 41.4191, lng: 2.1655 },
  { nombre: "Horta · Tajo",             barrio: "Horta",                      distrito: "Horta-Guinardó",   lat: 41.4321, lng: 2.1636 },
  { nombre: "Av. Borbó",                barrio: "El Baix Guinardó",           distrito: "Horta-Guinardó",   lat: 41.4146, lng: 2.1706 },
  { nombre: "Pg. Valldaura",            barrio: "Vilapicina",                 distrito: "Nou Barris",       lat: 41.4308, lng: 2.1780 },
  { nombre: "Pg. Santa Coloma",         barrio: "Trinitat Vella",             distrito: "Sant Andreu",      lat: 41.4482, lng: 2.1900 },
  { nombre: "Pg. Fabra i Puig",         barrio: "Porta",                      distrito: "Nou Barris",       lat: 41.4336, lng: 2.1749 },
  { nombre: "Pg. Urrutia",              barrio: "Les Roquetes",               distrito: "Nou Barris",       lat: 41.4421, lng: 2.1736 },
  { nombre: "Santa Caterina",           barrio: "Sant Pere · Santa Caterina", distrito: "Ciutat Vella",     lat: 41.3866, lng: 2.1804 },
  { nombre: "Allada-Vermell",           barrio: "La Ribera",                  distrito: "Ciutat Vella",     lat: 41.3851, lng: 2.1826 },
  { nombre: "Rec",                      barrio: "La Ribera",                  distrito: "Ciutat Vella",     lat: 41.3835, lng: 2.1845 },
  { nombre: "Còrsega",                  barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3944, lng: 2.1560 },
  { nombre: "Rosselló",                 barrio: "L'Antiga Esquerra Eixample", distrito: "Eixample",         lat: 41.3960, lng: 2.1576 },
  { nombre: "Mandri",                   barrio: "Sant Gervasi – la Bonanova", distrito: "Sarrià-Sant Gervasi", lat: 41.4043, lng: 2.1394 },
  { nombre: "Cardedeu",                 barrio: "La Guineueta",               distrito: "Nou Barris",       lat: 41.4395, lng: 2.1821 },
  { nombre: "Doctor Pi i Molist",       barrio: "Vilapicina",                 distrito: "Nou Barris",       lat: 41.4325, lng: 2.1799 },
  { nombre: "Estadi Olímpic",           barrio: "La Marina del Prat Vermell", distrito: "Sants-Montjuïc",   lat: 41.3647, lng: 2.1567 },
  { nombre: "Doctor Marañón",           barrio: "Pedralbes",                  distrito: "Les Corts",        lat: 41.3861, lng: 2.1157 },
  { nombre: "Av. Madrid",               barrio: "Les Corts",                  distrito: "Les Corts",        lat: 41.3800, lng: 2.1256 },
  { nombre: "Plaça Joanic",             barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4064, lng: 2.1638 },
  { nombre: "Plaça del Diamant",        barrio: "Vila de Gràcia",             distrito: "Gràcia",           lat: 41.4036, lng: 2.1580 },
  { nombre: "Encants Vells",            barrio: "El Fort Pienc",              distrito: "Eixample",         lat: 41.4020, lng: 2.1854 },
  { nombre: "Glòries",                  barrio: "El Parc i la Llacuna",       distrito: "Sant Martí",       lat: 41.4045, lng: 2.1868 },
  { nombre: "22@ · Llacuna",            barrio: "El Parc i la Llacuna",       distrito: "Sant Martí",       lat: 41.4020, lng: 2.1973 },
  { nombre: "Avinguda Diagonal Mar",    barrio: "Diagonal Mar",               distrito: "Sant Martí",       lat: 41.4097, lng: 2.2180 },
  { nombre: "Plaça del Centre",         barrio: "Les Corts",                  distrito: "Les Corts",        lat: 41.3849, lng: 2.1322 },
  { nombre: "Plaça Adrià",              barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.3973, lng: 2.1415 },
  { nombre: "Plaça Molina",             barrio: "Sant Gervasi – Galvany",     distrito: "Sarrià-Sant Gervasi", lat: 41.4015, lng: 2.1445 },
  { nombre: "Plaça Osca",               barrio: "Sants",                      distrito: "Sants-Montjuïc",   lat: 41.3742, lng: 2.1376 },
];

function colorFromScore(score: number): "verde" | "amarillo" | "rojo" {
  if (score >= 75) return "verde";
  if (score >= 50) return "amarillo";
  return "rojo";
}

function buildExtraZones(): MockZona[] {
  const out: MockZona[] = [];
  for (let i = 0; i < STREET_SEEDS.length; i++) {
    const s = STREET_SEEDS[i];
    // Generador determinista: cada seed fija score/alquiler/m2 siempre igual.
    const rot  = (i * 1103515245 + 12345) & 0x7fffffff;
    const r1   = (rot >> 3)  % 100;   // 0-99
    const r2   = (rot >> 7)  % 100;
    const r3   = (rot >> 11) % 100;
    const score = 40 + Math.round((r1 / 100) * 52);         // 40-92
    const alquiler = 1100 + Math.round((r2 / 100) * 2700);  // 1100-3800
    const m2 = 40 + Math.round((r3 / 100) * 140);           // 40-180
    const prob = Math.round(((score / 100) * 0.82 + 0.12) * 100) / 100;
    const slug = s.nombre
      .normalize("NFD")
      .replace(/[^\w\s-]/g, "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "-")
      .slice(0, 40);
    out.push({
      zona_id: `bcn-${i.toString().padStart(3, "0")}-${slug}`,
      nombre: s.nombre,
      barrio: s.barrio,
      distrito: s.distrito,
      score_global: score,
      probabilidad_supervivencia_3a: prob,
      alquiler_estimado: alquiler,
      m2_disponibles: m2,
      color: colorFromScore(score),
      lat: s.lat,
      lng: s.lng,
      resumen_ia: `Zona ${s.distrito.toLowerCase()} · ${s.barrio} — perfil derivado de datos demográficos y comerciales.`,
    });
  }
  return out;
}

const EXTRA_ZONES: MockZona[] = buildExtraZones();

export const MOCK_BUSCAR: BuscarResponse = {
  estado: "ok",
  session_id: MOCK_SESSION_ID,
  total_zonas_analizadas: 2847,
  zonas: [...FEATURED_ZONES, ...EXTRA_ZONES],
};

function dim(
  score: number,
  titular: string,
  explicacion_corta: string,
  porque_sube: string[],
  porque_baja: string[],
  hechos_clave: string[],
  impacto_modelo: string,
  fuentes: string[],
  confianza = "alta"
): ExplicacionDimension {
  return {
    score,
    titular,
    explicacion_corta,
    porque_sube,
    porque_baja,
    hechos_clave,
    impacto_modelo,
    confianza,
    fuentes,
  };
}

function impact(
  contribucion: number,
  tendencia: string,
  top_features: ImpactoModeloDimension["top_features"]
): ImpactoModeloDimension {
  return { contribucion, tendencia, top_features };
}

function buildBaseExplanations(): Record<string, ExplicacionDimension> {
  return {
    flujo_peatonal: dim(
      32,
      "Flujo discreto y más fuerte por la tarde",
      "La calle funciona mejor para negocios de destino o cita que para tráfico impulsivo durante todo el día.",
      ["La tarde concentra el mayor volumen peatonal.", "El eje comercial mantiene actividad útil en horario comercial."],
      ["La mañana se queda corta para captación espontánea.", "No depende del turismo ni del gran paso de ciudad."],
      ["497 personas/hora por la mañana", "1.324 personas/hora por la tarde", "589 personas/hora por la noche"],
      "Empuja ligeramente en contra por baja captación espontánea.",
      ["Sensores municipales", "Popular times agregados"]
    ),
    demografia: dim(
      56,
      "Demografía correcta, sin ser diferencial",
      "La base residencial es suficiente para sostener un negocio especializado, aunque no marca una ventaja extraordinaria por sí sola.",
      ["Renta media adecuada.", "Edad media compatible con consumo recurrente."],
      ["La señal es más estable que sobresaliente."],
      ["Renta media del hogar: 38.500 €", "Edad media: 36 años", "Población extranjera: 18%"],
      "Aporta soporte más que impulso principal.",
      ["Barcelona Open Data", "Padrón municipal"]
    ),
    competencia: dim(
      84,
      "Competencia manejable con espacio para entrar",
      "Hay oferta parecida en el entorno, pero la presión directa sigue siendo razonable y deja margen a una propuesta bien diferenciada.",
      ["Solo dos competidores directos cerca.", "Hay masa comercial alrededor que también genera demanda."],
      ["La cercanía de dos estudios obliga a diferenciar bien el posicionamiento."],
      ["2 competidores directos a menos de 400 m"],
      "Empuja a favor porque el mercado no está cerrado.",
      ["Google Places", "Censo comercial BCN"]
    ),
    precio_alquiler: dim(
      78,
      "Coste de entrada razonable para el barrio",
      "El alquiler está por debajo de zonas más tensas y mantiene un mejor equilibrio entre coste y potencial comercial.",
      ["1.900 €/mes es competitivo para esta zona.", "70 m² permiten una operación compacta."],
      ["Sigue siendo un barrio exigente en ventas si se quiere escalar."],
      ["1.900 €/mes", "70 m²", "27 €/m² aprox."],
      "Empuja a favor por equilibrio coste-potencial.",
      ["Oferta comercial agregada"]
    ),
    transporte: dim(
      97,
      "Accesibilidad excelente",
      "El local se beneficia de una conectividad muy alta y una llegada cómoda desde varios puntos de la ciudad.",
      ["Varias líneas y paradas cercanas.", "Acceso fácil para clientela de cita."],
      [],
      ["6 líneas de transporte", "4 paradas cercanas"],
      "Empuja claramente a favor.",
      ["TMB", "OpenStreetMap"]
    ),
    seguridad: dim(
      72,
      "Entorno comercial razonablemente estable",
      "La seguridad acompaña y no introduce una penalización fuerte en la recomendación final.",
      ["Entorno urbano consolidado.", "Actividad comercial constante."],
      ["No es una zona especialmente tranquila en todas las franjas."],
      ["Score seguridad: 72"],
      "Aporta tranquilidad, aunque no define la decisión.",
      ["Indicadores municipales de seguridad"],
      "media"
    ),
    turismo: dim(
      10,
      "Zona poco apoyada en turismo",
      "La recomendación descansa casi totalmente en demanda local, no en tráfico visitante.",
      ["Menor dependencia de estacionalidad turística."],
      ["No hay apoyo relevante de cliente visitante.", "No es una zona de captación casual turística."],
      ["Score turismo: 10"],
      "Neutral o ligeramente en contra según el negocio.",
      ["Indicadores turísticos"],
      "media"
    ),
  };
}

function buildBaseImpact(): Record<string, ImpactoModeloDimension> {
  return {
    flujo_peatonal: impact(-0.08, "empuja_en_contra", [
      { feature: "flujo_tarde", valor: 1324, descripcion: "Sostiene la zona en horario útil." },
      { feature: "flujo_manana", valor: 497, descripcion: "Penaliza la captación espontánea." },
    ]),
    demografia: impact(0.04, "neutral", [
      { feature: "renta_media_hogar", valor: 38500, descripcion: "Señal de gasto razonable." },
    ]),
    competencia: impact(0.12, "empuja_a_favor", [
      { feature: "score_competencia", valor: 84, descripcion: "Mercado abierto para entrar." },
    ]),
    precio_alquiler: impact(0.09, "empuja_a_favor", [
      { feature: "alquiler_mensual", valor: 1900, descripcion: "Coste de entrada contenido." },
    ]),
    transporte: impact(0.11, "empuja_a_favor", [
      { feature: "num_lineas_transporte", valor: 6, descripcion: "Accesibilidad cómoda." },
    ]),
    seguridad: impact(0.03, "neutral", []),
    turismo: impact(-0.02, "neutral", []),
  };
}

const BASE_DETAIL: LocalDetalleResponse = {
  zona: {
    zona_id: "gracia-diagonal",
    nombre: "C/ Diagonal Gràcia",
    barrio: "El Camp de Gràcia",
    distrito: "Gràcia",
    lat: 41.3985,
    lng: 2.1587,
    direccion: "Av. Diagonal, 234",
    m2: 70,
    alquiler_mensual: 1900,
    disponible: true,
    score_global: 60,
    scores_dimensiones: {
      flujo_peatonal: 32,
      demografia: 56,
      competencia: 84,
      precio_alquiler: 78,
      transporte: 97,
      seguridad: 72,
      turismo: 10,
      dinamismo: 61,
    },
    probabilidad_supervivencia: 0.69,
    flujo_peatonal_dia: { manana: 497, tarde: 1324, noche: 589 },
    renta_media_hogar: 38500,
    edad_media: 36,
    pct_extranjeros: 18,
    num_negocios_activos: 105,
    pct_locales_vacios: 8,
    num_lineas_transporte: 6,
    num_paradas_transporte: 4,
    competidores_cercanos: [
      {
        nombre: "Tattoo Lab Barcelona",
        sector: "Estudio de tatuajes",
        distancia_m: 120,
        rating: 4.8,
        precio_nivel: 3,
        es_competencia_directa: true,
        num_resenas: 189,
      },
      {
        nombre: "Ink Society",
        sector: "Estudio de tatuajes",
        distancia_m: 340,
        rating: 4.5,
        precio_nivel: 2,
        es_competencia_directa: true,
        num_resenas: 94,
      },
      {
        nombre: "Peluquería Gràcia",
        sector: "Peluquería",
        distancia_m: 80,
        rating: 4.2,
        precio_nivel: 2,
        es_competencia_directa: false,
        es_complementario: true,
      },
    ],
    alertas: [
      {
        tipo: "Saturación sectorial",
        texto: "Existen 2 estudios de tatuajes en un radio de 400 m. La diferenciación será clave.",
        fuente: "Censo de actividades comerciales BCN 2024",
      },
      {
        tipo: "Flujo peatonal bajo",
        texto: "El tráfico matinal es reducido. El mejor horario está entre las 13:00 y las 20:00.",
        fuente: "Sensores municipales de movilidad",
      },
    ],
    analisis_ia: {
      resumen:
        "La zona ofrece una oportunidad intermedia: destaca por acceso, entorno comercial y coste contenido, pero depende mucho de una demanda de destino y no tanto de paso espontáneo.",
      puntos_fuertes: [
        "Entorno comercial muy consolidado",
        "Accesibilidad excelente",
        "Alquiler razonable para el barrio",
      ],
      puntos_debiles: [
        "Flujo peatonal bajo por la mañana",
        "Poca aportación de turismo",
      ],
      oportunidad:
        "Captar clientela local fiel y posicionarse con una propuesta diferenciada en un eje ya activo.",
      riesgos:
        "Depender demasiado de la compra impulsiva o entrar con una propuesta poco diferenciada frente a la competencia cercana.",
      recomendacion_final: "viable_con_condiciones",
      razon_recomendacion:
        "Viable si el negocio sabe generar demanda de destino y aprovechar la accesibilidad y el entorno comercial.",
      resumen_global:
        "Este local encaja porque combina coste de entrada razonable, buena accesibilidad y calle comercial fuerte, aunque necesita una propuesta que no dependa del paso casual.",
      explicaciones_dimensiones: buildBaseExplanations(),
      impacto_modelo_por_dimension: buildBaseImpact(),
    },
    explicaciones_dimensiones: buildBaseExplanations(),
    impacto_modelo_por_dimension: buildBaseImpact(),
    resumen_global_llm:
      "Este local sale bien sobre todo por el equilibrio entre coste, accesibilidad y entorno comercial. No destaca por flujo espontáneo, así que funciona mejor si el negocio sabe generar demanda de destino.",
  },
};

const DETAIL_OVERRIDES: Record<string, Partial<LocalDetalleResponse["zona"]>> = {
  "eixample-consell-cent": {
    zona_id: "eixample-consell-cent",
    nombre: "Consell de Cent",
    barrio: "L'Antiga Esquerra de l'Eixample",
    distrito: "Eixample",
    lat: 41.3888,
    lng: 2.1551,
    m2: 95,
    alquiler_mensual: 2800,
    score_global: 82,
    probabilidad_supervivencia: 0.84,
    scores_dimensiones: {
      flujo_peatonal: 91,
      demografia: 74,
      competencia: 62,
      precio_alquiler: 45,
      transporte: 95,
      seguridad: 88,
      turismo: 70,
      dinamismo: 78,
    },
    flujo_peatonal_dia: { manana: 1820, tarde: 3450, noche: 1240 },
    num_negocios_activos: 287,
    resumen_global_llm:
      "Este local destaca por una calle con muchísimo paso, actividad comercial muy sólida y excelente acceso. La contrapartida es el coste y una competencia más presente.",
    analisis_ia: {
      resumen:
        "Consell de Cent es una calle de altísima tracción comercial. El principal reto no es atraer demanda, sino defender margen y diferenciarse.",
      puntos_fuertes: [
        "Flujo peatonal excepcional",
        "Entorno comercial consolidado",
        "Muy buena conectividad",
      ],
      puntos_debiles: [
        "Alquiler elevado",
        "Mayor presión competitiva que en barrios emergentes",
      ],
      oportunidad: "Convertirse en una referencia visible aprovechando el gran volumen de paso.",
      riesgos: "El coste fijo alto exige una ejecución muy fina desde el inicio.",
      recomendacion_final: "viable",
      razon_recomendacion: "Muy buena opción si el modelo económico soporta una estructura de costes más alta.",
    },
    alertas: [],
  },
  "poble-nou-rambla": {
    zona_id: "poble-nou-rambla",
    nombre: "Rambla del Poblenou",
    barrio: "El Poblenou",
    distrito: "Sant Martí",
    m2: 80,
    alquiler_mensual: 1650,
    score_global: 77,
    probabilidad_supervivencia: 0.79,
    scores_dimensiones: {
      flujo_peatonal: 68,
      demografia: 81,
      competencia: 90,
      precio_alquiler: 82,
      transporte: 74,
      seguridad: 85,
      turismo: 45,
      dinamismo: 81,
    },
    flujo_peatonal_dia: { manana: 890, tarde: 2100, noche: 1450 },
    num_negocios_activos: 143,
    resumen_global_llm:
      "La zona encaja por perfil demográfico joven, baja saturación directa y un coste de entrada más amable. Es una apuesta de crecimiento más que de volumen inmediato.",
    analisis_ia: {
      resumen:
        "Rambla del Poblenou es una opción con recorrido: tiene menos presión directa y un público muy compatible, aunque el volumen es más selectivo.",
      puntos_fuertes: [
        "Competencia muy baja",
        "Demografía joven y afín",
        "Buen equilibrio entre alquiler y potencial",
      ],
      puntos_debiles: [
        "Menos volumen inmediato que grandes ejes centrales",
        "Turismo moderado",
      ],
      oportunidad: "Ser la opción de referencia del barrio antes de que aumente la competencia.",
      riesgos: "Que la ventaja de mercado se reduzca si suben alquileres o llegan nuevos operadores.",
      recomendacion_final: "viable",
      razon_recomendacion: "Excelente opción para posicionarse pronto en una zona todavía abierta.",
    },
    alertas: [
      {
        tipo: "Barrio en transformación",
        texto: "El Poblenou está revalorizándose y los alquileres podrían tensionarse en los próximos años.",
        fuente: "Observatorio de la Vivienda de Barcelona 2024",
      },
    ],
  },
};

const FEATURED_ZONA_IDS = new Set(FEATURED_ZONES.map((z) => z.zona_id));

export function getMockDetalle(zona_id: string): LocalDetalleResponse {
  // Solo las 3 zonas "featured" tienen detalle completo coherente (dimensiones,
  // competidores, análisis IA). Las ~100 zonas generadas son SOLO pin en el
  // mapa — no inventamos competencia ni narrativa, el usuario verá el header
  // con nombre/barrio/alquiler/score y el resto vacío.
  const override = DETAIL_OVERRIDES[zona_id];
  if (override) {
    return {
      zona: {
        ...BASE_DETAIL.zona,
        ...override,
        competidores_cercanos: override.competidores_cercanos ?? BASE_DETAIL.zona.competidores_cercanos,
        alertas: override.alertas ?? BASE_DETAIL.zona.alertas,
        explicaciones_dimensiones: override.explicaciones_dimensiones ?? BASE_DETAIL.zona.explicaciones_dimensiones,
        impacto_modelo_por_dimension: override.impacto_modelo_por_dimension ?? BASE_DETAIL.zona.impacto_modelo_por_dimension,
        analisis_ia: override.analisis_ia
          ? {
              ...BASE_DETAIL.zona.analisis_ia!,
              ...override.analisis_ia,
            }
          : BASE_DETAIL.zona.analisis_ia,
      },
    };
  }

  if (FEATURED_ZONA_IDS.has(zona_id)) return BASE_DETAIL;

  // Zona generada: solo cabecera. Sin dimensiones, competidores ni análisis
  // inventados. Evita incoherencias tipo "restaurante con competencia tatuajes".
  const zonaMock = MOCK_BUSCAR.zonas?.find((z) => z.zona_id === zona_id);
  if (!zonaMock) return BASE_DETAIL;

  return {
    zona: {
      zona_id: zonaMock.zona_id,
      nombre: zonaMock.nombre,
      barrio: zonaMock.barrio,
      distrito: zonaMock.distrito,
      lat: zonaMock.lat,
      lng: zonaMock.lng,
      direccion: zonaMock.nombre,
      m2: zonaMock.m2_disponibles,
      alquiler_mensual: zonaMock.alquiler_estimado,
      disponible: true,
      score_global: zonaMock.score_global,
      probabilidad_supervivencia: zonaMock.probabilidad_supervivencia_3a,
      competidores_cercanos: [],
      alertas: [],
      resumen_global_llm: zonaMock.resumen_ia ?? null,
    },
  };
}

export function getMockCompetencia(zona_id: string): CompetenciaDetalle {
  // Solo featured tienen competencia inventada coherente. El resto devuelve
  // listas vacías para no mezclar sectores (ej. tatuajes sobre restauración).
  if (!FEATURED_ZONA_IDS.has(zona_id) && !(zona_id in DETAIL_OVERRIDES)) {
    return {
      zona_id,
      sector: "",
      radio_m: 500,
      score_competencia: 0,
      score_cluster: 0,
      amenaza_incumbentes: 0,
      oportunidad_mercado: 0,
      score_complementarios: 0,
      num_directos: 0,
      pct_vulnerables: 0,
      hhi_index: 0,
      ratio_complementarios: 0,
      precio_segmento: {
        nivel_dominante: 0,
        etiqueta: "",
        distribucion: {},
        tiene_gap: false,
      },
      amenaza: [],
      oportunidad: [],
      sinergicos: [],
      fuente: "mock",
      datos_calculados: false,
    };
  }

  const zona = getMockDetalle(zona_id).zona;

  return {
    zona_id,
    sector: "estudio_tatuajes",
    radio_m: 500,
    score_competencia: zona.scores_dimensiones?.competencia ?? 74,
    score_cluster: 68,
    amenaza_incumbentes: 42,
    oportunidad_mercado: 71,
    score_complementarios: 63,
    num_directos: zona.competidores_cercanos.filter((c) => c.es_competencia_directa).length,
    pct_vulnerables: 33,
    hhi_index: 0.24,
    ratio_complementarios: 1.4,
    precio_segmento: {
      nivel_dominante: 2,
      etiqueta: "€€",
      distribucion: { 1: 1, 2: 4, 3: 1, 4: 0 },
      tiene_gap: true,
      gap_nivel: 3,
      gap_etiqueta: "€€€",
    },
    amenaza: [
      {
        nombre: "Tattoo Lab Barcelona",
        sector: "Estudio de tatuajes",
        distancia_m: 120,
        rating: 4.8,
        num_resenas: 189,
        precio_nivel: 3,
        es_competencia_directa: true,
        amenaza_score: 82,
      },
      {
        nombre: "Ink Society",
        sector: "Estudio de tatuajes",
        distancia_m: 340,
        rating: 4.5,
        num_resenas: 94,
        precio_nivel: 2,
        es_competencia_directa: true,
        amenaza_score: 64,
      },
    ],
    oportunidad: [
      {
        nombre: "Body Art Studio",
        sector: "Piercing y tatuajes",
        distancia_m: 430,
        rating: 3.9,
        num_resenas: 28,
        precio_nivel: 2,
        es_competencia_directa: true,
        es_vulnerable: true,
        amenaza_score: 29,
      },
    ],
    sinergicos: [
      {
        nombre: "Peluquería Gràcia",
        sector: "Belleza",
        distancia_m: 80,
        rating: 4.2,
        num_resenas: 57,
        precio_nivel: 2,
        es_competencia_directa: false,
        es_complementario: true,
      },
      {
        nombre: "Café Diagonal",
        sector: "Hostelería",
        distancia_m: 55,
        rating: 4.0,
        num_resenas: 143,
        precio_nivel: 1,
        es_competencia_directa: false,
        es_complementario: true,
      },
    ],
    fuente: "mock",
    datos_calculados: true,
  };
}

function buildProyeccion() {
  const meses = [];
  let acumCons = -28000;
  let acumOpt = -28000;

  for (let mes = 1; mes <= 36; mes++) {
    const ramp = Math.min(1, 0.4 + (mes / 36) * 0.7);
    const ingresosCons = 8500 * ramp;
    const ingresosOpt = 12000 * ramp;
    const costesVar = ingresosCons * 0.35;
    const costesFijos = 3800;
    const ebitdaCons = ingresosCons - costesVar - costesFijos;
    const ebitdaOpt = ingresosOpt - ingresosOpt * 0.3 - costesFijos;
    acumCons += ebitdaCons;
    acumOpt += ebitdaOpt;
    meses.push({
      mes,
      ingresos_conservador: ingresosCons,
      ingresos_optimista: ingresosOpt,
      costes_variables: costesVar,
      costes_fijos: costesFijos,
      ebitda_conservador: ebitdaCons,
      ebitda_optimista: ebitdaOpt,
      acumulado_conservador: acumCons,
      acumulado_optimista: acumOpt,
      ramp_factor: ramp,
    });
  }

  return meses;
}

// Helper para mock de ParametroResponse (7 campos v2)
function mkParam(
  valor: number, fuente: string, rmin: number, rmax: number,
  confianza: "alta" | "media" | "baja" = "media",
): import("./types").ParametroResponse {
  return {
    valor_estimado: valor,
    valor_usado: valor,
    es_override: false,
    fuente,
    confianza,
    rango_min: rmin,
    rango_max: rmax,
  };
}

export const MOCK_FINANCIERO: FinancieroResponse = {
  parametros: {
    ticket_medio:        mkParam(180, "Sector tatuajes Barcelona 2024", 80, 400, "alta"),
    clients_per_day:     mkParam(5, "Estimación sector + flujo peatonal", 2, 14, "media"),
    alquiler_mensual:    mkParam(1900, "Datos zona Gràcia", 1400, 2800, "alta"),
    dias_apertura_mes:   mkParam(24, "Operativa estándar", 18, 30, "alta"),
    salarios_mensual:    mkParam(2600, "Convenio + estructura mínima", 1500, 5200, "media"),
    otros_fijos_mensual: mkParam(650, "Suministros + software + gestoría", 300, 1400, "media"),
    coste_mercancia_pct: mkParam(0.18, "Materiales y consumibles", 0.08, 0.35, "alta"),
    reforma_local:       mkParam(12000, "Reforma ligera especializada", 5000, 25000, "baja"),
    equipamiento:        mkParam(8000, "Benchmarks sectoriales", 4000, 15000, "baja"),
    deposito_fianza:     mkParam(3800, "Art. 36 LAU — 2 meses", 1800, 5600, "alta"),
    otros_iniciales:     mkParam(3500, "Licencias + gestoría apertura", 2000, 6000, "media"),
  },
  inversion_total: 28000,
  desglose_inversion: {
    reforma_local: 12000,
    equipamiento: 8000,
    deposito_fianza: 4500,
    otros_iniciales: 3500,
  },
  ingresos_anuales_conservador: 91800,
  ingresos_anuales_optimista: 136800,
  margen_bruto_pct: 0.65,
  ebitda_anual_conservador: 14200,
  ebitda_anual_optimista: 38400,
  roi_3a_conservador: 0.52,
  roi_3a_optimista: 1.24,
  payback_meses_conservador: 24,
  payback_meses_optimista: 14,
  breakeven_clientes_dia: 3,
  proyeccion: buildProyeccion(),
  margen_sector_tipico: 0.62,
  alquiler_sobre_ventas_pct: 0.248,
  alerta_alquiler: true,
};

export const MOCK_LEGAL: LegalRoadmapResponse = {
  tipo_negocio: "Salón de Tatuajes",
  distrito: "Gràcia",
  zona_restringida: true,
  equipo_externo: [
    {
      nombre: "Gestoría / Asesoría Fiscal",
      descripcion: "Alta de empresa, nóminas e impuestos trimestrales (IVA e IRPF). Recomendable desde el día 1.",
      coste_aprox: "50–80 €/mes",
    },
    {
      nombre: "Arquitecto o Ingeniero Técnico",
      descripcion: "Firma el Proyecto Técnico visado para la licencia de actividad. Sin su firma, el Ayuntamiento no tramita el permiso de apertura.",
      coste_aprox: "1.800–4.500 €",
    },
    {
      nombre: "Servicio de Prevención Ajeno",
      descripcion: "Obligatorio por ley en el momento en que contrates tu primer empleado, incluso a media jornada.",
      coste_aprox: "300–600 €/año",
    },
  ],
  fases: [
    {
      id: "viabilidad",
      numero: 1,
      titulo: "Viabilidad previa",
      descripcion: "Confirmar que la actividad está permitida en la dirección exacta antes de firmar el alquiler.",
      tramites: [
        {
          numero: 1,
          titulo: "Informe de Compatibilidad Urbanística",
          nombre_oficial: "Informe urbanístic de compatibilitat / Informe previ d'activitats",
          que_es: "Consulta obligatoria al Ayuntamiento para que confirmen por escrito si en tu calle y número exactos está permitido el tipo de negocio que quieres montar. Es el documento más importante antes de firmar el contrato de alquiler.",
          donde: "Ajuntament de Barcelona — Sede Electrónica",
          documentos: ["Referencia Catastral del local (viene en el recibo del IBI o en sede.catastro.gob.es)"],
          tiempo_estimado: "15–30 días",
          enlace: "https://seuelectronica.ajuntament.barcelona.cat/es/tramits-telematics",
          alerta: "Zona con Plan de Usos Restrictivo en Gràcia. El proceso puede ser más lento y existe riesgo de denegación si ya hay muchos locales del mismo tipo en un radio de 50–100 m.",
        },
      ],
    },
    {
      id: "constitucion",
      numero: 2,
      titulo: "Constitución legal y fiscal",
      descripcion: "Alta de empresa en Hacienda y elección de forma jurídica.",
      tramites: [
        {
          numero: 2,
          titulo: "Alta en el Censo de Empresarios (Modelo 036/037)",
          nombre_oficial: "Declaración Censal de inicio, modificación o cese de actividad",
          que_es: "Dar de alta tu empresa en la AEAT para obtener el NIF definitivo y asignar el código de actividad económica. Para un salón de tatuajes el epígrafe IAE es el 972.9 (Otros servicios personales).",
          donde: "Sede Electrónica de la AEAT — lo tramita la gestoría.",
          documentos: ["DNI/NIE del titular", "Contrato de arrendamiento o nota simple del local"],
          tiempo_estimado: "1–3 días hábiles",
          enlace: "https://sede.agenciatributaria.gob.es/",
        },
        {
          numero: 3,
          titulo: "Alta en el Régimen Especial de Autónomos (RETA) o constitución de S.L.",
          que_es: "Autónomo: más fácil al inicio, pero tu patrimonio personal responde ante deudas. S.L.: ~600 € de coste inicial, pero la responsabilidad se limita al capital de la empresa. Si no has sido autónomo en los últimos 2 años, solicita la Tarifa Plana: ~80 €/mes en lugar de 290 €+.",
          donde: "Seguridad Social — Sede Electrónica o en cualquier oficina de la TGSS.",
          documentos: ["Modelo TA.0521 (alta autónomos)", "Modelo 036/037 previamente tramitado", "Fotocopia del DNI"],
          tiempo_estimado: "1–5 días",
          enlace: "https://sede.seg-social.gob.es/",
        },
      ],
    },
    {
      id: "licencias",
      numero: 3,
      titulo: "Licencias de actividad",
      descripcion: "Proyecto técnico visado, tasas municipales y permisos sectoriales específicos.",
      tramites: [
        {
          numero: 4,
          titulo: "Licencia Ambiental / Comunicación Previa de Actividad",
          nombre_oficial: "Comunicació prèvia d'inici d'activitat / Llicència ambiental",
          que_es: "Permiso formal del Ayuntamiento para abrir el local al público. Para un salón de tatuajes el requisito crítico es el aislamiento acústico y la gestión correcta de residuos biológicos (agujas y tintas caducadas). Necesitas un Proyecto Técnico visado con planos, estudio acústico y plan de gestión de residuos.",
          donde: "Ajuntament de Barcelona — Portal de Tràmits (seuelectronica).",
          documentos: ["Proyecto Técnico firmado y visado por Arquitecto o Ingeniero", "Planos del local (distribución, instalaciones, salidas de emergencia)", "Estudio acústico", "Plan de gestión de residuos biosanitarios (agujas, tintas)"],
          tiempo_estimado: "2–4 meses",
          coste_estimado: "400–1.200 €",
          enlace: "https://seuelectronica.ajuntament.barcelona.cat/es/tramits-telematics",
        },
        {
          numero: 5,
          titulo: "Autorización Sanitaria y Registro en ASPCAT",
          nombre_oficial: "Autorització sanitària d'establiments de tatuatge, pírcing i tècniques similars",
          que_es: "Obligatorio en Cataluña. El titular debe obtener el Diploma de Formación Higiénico-Sanitaria (mínimo 40h presenciales en un centro acreditado) y registrar el establecimiento en el ASPCAT antes de abrir.",
          donde: "Agència de Salut Pública de Catalunya (ASPCAT) — Canal Empresa.",
          documentos: ["Diploma de formación higiénico-sanitaria (mín. 40h)", "Plano del local con distribución detallada", "Alta IAE", "Justificante de titularidad del local"],
          tiempo_estimado: "1–2 meses",
          coste_estimado: "150–300 €",
          enlace: "https://canalempresa.gencat.cat/es/tramit/que-vols-fer/salon-tatuajes/",
        },
      ],
    },
    {
      id: "operativa",
      numero: 4,
      titulo: "Puesta a punto operativa",
      descripcion: "Protección de datos, apertura del centro de trabajo y obligaciones catalanas.",
      tramites: [
        {
          numero: 6,
          titulo: "Adaptación al RGPD / LOPD (Protección de Datos)",
          que_es: "Los salones de tatuajes tratan datos de salud (alergias, historial de sesiones). Necesitas un consentimiento explícito y firmado de cada cliente antes de la primera sesión, una política de privacidad visible en el local y en tu web, y un Registro de Actividades de Tratamiento.",
          donde: "Agencia Española de Protección de Datos (AEPD) — hay modelos gratuitos en su web.",
          documentos: ["Cláusula de consentimiento informado del cliente", "Política de privacidad", "Registro de actividades de tratamiento (RAT)"],
          tiempo_estimado: "1–2 semanas",
          enlace: "https://www.aepd.es/es",
          alerta: "Datos de salud = categoría especial (Art. 9 RGPD). El consentimiento debe ser siempre por escrito y el cliente puede revocarlo en cualquier momento.",
        },
        {
          numero: 7,
          titulo: "Comunicación de Apertura del Centro de Trabajo",
          que_es: "Aviso obligatorio a la Generalitat de que tienes un local con personas trabajando. Es obligatorio aunque el único trabajador seas tú como autónomo. Plazo máximo: 30 días desde el inicio de actividad.",
          donde: "Canal Empresa de la Generalitat de Catalunya — trámite 100% online.",
          documentos: ["NIF/CIF del titular o empresa", "Dirección exacta del centro de trabajo", "Código de actividad (CNAE o IAE)"],
          tiempo_estimado: "Inmediato (online)",
          enlace: "https://canalempresa.gencat.cat/es/tramit/que-vols-fer/obrir-un-centre-de-treball/",
        },
        {
          numero: 8,
          titulo: "Obligaciones de Atención al Cliente en Cataluña",
          que_es: "Debes tener las Hojas Oficiales de Reclamación de la Generalitat (amarillas/verdes) visibles al público. La rotulación del local, la lista de precios y los formularios de consentimiento deben estar disponibles en catalán como mínimo.",
          donde: "Dirección General de Consumo — Generalitat de Catalunya.",
          tiempo_estimado: "Antes de la apertura",
          enlace: "https://consum.gencat.cat/ca/tramits/tramits-temes/fulls-reclamacio/",
        },
      ],
    },
    {
      id: "supervivencia",
      numero: 5,
      titulo: "Checklist supervivencia",
      descripcion: "Lo que debes controlar durante los primeros 3 meses de actividad.",
      tramites: [
        {
          numero: 9,
          titulo: "Impuestos Trimestrales — Modelo 303 (IVA)",
          que_es: "Cada trimestre (Abril, Julio, Octubre y Enero) debes presentar el Modelo 303 de IVA a la AEAT. Guarda siempre un 15–20% de lo que factures para pagar el IVA. Tu gestoría lo presenta automáticamente si tienes contrato de presentación.",
          donde: "Sede Electrónica de la AEAT — plazo días 1 al 20 del mes siguiente al trimestre.",
          tiempo_estimado: "Cada 3 meses (trimestral)",
          enlace: "https://sede.agenciatributaria.gob.es/Sede/procedimientoini/G415.shtml",
        },
        {
          numero: 10,
          titulo: "Preparación para la Inspección Municipal",
          que_es: "El Ayuntamiento de Barcelona realiza visitas sorpresa entre 3 y 6 meses después de la apertura para verificar que las obras realizadas coinciden con el Proyecto Técnico visado. Si no coinciden, pueden obligarte a restablecerlas o solicitar una nueva licencia.",
          donde: "Iniciativa del Ayuntamiento — no tienes que solicitar nada, solo estar preparado.",
          documentos: ["Certificado Final de Obra (expedido por el arquitecto o ingeniero)", "Copia de la licencia de actividad concedida", "Carné de formación higiénico-sanitaria del titular o empleados"],
          tiempo_estimado: "3–6 meses tras la apertura",
          alerta: "Ten siempre el Certificado Final de Obra y la licencia de actividad accesibles en el local. Una discrepancia puede implicar multa o cierre cautelar.",
        },
      ],
    },
  ],
  costes_resumen: [
    { concepto: "Gestoría (apertura + 1er año)", coste: "600 – 900 €" },
    { concepto: "Proyecto Técnico visado (Arquitecto/Ingeniero)", coste: "1.800 – 4.500 €" },
    { concepto: "Tasas del Ayuntamiento (Licencia Actividad)", coste: "400 – 1.200 €" },
    { concepto: "Autorización Sanitaria ASPCAT", coste: "150 – 300 €" },
    { concepto: "Legalización suministros (boletín eléctrico/gas)", coste: "200 – 600 €" },
    { concepto: "Seguro de Responsabilidad Civil (anual)", coste: "350 – 700 €" },
    { concepto: "TOTAL APROX. EN PAPELEO", coste: "3.500 – 8.200 €" },
  ],
  proximos_pasos: [
    "Solicitar el Informe de Compatibilidad Urbanística ANTES de firmar el contrato de alquiler.",
    "Reservar presupuesto para el Proyecto Técnico visado (1.800–4.500 €): es el coste más alto e inevitable.",
    "Inscribirte en el curso de Formación Higiénico-Sanitaria (mín. 40h): los plazos de convocatoria son lentos.",
    "Guardar el 15–20% de cada factura emitida para el pago del IVA trimestral.",
    "Tener siempre el Certificado Final de Obra en el local para la inspección municipal.",
  ],
};
