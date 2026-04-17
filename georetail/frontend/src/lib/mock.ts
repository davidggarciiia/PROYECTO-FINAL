import type {
  BuscarResponse,
  CompetenciaDetalle,
  ExplicacionDimension,
  FinancieroResponse,
  ImpactoModeloDimension,
  LegalRoadmapResponse,
  LocalDetalleResponse,
} from "./types";

export const delay = (ms = 700) => new Promise<void>((res) => setTimeout(res, ms));

export const MOCK_SESSION_ID = "mock-session-001";

export const MOCK_BUSCAR: BuscarResponse = {
  estado: "ok",
  session_id: MOCK_SESSION_ID,
  total_zonas_analizadas: 312,
  zonas: [
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
  ],
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
      entorno_comercial: 95,
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
      entorno_comercial: 93,
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
      entorno_comercial: 76,
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

export function getMockDetalle(zona_id: string): LocalDetalleResponse {
  const override = DETAIL_OVERRIDES[zona_id];
  if (!override) return BASE_DETAIL;

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

export function getMockCompetencia(zona_id: string): CompetenciaDetalle {
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

export const MOCK_FINANCIERO: FinancieroResponse = {
  parametros: {
    ticket_medio: {
      valor_usado: 180,
      fuente: "Sector tatuajes Barcelona 2024",
      rango_min: 80,
      rango_max: 400,
    },
    alquiler_mensual: {
      valor_usado: 1900,
      fuente: "Datos zona Gràcia",
      rango_min: 1400,
      rango_max: 2800,
    },
    clientes_dia_conservador: {
      valor_usado: 4,
      fuente: "Estimación sector + flujo peatonal",
      rango_min: 2,
      rango_max: 10,
    },
    clientes_dia_optimista: {
      valor_usado: 6,
      fuente: "Estimación sector + captación digital",
      rango_min: 3,
      rango_max: 14,
    },
    dias_apertura_mes: {
      valor_usado: 24,
      fuente: "Operativa estándar",
      rango_min: 18,
      rango_max: 30,
    },
    salarios_mensual: {
      valor_usado: 2600,
      fuente: "Convenio + estructura mínima",
      rango_min: 1500,
      rango_max: 5200,
    },
    otros_fijos_mensual: {
      valor_usado: 650,
      fuente: "Suministros + software + gestoría",
      rango_min: 300,
      rango_max: 1400,
    },
    coste_mercancia_pct: {
      valor_usado: 0.18,
      fuente: "Materiales y consumibles",
      rango_min: 0.08,
      rango_max: 0.35,
    },
    reforma_local: {
      valor_usado: 12000,
      fuente: "Reforma ligera especializada",
      rango_min: 5000,
      rango_max: 25000,
    },
    num_empleados: {
      valor_usado: 2,
      fuente: "Operativa mínima",
      rango_min: 1,
      rango_max: 6,
    },
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
  tipo_negocio: "Restaurante / Bar",
  distrito: "Eixample",
  zona_restringida: true,
  equipo_externo: [
    {
      nombre: "Gestoría / Asesoría Fiscal",
      descripcion: "Alta de empresa, impuestos y soporte recurrente.",
      coste_aprox: "50–80 €/mes",
    },
    {
      nombre: "Arquitecto o ingeniero técnico",
      descripcion: "Proyecto técnico y adecuación del local.",
      coste_aprox: "1.800–4.500 €",
    },
  ],
  fases: [
    {
      id: "viabilidad",
      numero: 1,
      titulo: "Viabilidad previa",
      descripcion: "Comprobar compatibilidad urbanística antes de firmar el alquiler.",
      tramites: [
        {
          numero: 1,
          titulo: "Informe de Compatibilidad Urbanística",
          que_es: "Confirmación municipal de que la actividad se puede implantar en esa dirección.",
          donde: "Ajuntament de Barcelona",
          tiempo_estimado: "15–30 días",
          alerta: "Zona con plan de usos restrictivo.",
        },
      ],
    },
    {
      id: "licencias",
      numero: 2,
      titulo: "Licencias y puesta en marcha",
      descripcion: "Proyecto técnico, tasas y permisos operativos.",
      tramites: [
        {
          numero: 2,
          titulo: "Licencia de actividad",
          que_es: "Permiso de apertura del local.",
          donde: "Ajuntament de Barcelona",
          tiempo_estimado: "2–4 meses",
          coste_estimado: "400–1.200 €",
        },
      ],
    },
  ],
  costes_resumen: [
    { concepto: "Proyecto técnico + tasas", coste: "2.200 – 5.700 €" },
    { concepto: "Seguro y gestoría primer año", coste: "800 – 1.600 €" },
  ],
  proximos_pasos: [
    "Solicitar compatibilidad urbanística antes de firmar.",
    "Validar el proyecto técnico del local.",
    "Reservar presupuesto específico para licencias y tasas.",
  ],
};
