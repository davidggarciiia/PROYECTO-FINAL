// ─────────────────────────────────────────────────────────────────────────────
// mock.ts  —  Datos simulados para desarrollo sin backend
// Para activar: USE_MOCK = true en api.ts
// Para desactivar: USE_MOCK = false en api.ts
// ─────────────────────────────────────────────────────────────────────────────

import type {
    BuscarResponse,
    LocalDetalleResponse,
    FinancieroResponse,
  } from "./types";
  
  /** Simula latencia de red */
  export const delay = (ms = 700) =>
    new Promise<void>((res) => setTimeout(res, ms));
  
  // ─── BÚSQUEDA ────────────────────────────────────────────────────────────────
  
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
        resumen_ia: "Zona en auge con clientela joven y local emergente.",
      },
      {
        zona_id: "barceloneta-mar",
        nombre: "Pg. Joan de Borbó",
        barrio: "La Barceloneta",
        distrito: "Ciutat Vella",
        score_global: 48,
        probabilidad_supervivencia_3a: 0.51,
        alquiler_estimado: 3200,
        m2_disponibles: 55,
        color: "rojo",
        lat: 41.3797,
        lng: 2.1888,
        resumen_ia: "Turismo alto pero competencia muy saturada y alquiler elevado.",
      },
      {
        zona_id: "sant-gervasi-muntaner",
        nombre: "C/ Muntaner Sant Gervasi",
        barrio: "Sant Gervasi - Galvany",
        distrito: "Sarrià-Sant Gervasi",
        score_global: 71,
        probabilidad_supervivencia_3a: 0.74,
        alquiler_estimado: 2100,
        m2_disponibles: 65,
        color: "amarillo",
        lat: 41.4012,
        lng: 2.1498,
        resumen_ia: "Barrio residencial de renta alta con comercio de proximidad.",
      },
    ],
  };
  
  // ─── DETALLE LOCAL ────────────────────────────────────────────────────────────
  
  const MOCK_DETALLE_BASE: LocalDetalleResponse = {
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
      },
      probabilidad_supervivencia: 0.69,
      flujo_peatonal_dia: {
        manana: 497,
        tarde: 1324,
        noche: 589,
      },
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
        },
        {
          nombre: "Ink Society",
          sector: "Estudio de tatuajes",
          distancia_m: 340,
          rating: 4.5,
          precio_nivel: 2,
          es_competencia_directa: true,
        },
        {
          nombre: "Peluquería Gràcia",
          sector: "Peluquería",
          distancia_m: 80,
          rating: 4.2,
          precio_nivel: 2,
          es_competencia_directa: false,
        },
        {
          nombre: "Café Diagonal",
          sector: "Hostelería",
          distancia_m: 55,
          rating: 4.0,
          precio_nivel: 1,
          es_competencia_directa: false,
        },
      ],
      alertas: [
        {
          tipo: "Saturación sectorial",
          texto:
            "Existen 2 estudios de tatuajes en un radio de 400m. La diferenciación será clave para captar cuota de mercado.",
          fuente: "Censo de actividades comerciales BCN 2024",
        },
        {
          tipo: "Flujo peatonal bajo",
          texto:
            "El tráfico matinal es muy reducido (497 personas/hora). Se recomienda horario de tarde centrado en 13:00–20:00h.",
          fuente: "Sensores municipales de movilidad",
        },
      ],
      analisis_ia: {
        resumen:
          "La zona de C/ Diagonal Gràcia presenta una oportunidad moderada para un negocio de tatuajes con una puntuación global de 64.0/100 y una probabilidad de supervivencia de 3 años del 69%. Sin embargo, el bajo flujo peatonal y la puntuación de turismo pueden limitar el alcance de los clientes.",
        puntos_fuertes: [
          "Alta puntuación de competencia (84/100) que indica baja saturación",
          "Alquiler asequible de 1900.0 €/mes por 70.0 m²",
          "Excelente puntuación de acceso al transporte (97/100)",
        ],
        puntos_debiles: [
          "Baja puntuación de flujo peatonal (32/100) puede obstaculizar a los clientes que entran sin cita",
          "Puntuación de turismo muy baja (10/100) limita la clientela potencial de turistas",
        ],
        oportunidad:
          "Clientela local fiel y barrio en proceso de gentrificación con renta media creciente.",
        riesgos:
          "Dependencia de citas previas por bajo tráfico espontáneo. Posible presión de nuevos competidores al alza del barrio.",
        recomendacion_final: "viable_con_condiciones",
        razon_recomendacion:
          "Viable si se apuesta por diferenciación de estilo y estrategia digital fuerte para compensar el bajo flujo peatonal.",
      },
    },
  };
  
  /** Devuelve el detalle correspondiente a la zona, o el base si no hay uno específico */
  export function getMockDetalle(zona_id: string): LocalDetalleResponse {
    const overrides: Record<string, Partial<LocalDetalleResponse["zona"]>> = {
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
        },
        flujo_peatonal_dia: { manana: 1820, tarde: 3450, noche: 1240 },
        num_negocios_activos: 287,
        analisis_ia: {
          resumen:
            "Consell de Cent es una de las calles comerciales más activas del Eixample. El altísimo flujo peatonal y el entorno comercial consolidado ofrecen una base de clientes sólida, aunque el alquiler elevado comprime los márgenes iniciales.",
          puntos_fuertes: [
            "Flujo peatonal excepcional (91/100) con más de 3.400 personas/hora en tarde",
            "Entorno comercial muy consolidado (93/100)",
            "Excelente conectividad de transporte público (95/100)",
          ],
          puntos_debiles: [
            "Precio de alquiler alto (45/100) reduce margen de beneficio inicial",
            "Competencia moderada-alta con 3 estudios similares en 500m",
          ],
          oportunidad: "Clientela diversa con alto poder adquisitivo y turismo activo.",
          riesgos: "Alquiler elevado puede comprometer la viabilidad si las ventas tardan en consolidarse.",
          recomendacion_final: "viable",
          razon_recomendacion: "Alta viabilidad gracias al volumen de tráfico y entorno premium.",
        },
        alertas: [],
        competidores_cercanos: [
          { nombre: "Black Rose Tattoo", sector: "Tatuajes", distancia_m: 210, rating: 4.9, precio_nivel: 3, es_competencia_directa: true },
          { nombre: "Eixample Ink", sector: "Tatuajes", distancia_m: 480, rating: 4.3, precio_nivel: 2, es_competencia_directa: true },
        ],
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
        },
        flujo_peatonal_dia: { manana: 890, tarde: 2100, noche: 1450 },
        num_negocios_activos: 143,
        analisis_ia: {
          resumen:
            "La Rambla del Poblenou es una apuesta con alta potencial de crecimiento. El barrio atrae a un perfil joven y creativo, afín a la cultura del tatuaje, y la baja saturación competitiva es una ventaja clara.",
          puntos_fuertes: [
            "Competencia muy baja (90/100) — prácticamente sin estudios de tatuaje en la zona",
            "Demografía joven y creativa muy receptiva al sector (81/100)",
            "Alquiler muy asequible para la ubicación (82/100)",
          ],
          puntos_debiles: [
            "Turismo moderado (45/100) — clientela mayoritariamente local",
            "Flujo peatonal medio — necesita estrategia de captación activa",
          ],
          oportunidad: "Ser el estudio de referencia en un barrio sin oferta consolidada.",
          riesgos: "El crecimiento del barrio puede atraer competencia en 12-18 meses.",
          recomendacion_final: "viable",
          razon_recomendacion: "Excelente oportunidad de posicionamiento en barrio emergente.",
        },
        alertas: [
          {
            tipo: "Barrio en transformación",
            texto: "El Poblenou está experimentando una fuerte revalorización. Los alquileres podrían subir un 15-20% en los próximos 2 años.",
            fuente: "Observatorio de la Vivienda de Barcelona 2024",
          },
        ],
        competidores_cercanos: [
          { nombre: "Body Art Studio", sector: "Tatuajes y piercing", distancia_m: 680, rating: 4.1, precio_nivel: 2, es_competencia_directa: false },
        ],
      },
    };
  
    const base = MOCK_DETALLE_BASE.zona;
    const override = overrides[zona_id];
  
    if (!override) return MOCK_DETALLE_BASE;
  
    return {
      zona: {
        ...base,
        ...override,
        competidores_cercanos: override.competidores_cercanos ?? base.competidores_cercanos,
        alertas: override.alertas ?? base.alertas,
        analisis_ia: override.analisis_ia ?? base.analisis_ia,
      },
    };
  }
  
  // ─── FINANCIERO ───────────────────────────────────────────────────────────────
  
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
    },
    inversion_total: 28000,
    desglose_inversion: {
      "Acondicionamiento local": 12000,
      "Equipamiento profesional": 8000,
      "Licencias y tasas": 3500,
      "Fondo de maniobra (3 meses)": 4500,
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