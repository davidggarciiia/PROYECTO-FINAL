// ─────────────────────────────────────────────────────────────────────────────
// mock.ts  —  Datos simulados para desarrollo sin backend
// Para activar: USE_MOCK = true en api.ts
// Para desactivar: USE_MOCK = false en api.ts
// ─────────────────────────────────────────────────────────────────────────────

import type {
    BuscarResponse,
    LocalDetalleResponse,
    FinancieroResponse,
    LegalRoadmapResponse,
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
  
  // ─── LEGAL ROADMAP ───────────────────────────────────────────────────────────

  export const MOCK_LEGAL: LegalRoadmapResponse = {
    tipo_negocio: "Restaurante / Bar",
    distrito: "Eixample",
    zona_restringida: true,
    equipo_externo: [
      {
        nombre: "Gestoría / Asesoría Fiscal",
        descripcion: "Alta de empresa, nóminas, IVA trimestral e IRPF. Sin ella el papeleo fiscal se vuelve inmanejable.",
        coste_aprox: "50–80 €/mes",
      },
      {
        nombre: "Arquitecto o Ingeniero Técnico",
        descripcion: "Firma el Proyecto Técnico visado. Sin su firma el Ayuntamiento de Barcelona no tramita ningún permiso de apertura.",
        coste_aprox: "1.800–4.500 € (proyecto completo)",
      },
      {
        nombre: "Servicio de Prevención Ajeno",
        descripcion: "Obligatorio desde el primer empleado (aunque sea media jornada). Incluye evaluación de riesgos y formación básica.",
        coste_aprox: "300–600 €/año",
      },
    ],
    fases: [
      {
        id: "viabilidad",
        numero: 1,
        titulo: "Viabilidad específica para tu local",
        descripcion: "Antes de firmar el contrato de alquiler, confirma por escrito que el Ayuntamiento permite tu actividad en esa dirección exacta.",
        tramites: [
          {
            numero: 1,
            titulo: "Informe de Compatibilidad Urbanística",
            nombre_oficial: "Informe urbanístic de compatibilitat / Informe previ d'activitats",
            que_es: "Consulta obligatoria al Ayuntamiento para que confirmen por escrito si en tu calle y número exactos se permite el tipo de negocio que quieres montar. Es el documento más importante antes de firmar el contrato de alquiler.",
            donde: "Ajuntament de Barcelona — Sede Electrónica",
            documentos: ["Referencia Catastral del local (viene en el recibo del IBI o en la web del Catastro)"],
            tiempo_estimado: "15–30 días",
            enlace: "https://seuelectronica.ajuntament.barcelona.cat/es/tramits-telematics",
            alerta: "⚠ ZONA RESTRINGIDA — Tu local está en una zona con Plan de Usos Restrictivo. El Ayuntamiento aplica aquí limitaciones especiales a la apertura de negocios de restauración para evitar la masificación. El proceso puede ser más lento y caro, y existe riesgo real de denegación si ya hay muchos locales del mismo tipo en un radio de 50–100 m.",
          },
        ],
      },
      {
        id: "constitucion",
        numero: 2,
        titulo: "Constitución legal y fiscal",
        descripcion: "El nacimiento oficial de la empresa. Hazlo antes de buscar local para tener el NIF listo.",
        tramites: [
          {
            numero: 2,
            titulo: "Alta en el Censo de Empresarios (Modelo 036/037)",
            que_es: "Dar de alta tu empresa en Hacienda (AEAT) para obtener el NIF definitivo y asignar el código de actividad económica (IAE). Epígrafe IAE recomendado: 671 — Restaurantes.",
            donde: "Sede Electrónica de la AEAT (normalmente lo tramita la gestoría)",
            tiempo_estimado: "1–3 días hábiles",
            enlace: "https://sede.agenciatributaria.gob.es/",
          },
          {
            numero: 3,
            titulo: "Alta como Autónomo (RETA) o constitución de S.L.",
            que_es: "Autónomo: más fácil y barato, pero tu patrimonio personal responde ante deudas. Sociedad Limitada: más papeleo inicial (~600 €), pero tu responsabilidad se limita al capital de la empresa. Muy recomendado para restauración dado el riesgo operativo.",
            donde: "Seguridad Social — Sede Electrónica (RETA) o Notaría + Registro Mercantil (S.L.)",
            tiempo_estimado: "1 día (autónomo) / 2–4 semanas (S.L.)",
            coste_estimado: "Cuota RETA desde 80 €/mes (tarifa plana si no has sido autónomo en los últimos 2 años)",
            enlace: "https://sede.seg-social.gob.es/",
          },
        ],
      },
      {
        id: "licencias",
        numero: 3,
        titulo: "Licencias de actividad y local",
        descripcion: "El gran filtro burocrático. Aquí se concentra el mayor gasto técnico y el mayor tiempo de espera.",
        tramites: [
          {
            numero: 4,
            titulo: "Licencia Ambiental / Comunicación Previa de Actividad",
            nombre_oficial: "Comunicació prèvia d'inici d'activitat o Llicència ambiental",
            que_es: "Permiso municipal que autoriza el inicio de la actividad en el local. Para restauración es obligatoria la Llicència ambiental (Annexe III.2), más exigente que una simple comunicación previa. Requiere Proyecto Técnico visado con planos, estudio acústico, plan de evacuación y cumplimiento del CTE.",
            donde: "Ajuntament de Barcelona — Portal de Tràmits",
            documentos: [
              "Proyecto Técnico visado por Arquitecto/Ingeniero",
              "Planos a escala del local (planta, alzados, secciones)",
              "Estudio acústico (obligatorio para actividades con música o cocina industrial)",
              "Plan de Evacuación y cumplimiento CTE-SI",
              "Certificado de instalación eléctrica (boletín)",
              "Certificado de instalación de gas (si aplica)",
            ],
            tiempo_estimado: "2–4 meses",
            coste_estimado: "Tasas municipales: 400–1.200 € según m² y actividad",
            enlace: "https://seuelectronica.ajuntament.barcelona.cat/es/tramits-telematics",
          },
          {
            numero: 5,
            titulo: "Permisos Sectoriales Específicos — Sanidad y Seguridad",
            que_es: "Permisos adicionales obligatorios para actividades de restauración: autorización sanitaria de cocina (Agència de Salut Pública de Barcelona), carné de manipulador de alimentos para todo el personal, y seguro de Responsabilidad Civil mínimo de 300.000 €.",
            donde: "Agència de Salut Pública de Barcelona (ASPB) + aseguradora",
            documentos: [
              "Solicitud de autorización sanitaria de establecimiento alimentario",
              "Plano de la zona de manipulación con superficies y materiales",
              "Certificados de carné de manipulador de alimentos del personal",
            ],
            tiempo_estimado: "4–8 semanas",
            coste_estimado: "Carné manipulador: ~30–50 €/persona. Seguro RC: 400–800 €/año",
          },
        ],
      },
      {
        id: "operativa",
        numero: 4,
        titulo: "Puesta a punto operativa y legal",
        descripcion: "Obligaciones legales del día a día que no pueden ignorarse desde el primer momento.",
        tramites: [
          {
            numero: 6,
            titulo: "Protección de Datos (RGPD / LOPD-GDD)",
            que_es: "Si gestionas reservas, programa de fidelización, o newsletter, estás tratando datos personales de clientes. Necesitas política de privacidad visible, formularios de consentimiento y registro de actividades de tratamiento. Si usas cámaras de seguridad, hay obligaciones adicionales.",
            donde: "Agencia Española de Protección de Datos (AEPD) — autogestión o con asesor",
            tiempo_estimado: "1–2 semanas para preparar documentación",
            enlace: "https://www.aepd.es/",
          },
          {
            numero: 7,
            titulo: "Comunicación de Apertura del Centro de Trabajo",
            que_es: "Avisar a la Generalitat de que tienes un local con personas trabajando. Obligatorio aunque el único trabajador seas tú como autónomo.",
            donde: "Canal Empresa — Generalitat de Catalunya",
            tiempo_estimado: "Dentro de los 30 días siguientes al inicio de actividad",
            enlace: "https://canalempresa.gencat.cat/es/tramit/que-vols-fer/obrir-un-centre-de-treball/",
          },
          {
            numero: 8,
            titulo: "Obligaciones específicas de Catalunya",
            que_es: "Debes tener las Hojas Oficiales de Reclamación de la Generalitat (color amarillo/verde) a la vista del público. La carta/menú de precios debe estar disponible como mínimo en catalán. El rótulo exterior también debe incluir el catalán como lengua principal.",
            donde: "Agència Catalana del Consum + Consorci de Normalització Lingüística",
            tiempo_estimado: "Antes de la apertura",
          },
        ],
      },
      {
        id: "checklist",
        numero: 5,
        titulo: "Checklist final de supervivencia",
        descripcion: "Lo que tienes que hacer o tener listo en los primeros 30 días tras abrir.",
        tramites: [
          {
            numero: 9,
            titulo: "Impuestos trimestrales — Modelo 303 (IVA)",
            que_es: "Tu gestoría presenta el IVA trimestral en abril, julio, octubre y enero. Reserva siempre el 10% de lo que factures (tipo reducido restauración) para no tener sorpresas al liquidar.",
            donde: "Sede Electrónica AEAT (gestoría lo hace por ti)",
            tiempo_estimado: "Plazo: 20 días naturales después de cada trimestre",
          },
          {
            numero: 10,
            titulo: "Inspección Municipal de Comprobación",
            que_es: "El Ayuntamiento de Barcelona suele realizar una visita sorpresa entre 3 y 6 meses después de la apertura para verificar que las obras coinciden con el Proyecto Técnico visado. Ten siempre a mano en el local: Certificado Final de Obra, licencia de actividad, último boletín eléctrico y seguro RC.",
            donde: "El inspector viene a ti — no hay que solicitarla",
            alerta: "Si la inspección detecta incumplimientos, pueden ordenar el cierre cautelar del local hasta que se subsanen. Guarda todos los documentos originales en el local.",
          },
        ],
      },
    ],
    costes_resumen: [
      { concepto: "Gestoría (apertura + 1er año)", coste: "600 – 900 €" },
      { concepto: "Proyecto Técnico visado (Arquitecto)", coste: "1.800 – 4.500 €" },
      { concepto: "Tasas municipales (Licencia Actividad)", coste: "400 – 1.200 €" },
      { concepto: "Legalización suministros (eléctrico / gas)", coste: "200 – 600 €" },
      { concepto: "Seguro Responsabilidad Civil (anual)", coste: "400 – 800 €" },
      { concepto: "Carné manipulador alimentos (por persona)", coste: "30 – 50 €" },
      { concepto: "TOTAL APROXIMADO EN PAPELEO", coste: "3.500 – 8.000 €" },
    ],
    proximos_pasos: [
      "Solicita el Informe de Compatibilidad Urbanística ANTES de firmar el alquiler.",
      "Contrata un arquitecto técnico para valorar el Proyecto de Actividad.",
      "Deja el alta en Hacienda (036) y en la Seguridad Social (RETA) para la gestoría.",
      "Solicita autorización sanitaria en la ASPB con al menos 2 meses de antelación a la apertura.",
      "Reserva un fondo de 4.000–8.000 € exclusivamente para burocracia y proyectos técnicos.",
      "Guarda el Certificado Final de Obra y la licencia de actividad siempre en el local.",
    ],
  };

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