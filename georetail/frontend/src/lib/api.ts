import type {
  BuscarRequest,
  BuscarResponse,
  CuestionarioRequest,
  CuestionarioResponse,
  LocalDetalleResponse,
  FinancieroResponse,
  LegalRoadmapResponse,
  CompetenciaDetalle,
  TransporteDetalleZona,
} from "./types";

// ─────────────────────────────────────────────────────────────────────────────
// 🔧 MOCK FLAG — cambiar a false para conectar con el backend real
// ─────────────────────────────────────────────────────────────────────────────
const USE_MOCK = process.env.NEXT_PUBLIC_USE_MOCK === "true";
// ─────────────────────────────────────────────────────────────────────────────

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const err = await res.text().catch(() => res.statusText);
    throw new Error(`API ${path} → ${res.status}: ${err}`);
  }
  return res.json() as Promise<T>;
}

// ─── MOCK HELPERS ─────────────────────────────────────────────────────────────

async function mockFetch<T>(data: T, ms = 700): Promise<T> {
  await new Promise<void>((res) => setTimeout(res, ms));
  return structuredClone(data) as T;
}

// ─── API ──────────────────────────────────────────────────────────────────────

export const api = {
  buscar: async (body: BuscarRequest): Promise<BuscarResponse> => {
    if (USE_MOCK) {
      const { MOCK_BUSCAR } = await import("./mock");
      return mockFetch(MOCK_BUSCAR, 800);
    }
    return apiFetch<BuscarResponse>("/api/buscar", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },

  cuestionario: async (body: CuestionarioRequest): Promise<CuestionarioResponse> => {
    if (USE_MOCK) {
      return mockFetch<CuestionarioResponse>(
        { estado: "ok", progreso_pct: 100, trigger_busqueda: false },
        400
      );
    }
    return apiFetch<CuestionarioResponse>("/api/cuestionario", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },

  localPreview: async (
    zona_id: string,
    session_id: string
  ): Promise<{
    zona_id: string;
    nombre: string;
    calle?: string;
    m2?: number;
    alquiler_mensual?: number;
    score_global?: number;
    color: string;
  }> => {
    if (USE_MOCK) {
      return mockFetch(
        { zona_id, nombre: zona_id, color: "amarillo", m2: 70, alquiler_mensual: 1900, score_global: 60 },
        300
      );
    }
    return apiFetch("/api/local/preview", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id }),
    });
  },

  localDetalle: async (zona_id: string, session_id: string): Promise<LocalDetalleResponse> => {
    if (USE_MOCK) {
      const { getMockDetalle } = await import("./mock");
      return mockFetch(getMockDetalle(zona_id), 900);
    }
    return apiFetch<LocalDetalleResponse>("/api/local", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id }),
    });
  },

  transporte: async (zona_id: string, radio_m = 500): Promise<TransporteDetalleZona> => {
    if (USE_MOCK) {
      return mockFetch<TransporteDetalleZona>(
        {
          zona_id,
          radio_m,
          total_lineas: 0,
          total_paradas: 0,
          lineas: [],
        },
        300,
      );
    }
    const params = new URLSearchParams({ radio_m: String(radio_m) });
    return apiFetch<TransporteDetalleZona>(
      `/api/transporte/${encodeURIComponent(zona_id)}?${params.toString()}`,
    );
  },

  competencia: async (zona_id: string, session_id: string): Promise<CompetenciaDetalle> => {
    if (USE_MOCK) {
      const { getMockCompetencia } = await import("./mock");
      return mockFetch(getMockCompetencia(zona_id), 700);
    }
    const params = new URLSearchParams({ session_id });
    return apiFetch<CompetenciaDetalle>(`/api/competencia/${encodeURIComponent(zona_id)}?${params.toString()}`);
  },

  dimensionTurismo: async (zona_id: string, session_id: string) => {
    const params = new URLSearchParams({ session_id });
    return apiFetch<import("./types").DimensionTurismoDetalle>(
      `/api/dimension/turismo/${encodeURIComponent(zona_id)}?${params.toString()}`,
    );
  },

  financiero: async (
    zona_id: string,
    session_id: string,
    overrides: Record<string, number> = {}
  ): Promise<FinancieroResponse> => {
    if (USE_MOCK) {
      const { MOCK_FINANCIERO } = await import("./mock");
      // Aplicar overrides a los parámetros del mock
      const result = structuredClone(MOCK_FINANCIERO);
      if (overrides.ticket_medio)
        result.parametros.ticket_medio.valor_usado = overrides.ticket_medio;
      if (overrides.alquiler_mensual)
        result.parametros.alquiler_mensual.valor_usado = overrides.alquiler_mensual;
      if (overrides.clientes_dia_conservador)
        result.parametros.clientes_dia_conservador.valor_usado =
          overrides.clientes_dia_conservador;
      return mockFetch(result, 600);
    }
    return apiFetch<FinancieroResponse>("/api/financiero", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id, overrides }),
    });
  },

  refinamiento: async (
    session_id: string,
    texto: string
  ): Promise<{
    zonas: import("./types").ZonaPreview[];
    total: number;
    mensaje_confirmacion: string;
  }> => {
    if (USE_MOCK) {
      return mockFetch({ zonas: [], total: 0, mensaje_confirmacion: "Mock refinamiento" }, 500);
    }
    return apiFetch("/api/refinamiento", {
      method: "POST",
      body: JSON.stringify({ session_id, texto }),
    });
  },

  exportar: async (
    session_id: string,
    zona_ids: string[],
    nombre_empresa?: string
  ): Promise<{ pdf_id: string; estado: string; url_descarga?: string }> => {
    if (USE_MOCK) {
      return mockFetch({ pdf_id: "mock-pdf-001", estado: "ok" }, 1200);
    }
    return apiFetch("/api/exportar", {
      method: "POST",
      body: JSON.stringify({
        session_id,
        zona_ids,
        nombre_empresa,
        incluir_financiero: true,
        incluir_competencia: true,
      }),
    });
  },

  // ─────────────────────────────────────────────────────────────────────────────
  // 🔧 LEGAL_USE_MOCK — cambiar a false para conectar con DeepSeek vía backend
  // Independiente del USE_MOCK global para poder activar DeepSeek por separado
  // ─────────────────────────────────────────────────────────────────────────────
  legal: async (zona_id: string, session_id: string): Promise<LegalRoadmapResponse> => {
    if (USE_MOCK) {
      const { MOCK_LEGAL } = await import("./mock");
      return mockFetch(MOCK_LEGAL, 1200);
    }
    return apiFetch<LegalRoadmapResponse>("/api/legal/roadmap", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id }),
    });
  },

  health: async (): Promise<{ status: string; version: string }> => {
    if (USE_MOCK) return mockFetch({ status: "ok", version: "mock-1.0.0" }, 100);
    return apiFetch<{ status: string; version: string }>("/api/health");
  },
};
