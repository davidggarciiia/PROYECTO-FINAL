import type {
  BuscarRequest,
  BuscarResponse,
  CuestionarioRequest,
  CuestionarioResponse,
  LocalDetalleResponse,
  FinancieroResponse,
} from "./types";
import { mockBuscar, mockLocalDetalle, mockFinanciero } from "./mockData";

const IS_DEMO = process.env.NEXT_PUBLIC_DEMO_MODE === "true";
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

const delay = (ms = 600) => new Promise(r => setTimeout(r, ms));

export const api = {
  buscar: async (body: BuscarRequest): Promise<BuscarResponse> => {
    if (IS_DEMO) { await delay(); return mockBuscar(body.descripcion); }
    return apiFetch<BuscarResponse>("/api/buscar", { method: "POST", body: JSON.stringify(body) });
  },

  cuestionario: (body: CuestionarioRequest) =>
    apiFetch<CuestionarioResponse>("/api/cuestionario", { method: "POST", body: JSON.stringify(body) }),

  localPreview: (zona_id: string, session_id: string) =>
    apiFetch<{ zona_id: string; nombre: string; calle?: string; m2?: number; alquiler_mensual?: number; score_global?: number; color: string }>(
      "/api/local/preview",
      { method: "POST", body: JSON.stringify({ zona_id, session_id }) }
    ),

  localDetalle: async (zona_id: string, _session_id: string): Promise<LocalDetalleResponse> => {
    if (IS_DEMO) { await delay(800); return mockLocalDetalle(zona_id); }
    return apiFetch<LocalDetalleResponse>("/api/local", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id: _session_id }),
    });
  },

  financiero: async (zona_id: string, session_id: string, overrides: Record<string, number> = {}): Promise<FinancieroResponse> => {
    if (IS_DEMO) { await delay(600); return mockFinanciero(); }
    return apiFetch<FinancieroResponse>("/api/financiero", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id, overrides }),
    });
  },

  refinamiento: (session_id: string, texto: string) =>
    apiFetch<{ zonas: import("./types").ZonaPreview[]; total: number; mensaje_confirmacion: string }>(
      "/api/refinamiento",
      { method: "POST", body: JSON.stringify({ session_id, texto }) }
    ),

  exportar: (session_id: string, zona_ids: string[], nombre_empresa?: string) =>
    apiFetch<{ pdf_id: string; estado: string; url_descarga?: string }>(
      "/api/exportar",
      { method: "POST", body: JSON.stringify({ session_id, zona_ids, nombre_empresa, incluir_financiero: true, incluir_competencia: true }) }
    ),

  health: () => apiFetch<{ status: string; version: string }>("/api/health"),
};
