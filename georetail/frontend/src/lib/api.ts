import type {
  BuscarRequest,
  BuscarResponse,
  CuestionarioRequest,
  CuestionarioResponse,
  LocalDetalleResponse,
  FinancieroResponse,
  CompetenciaDetalle,
} from "./types";

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

export const api = {
  buscar: (body: BuscarRequest) =>
    apiFetch<BuscarResponse>("/api/buscar", { method: "POST", body: JSON.stringify(body) }),

  cuestionario: (body: CuestionarioRequest) =>
    apiFetch<CuestionarioResponse>("/api/cuestionario", { method: "POST", body: JSON.stringify(body) }),

  localPreview: (zona_id: string, session_id: string) =>
    apiFetch<{ zona_id: string; nombre: string; calle?: string; m2?: number; alquiler_mensual?: number; score_global?: number; color: string }>(
      "/api/local/preview",
      { method: "POST", body: JSON.stringify({ zona_id, session_id }) }
    ),

  localDetalle: (zona_id: string, session_id: string) =>
    apiFetch<LocalDetalleResponse>("/api/local", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id }),
    }),

  financiero: (zona_id: string, session_id: string, overrides: Record<string, number> = {}) =>
    apiFetch<FinancieroResponse>("/api/financiero", {
      method: "POST",
      body: JSON.stringify({ zona_id, session_id, overrides }),
    }),

  competencia: (zona_id: string, session_id: string) =>
    apiFetch<CompetenciaDetalle>(`/api/competencia/${zona_id}?session_id=${encodeURIComponent(session_id)}`),

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
