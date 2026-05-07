/**
 * Helpers compartidos por los sub-componentes del DossierEditorial.
 * Sin dependencias externas — formateo y derivaciones puras.
 */

export function formatPeatones(n: number): string {
  if (n >= 1000) return `${Math.round((n / 1000) * 10) / 10}k`;
  return String(Math.round(n));
}

export function formatEur(n: number): string {
  return n.toLocaleString("es-ES");
}

export function formatEurWithSymbol(n: number): string {
  return n.toLocaleString("es-ES", { maximumFractionDigits: 0 }) + " €";
}

export function formatDist(m: number): string {
  if (m < 1000) return `${Math.round(m).toLocaleString("es-ES")} m`;
  return `${(m / 1000).toLocaleString("es-ES", { maximumFractionDigits: 1 })} km`;
}

export function formatPct(n: number, decimals = 0): string {
  return n.toLocaleString("es-ES", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }) + "%";
}

export function clampScore(value?: number | null): number {
  if (value == null || Number.isNaN(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

export function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

export function normalizeList(items?: string[] | null): string[] {
  return (items ?? []).map((item) => item.trim()).filter(Boolean);
}

/**
 * Texto del veredicto del lede a partir del score global.
 * Coherente con la convención de la app: ≥75 favorable, ≥55 con reservas.
 */
export function veredictoFromScore(score?: number | null): {
  label: string;
  band: "hi" | "mid" | "lo" | "na";
} {
  if (score == null) return { label: "Sin datos", band: "na" };
  if (score >= 75) return { label: "Recomendado", band: "hi" };
  if (score >= 55) return { label: "Con reservas", band: "mid" };
  return { label: "Replantear", band: "lo" };
}

/**
 * Convierte una probabilidad (0..1) en porcentaje entero (0..100).
 * Devuelve null si la entrada no es número.
 */
export function probToPct(p?: number | null): number | null {
  if (p == null || Number.isNaN(p)) return null;
  return Math.round(p * 100);
}

/**
 * Convierte una fecha YYYY-MM en label corto «Ene 24» en español.
 * Tolerante con formatos sucios (ya viene de PostgreSQL como '2024-01').
 */
const MES_SHORT = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                   "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];

export function mesLabel(mesISO: string | null | undefined): string {
  if (!mesISO) return "—";
  const m = mesISO.match(/^(\d{4})-(\d{2})/);
  if (!m) return mesISO;
  const yy = m[1].slice(2);
  const idx = parseInt(m[2], 10) - 1;
  if (idx < 0 || idx > 11) return mesISO;
  return `${MES_SHORT[idx]} ${yy}`;
}

/**
 * Fecha de hoy en formato editorial («7 de mayo de 2026»).
 */
export function fechaEditorial(d: Date = new Date()): string {
  return d.toLocaleDateString("es-ES", {
    day: "numeric",
    month: "long",
    year: "numeric",
  });
}
