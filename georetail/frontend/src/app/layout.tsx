import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Knowpoint — Ubica tu negocio en Barcelona",
  description: "Análisis de viabilidad de ubicaciones comerciales en tiempo real. Powered by AI.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="es">
      <body>{children}</body>
    </html>
  );
}
