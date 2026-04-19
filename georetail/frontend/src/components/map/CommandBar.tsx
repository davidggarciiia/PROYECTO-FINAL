"use client";

import { useState, FormEvent } from "react";
import styles from "./CommandBar.module.css";

interface Props {
  onRefine: (text: string) => void;
  chips?: string[];
  loading?: boolean;
}

const DEFAULT_CHIPS = ["Cerca de metro", "Presupuesto bajo", "Público universitario", "Más tranquilas"];

export default function CommandBar({ onRefine, chips = DEFAULT_CHIPS, loading = false }: Props) {
  const [cmd, setCmd] = useState("");

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const trimmed = cmd.trim();
    if (!trimmed || loading) return;
    onRefine(trimmed);
    setCmd("");
  };

  return (
    <>
      <form className={styles.command} onSubmit={submit}>
        <span className={styles.caret}>&rsaquo;</span>
        <input
          value={cmd}
          onChange={(e) => setCmd(e.target.value)}
          placeholder="refina la búsqueda, p.ej. 'cerca de metro' o 'solo presupuesto bajo'"
          disabled={loading}
          aria-label="Refinar búsqueda"
        />
        <button className={styles.go} type="submit" aria-label="Enviar" disabled={loading}>
          {loading ? (
            <div className="spinner" style={{ width: 12, height: 12, borderWidth: 2 }} />
          ) : (
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.4"
              strokeLinecap="round"
            >
              <path d="M5 12h14M12 5l7 7-7 7" />
            </svg>
          )}
        </button>
      </form>

      <div className={styles.chips}>
        {chips.map((s) => (
          <button
            key={s}
            type="button"
            className={styles.chip}
            onClick={() => onRefine(s)}
            disabled={loading}
          >
            + {s}
          </button>
        ))}
      </div>
    </>
  );
}
