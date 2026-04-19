"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import styles from "./LandingNav.module.css";

export default function LandingNav() {
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <header className={`${styles.nav} ${scrolled ? styles.scrolled : ""}`}>
      <div className={styles.inner}>
        <Link href="/" className={styles.brand} aria-label="KnowPoints">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src="/logo-wordmark.svg" alt="KnowPoints" height={36} />
        </Link>

        <nav className={styles.links} aria-label="Principal">
          <a href="#producto">Producto</a>
          <a href="#como-funciona">Cómo funciona</a>
          <a href="#testimonios">Testimonios</a>
        </nav>

        <div className={styles.cta}>
          <Link href="/app" className={`${styles.btn} ${styles.primary}`}>
            Probar gratis
          </Link>
        </div>
      </div>
    </header>
  );
}
