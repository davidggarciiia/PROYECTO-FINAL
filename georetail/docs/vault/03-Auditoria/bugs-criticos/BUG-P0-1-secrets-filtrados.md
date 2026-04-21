# BUG-P0-1: Secrets filtrados en `.claude/settings.local.json`

**Fecha:** 2026-04-22
**Severidad:** CRITICA
**Estado:** Parcialmente resuelto (gitignore añadido; rotación pendiente al usuario)
**Archivo:** `.claude/settings.local.json`

---

## Descripción

El archivo `.claude/settings.local.json` contenía claves de API en texto plano
dentro de su lista `permissions.allow` (comandos preaprobados para Bash):

```
Google Maps API:  AIzaSyCs0xfMeP8q5cXn9pnzFJrFStnokFNq3_E
TMB app_id:       9572a739
TMB app_key:      cfaea8fd10f68944261ab38553009f09
IERMB tokens:     1656400815-7880, 1656332618-6952, 1652100789-7334
```

Las URLs completas (incluyendo los tokens como query-string) quedaron almacenadas
por el harness como patrones de allowlist, convirtiendo el archivo en una fuga
de credenciales.

## Impacto

- **Google Maps** sin restricciones de dominio → facturas potenciales de miles
  de € si la clave se publica en un mirror/fork.
- **TMB app_id/app_key** → acceso al API de transporte metropolitano.
- **IERMB tokens** → URLs de descarga temporales (ya expiradas, pero de uso
  interno).

Cualquier persona con read access al repo (o a una copia) tenía las claves.

## Causa raíz

`.claude/settings.local.json` no estaba en `.gitignore` y el harness de Claude
Code persistía las URLs completas de los comandos aprobados con `!` durante
sesiones interactivas.

## Fix aplicado (acciones inmediatas en repo)

1. **`.gitignore` actualizado** — se añade `.claude/settings.local.json` para
   que futuras versiones del archivo no se suban al repo.
2. El archivo actual **sigue en el working tree** (puede estar ya en el
   historial de git). La limpieza de historial (`git filter-repo` / BFG)
   requiere decisión del usuario porque reescribe commits.

## Acciones PENDIENTES del usuario (no automatizables)

1. **Rotar las 3 claves** en los paneles correspondientes:
   - Google: https://console.cloud.google.com → APIs & Services → Credenciales
     → revocar `AIzaSyCs0xfMeP8q5cXn9pnzFJrFStnokFNq3_E` y crear nueva.
   - TMB: https://developer.tmb.cat → regenerar `app_id=9572a739` /
     `app_key=cfaea8fd10f68944261ab38553009f09`.
   - IERMB: los tokens son temporales, verificar que ya no son válidos.
2. **Restringir** la nueva Google Maps API key a los dominios de producción +
   `localhost`.
3. **Limpieza de historial** (opcional, dependiendo de si el archivo llegó a
   git): `git filter-repo --path .claude/settings.local.json --invert-paths`
   + `git push --force` (requiere coordinación con cualquier clon remoto).

## Verificación

```bash
grep -E "^\.claude/settings\.local\.json$" .gitignore   # → match
git check-ignore .claude/settings.local.json            # → ignored
```

## Notas

- Dejamos el archivo en disco porque el harness de Claude Code lo consulta
  localmente (contiene la allowlist de comandos). Sólo queremos que no siga a
  git.
- A futuro: considerar un pre-commit hook que escanee claves antes de cada
  commit (detect-secrets, git-secrets, gitleaks).

---

**Tags:** #bug #critico #seguridad #secrets #p0
