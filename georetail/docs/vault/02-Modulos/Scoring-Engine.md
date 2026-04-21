# Scoring Engine

## Archivos clave

| Archivo | Rol |
|---|---|
| `scoring/motor.py` | API pública — único punto de entrada |
| `scoring/scorer.py` | Lógica XGBoost + fallback manual |
| `scoring/features.py` | Vector de features (~70, v1–v15) |
| `scoring/infra/governance.py` | `slice_feature_matrix` — alinea features al modelo |
| `scoring/infra/model_registry.py` | Registro de versiones de modelo |
| `scoring/temporal_readiness.py` | Gate de promoción temporal |
| `scoring/dimensiones/` | Scorers puros por dimensión |
| `scoring/concepto/` | Matching semántico negocio↔concepto |
| `scoring/ml/` | Train, evaluate, hyperparams |

---

## Flujo de scoring

```
motor.py
  └─ scorer.py
       ├─ XGBoost (si modelo activo)
       │    └─ governance.slice_feature_matrix
       └─ fallback: pesos manuales por sector
            ├─ flujo_peatonal (25%)
            ├─ demografia (20%)
            ├─ competencia (15%)
            ├─ transporte (15%)
            ├─ entorno (10%)
            ├─ dinamismo (5%)
            ├─ seguridad (5%)
            └─ turismo (5%)
```

---

## Concepto matching

- Modelo: `paraphrase-multilingual-mpnet-base-v2` (768 dims)
- ~100 conceptos canónicos en `concepto/taxonomy.py`
- `score_afinidad_concepto` = 12% del score final, calculado en runtime
- NUNCA persistido en DB

---

## Cache de scoring

- Incluye versiones seed_v1 y posteriores
- `score_global` solo se recalcula cuando todas las 8 dimensiones manuales están disponibles

---

## Temporal gate

```bash
python -m scoring.temporal_readiness
```
Verifica cobertura histórica de features v14/v15 antes de promover modelo a `activo = TRUE`.
