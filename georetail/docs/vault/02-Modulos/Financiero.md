# Módulo Financiero

## Archivos

| Archivo | Rol |
|---|---|
| `financiero/estimador.py` | Estimación automática de parámetros |
| `financiero/calculadora.py` | Proyección 36 meses |
| `db/financiero.py` | Persistencia |
| `api/financiero.py` | Endpoint REST |

## Parámetros estimados automáticamente

- Ticket medio
- Clientes/día
- Alquiler mensual
- Salarios
- Coste de reforma

## Override por el usuario

- Sliders en el frontend
- Debounce 300ms
- Proyección se recalcula en tiempo real

## Proyección

- Horizonte: 36 meses
- Incluye break-even, payback period
