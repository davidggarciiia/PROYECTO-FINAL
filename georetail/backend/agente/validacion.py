"""
agente/validacion.py — Validación del tipo de negocio descrito por el usuario.

Re-exporta desde agente/validador.py para compatibilidad con los imports
del endpoint api/buscar.py que usa `from agente.validacion import validar_negocio`.
"""
# DEPRECATED: Este módulo es un wrapper de compatibilidad. Importa directamente desde agente.validador.
from agente.validador import validar_negocio

__all__ = ["validar_negocio"]
