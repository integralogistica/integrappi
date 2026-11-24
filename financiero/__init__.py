"""Módulo Financiero de IntegrApp (2026-11-24).

Réplica interna del módulo externo `ModuloFinanciero-api` (FastAPI+Postgres):
requisiciones, traslados, viajes, adjuntos, contabilidad. Vive en su propio
schema de PostgreSQL (`fin`) y no toca Mongo ni las tablas existentes.

Cada archivo portado lleva un comentario `# puerto de app/...` indicando su
origen en el repositorio externo (que se conserva como referencia viva).
"""
