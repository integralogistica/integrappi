#!/usr/bin/env python3
"""
Script para crear el índice en la colección whatsapp_sessions.

Este script debe ejecutarse una sola vez para inicializar el índice
de sesiones de WhatsApp en MongoDB.

Uso:
    python scripts/crear_indice_sesiones.py
"""

import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Funciones.chat_state_integra import create_session_index


def main():
    print("=" * 60)
    print("Creando índice en colección whatsapp_sessions...")
    print("=" * 60)
    
    try:
        create_session_index()
        print("\n✅ Índice creado exitosamente.")
        print("\nLa colección 'whatsapp_sessions' ahora tiene un índice único")
        print("en el campo 'phone' para mejorar el rendimiento.")
    except Exception as e:
        print(f"\n❌ Error creando índice: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()