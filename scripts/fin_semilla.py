"""Siembra inicial del Módulo Financiero (schema `fin`).

Idempotente y re-ejecutable. Carga:
  1. Catálogos desde los datos del módulo externo (static/data/):
     - centros-costo.txt  (~49k líneas  codigo<TAB>nombre)   → fin.cat_centros_costo (COPY)
     - proveedores.json   (~2.7k  [[nit, nombre, ...], ...]) → fin.cat_proveedores
     - presupuesto-responsables.json (12 meses)              → fin.cat_presupuesto_responsables
     - personal-viajes.json ({personal, destinos})           → fin.cat_personal_viajes
  2. Usuario administrador inicial (--admin correo) SOLO si no hay usuarios.
  3. Configuración por defecto (fin.configuracion) para las claves ausentes.

Uso (desde la raíz de integrappi):
  python scripts/fin_semilla.py --admin edwin.zarate@integralogistica.com \
      [--datos "ruta/a/ModuloFinanciero-api/ModuloFinanciero-api/static/data"]

Los catálogos se reemplazan por completo (truncate + insert): son semilla,
no datos operativos.
"""
import argparse
import json
import sys
from pathlib import Path

# Raíz del repo al path (importar financiero.* desde scripts/)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from financiero.database import obtener_session_factory
from financiero.models.usuario import RolUsuario, Usuario
from financiero.models.catalogo import CentroCosto, Proveedor, PresupuestoResponsable, PersonalViaje
from financiero.models.configuracion import Configuracion
from financiero.services.configuracion import CLAVES

RUTA_DATOS_DEFAULT = (
    Path(__file__).resolve().parents[2]
    / "ModuloFinanciero-api" / "ModuloFinanciero-api" / "static" / "data"
)


def sembrar_centros_costo(db, ruta: Path) -> int:
    archivo = ruta / "centros-costo.txt"
    if not archivo.exists():
        print(f"  [!]  {archivo} no existe: se omite centros de costo")
        return 0
    filas = []
    with archivo.open(encoding="utf-8", errors="replace") as f:
        for linea in f:
            linea = linea.rstrip("\n")
            if not linea.strip():
                continue
            partes = linea.split("\t", 1)
            if len(partes) != 2 or not partes[0].strip():
                continue
            filas.append((partes[0].strip(), partes[1].strip()[:200]))
    db.query(CentroCosto).delete()
    # COPY con psycopg2 (rápido para 49k filas)
    con = db.connection().connection  # conexión DBAPI cruda
    with con.cursor() as cur:
        cur.copy_expert(
            "COPY fin.cat_centros_costo (codigo, nombre, area, activo) FROM STDIN WITH (FORMAT csv)",
            _generador_csv(filas),
        )
    return len(filas)


def _generador_csv(filas):
    """Filas (codigo, nombre) → BytesIO CSV para copy_expert."""
    import csv
    import io
    buf = io.StringIO()
    writer = csv.writer(buf)
    for fila in filas:
        writer.writerow([fila[0], fila[1], None, True])
    return io.BytesIO(buf.getvalue().encode("utf-8"))


def sembrar_proveedores(db, ruta: Path) -> int:
    archivo = ruta / "proveedores.json"
    if not archivo.exists():
        print(f"  [!]  {archivo} no existe: se omite proveedores")
        return 0
    datos = json.loads(archivo.read_text(encoding="utf-8-sig"))
    db.query(Proveedor).delete()
    n = 0
    vistos: set[str] = set()  # el archivo de origen trae NITs repetidos
    for fila in datos or []:
        try:
            nit = str(fila[0]).strip() if fila[0] is not None else ""
            nombre = str(fila[1]).strip() if len(fila) > 1 and fila[1] is not None else ""
            if not nit or not nombre or nit in vistos:
                continue
            vistos.add(nit)
            db.add(Proveedor(
                nit=nit[:50],
                razon_social=nombre[:250],
                # formato del archivo: [nit, nombre, "", direccion, telefono, contacto, "", ciudad, flag]
                telefono=(str(fila[4]).strip()[:50] if len(fila) > 4 and fila[4] else None),
            ))
            n += 1
        except Exception:
            continue
    return n


def sembrar_presupuesto(db, ruta: Path, periodo: int | None = None) -> int:
    archivo = ruta / "presupuesto-responsables.json"
    if not archivo.exists():
        print(f"  [!]  {archivo} no existe: se omite presupuesto")
        return 0
    import datetime
    periodo = periodo or datetime.date.today().year
    datos = json.loads(archivo.read_text(encoding="utf-8-sig"))
    db.query(PresupuestoResponsable).filter(PresupuestoResponsable.periodo == periodo).delete()
    n = 0
    for responsable, rubros in (datos or {}).items():
        for rubro, meses in (rubros or {}).items():
            for mes_str, valores in (meses or {}).items():
                try:
                    mes = int(mes_str)
                    if not (1 <= mes <= 12) or not isinstance(valores, (list, tuple)) or len(valores) < 2:
                        continue
                    db.add(PresupuestoResponsable(
                        periodo=periodo,
                        responsable=str(responsable)[:150],
                        rubro=str(rubro)[:250],
                        mes=mes,
                        presupuesto=float(valores[0] or 0),
                        ejecutado=float(valores[1] or 0),
                    ))
                    n += 1
                except (ValueError, TypeError):
                    continue
    return n


def sembrar_personal_viajes(db, ruta: Path) -> int:
    archivo = ruta / "personal-viajes.json"
    if not archivo.exists():
        print(f"  [!]  {archivo} no existe: se omite personal de viajes")
        return 0
    datos = json.loads(archivo.read_text(encoding="utf-8-sig"))
    db.query(PersonalViaje).delete()
    n = 0
    for persona in (datos or {}).get("personal", []):
        nombre = str(persona.get("n", "")).strip()
        if not nombre:
            continue
        db.add(PersonalViaje(
            nombre=nombre[:150],
            cargo=(str(persona.get("cargo", "")).strip()[:150] or None),
            categoria=(str(persona.get("k", "")).strip()[:2].upper() or None),
        ))
        n += 1
    return n


def crear_admin_inicial(db, correo: str) -> bool:
    correo = correo.strip().lower()
    if not correo or "@" not in correo:
        print("  [!]  --admin sin correo válido: no se crea administrador")
        return False
    if db.query(Usuario).count() > 0:
        print("  [i]  Ya existen usuarios: no se crea el administrador inicial")
        return False
    db.add(Usuario(
        username=correo.split("@")[0][:60],
        nombre="Administrador",
        correo=correo,
        rol=RolUsuario.ADMIN,
        activo=True,
    ))
    print(f"  [OK] Administrador inicial: {correo}")
    return True


def sembrar_configuracion(db) -> int:
    n = 0
    for clave, spec in CLAVES.items():
        if db.get(Configuracion, clave) is None:
            db.add(Configuracion(clave=clave, valor={"v": spec["defecto"]}, version=1))
            n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin", default=None, help="Correo del primer administrador (solo si no hay usuarios)")
    parser.add_argument("--datos", default=str(RUTA_DATOS_DEFAULT), help="Carpeta static/data del módulo externo")
    parser.add_argument("--solo-config", action="store_true", help="Solo configuración y admin (sin catálogos)")
    args = parser.parse_args()

    ruta = Path(args.datos)
    factory = obtener_session_factory()
    db = factory()
    try:
        if not args.solo_config:
            print(f"Sembrando catálogos desde {ruta} ...")
            print(f"  [OK] Centros de costo: {sembrar_centros_costo(db, ruta)}")
            print(f"  [OK] Proveedores: {sembrar_proveedores(db, ruta)}")
            print(f"  [OK] Presupuesto responsables: {sembrar_presupuesto(db, ruta)}")
            print(f"  [OK] Personal viajes: {sembrar_personal_viajes(db, ruta)}")
        if args.admin:
            crear_admin_inicial(db, args.admin)
        print(f"  [OK] Configuración (claves nuevas): {sembrar_configuracion(db)}")
        db.commit()
        print("Siembra completa.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
