"""Usuarios del Módulo Financiero (administrador).

# puerto de app/routers/usuarios.py del módulo externo, simplificado al modo
portal: sin claves (la contraseña es la de IntegrApp), el correo es SIEMPRE
obligatorio (es la identidad con la que la persona entra) y el `username` se
DERIVA automáticamente del correo — nadie tiene que inventárselo.
"""
import io
import re
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy import exists, func, select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin, Base
from financiero.dependencies import require_admin
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services.audit_service import AuditService
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/usuarios", tags=["Financiero · Usuarios"])


def _derivar_username(correo: str, db: Session) -> str:
    """Username interno a partir del correo (solo [a-z0-9._-], único)."""
    base = re.sub(r"[^a-z0-9._-]+", ".", correo.split("@")[0].lower()).strip(".-")
    base = (base[:40] or "usuario")
    candidato, n = base, 2
    while db.scalar(select(Usuario).where(Usuario.username == candidato)):
        candidato = f"{base}{n}"
        n += 1
    return candidato


def _nombre_desde_baseusuarios(correo: str) -> Optional[str]:
    """El nombre vive en IntegrApp (baseusuarios), no se pide: se lee de ahí.
    Si Mongo no está disponible (tests), devuelve None y el llamador usa su
    fallback."""
    try:
        from bd.bd_cliente import bd_cliente
        col = bd_cliente["integra"]["baseusuarios"]
        doc = col.find_one({"correo": {"$regex": f"^{re.escape(correo.strip().lower())}$", "$options": "i"}})
        return (doc.get("nombre") or "").strip() if doc else None
    except Exception:
        return None


class UsuarioPayload(BaseModel):
    # Opcional: si no viene, se deriva del correo. Nadie debe inventárselo.
    username: Optional[str] = Field(None, pattern=r"^[a-z0-9._-]{3,}$")
    # Opcional: el nombre se toma de baseusuarios por el correo; esto es solo
    # fallback (tests sin Mongo).
    nombre: Optional[str] = Field(None, min_length=2)
    correo: str = Field(..., min_length=5)   # obligatorio: es la identidad de IntegrApp
    rol: RolUsuario
    area: Optional[str] = None
    activo: bool = True
    es_aprobador_pagos: bool = False
    es_admin_adicional: bool = False
    es_autorizador_viajes: bool = False
    # {seccion: true|false} — solo los overrides del rol; vacío = rol por defecto
    permisos: Optional[dict] = None


class UsuarioUpdate(BaseModel):
    correo: Optional[str] = None
    rol: Optional[RolUsuario] = None
    area: Optional[str] = None
    activo: Optional[bool] = None
    es_aprobador_pagos: Optional[bool] = None
    es_admin_adicional: Optional[bool] = None
    es_autorizador_viajes: Optional[bool] = None
    permisos: Optional[dict] = None


def _validar_permisos(permisos: Optional[dict]) -> Optional[dict]:
    """Solo secciones conocidas con booleanos; None/{} = heredar del rol."""
    from financiero.services.permisos import SECCIONES_FIN
    if permisos is None:
        return None
    if not isinstance(permisos, dict):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "permisos debe ser {seccion: true|false}.")
    invalidas = set(permisos) - set(SECCIONES_FIN)
    if invalidas:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"Secciones desconocidas: {', '.join(sorted(invalidas))}. "
                            f"Válidas: {', '.join(SECCIONES_FIN)}")
    for v in permisos.values():
        if not isinstance(v, bool):
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "permisos debe ser {seccion: true|false}.")
    return permisos or None


def _dict(u: Usuario) -> dict:
    from financiero.services.permisos import secciones_efectivas
    return {
        "id": str(u.id), "username": u.username, "nombre": u.nombre, "correo": u.correo,
        "rol": u.rol.value, "area": u.area, "activo": u.activo,
        "es_aprobador_pagos": u.es_aprobador_pagos, "es_admin_adicional": u.es_admin_adicional,
        "es_autorizador_viajes": u.es_autorizador_viajes,
        "permisos": u.permisos or {},
        "secciones_efectivas": sorted(secciones_efectivas(u)),
    }


@router.get("")
def listar_usuarios(
    solo_activos: bool = False,
    rol: Optional[RolUsuario] = None,
    area: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(require_admin),
):
    stmt = select(Usuario)
    if solo_activos:
        stmt = stmt.where(Usuario.activo == True)  # noqa: E712
    if rol:
        stmt = stmt.where(Usuario.rol == rol)
    if area:
        stmt = stmt.where(Usuario.area == area)
    return [_dict(u) for u in db.scalars(stmt.order_by(Usuario.nombre.asc())).all()]


@router.get("/candidatos")
def buscar_candidatos(
    q: str = "",
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(require_admin),
):
    """Candidatos a dar de alta: usuarios de IntegrApp con perfil FINANCIERO
    (activo y con correo), leídos de baseusuarios. Se marca `en_modulo` a los
    que ya existen en fin.usuarios para no duplicarlos."""
    try:
        from bd.bd_cliente import bd_cliente
        col = bd_cliente["integra"]["baseusuarios"]
        import re as _re
        filtro: dict = {
            "perfil": "FINANCIERO",
            "$or": [{"activo": True}, {"activo": {"$exists": False}}],
            "correo": {"$exists": True, "$nin": [None, ""]},
        }
        if q and q.strip():
            esc = _re.escape(q.strip())
            filtro["$or"] = [
                {"correo": {"$regex": esc, "$options": "i"}},
                {"nombre": {"$regex": esc, "$options": "i"}},
            ]
        docs = col.find(filtro, {"correo": 1, "nombre": 1, "usuario": 1, "celular": 1}).limit(25)
    except Exception as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            f"No se pudo consultar los usuarios de IntegrApp: {exc}")
    ya_en_modulo = {u.correo.lower() for u in db.query(Usuario).all() if u.correo}
    out = []
    for d in docs:
        correo = (d.get("correo") or "").strip().lower()
        if not correo or "@" not in correo:
            continue
        out.append({
            "correo": correo,
            "nombre": (d.get("nombre") or d.get("usuario") or "").strip(),
            "celular": d.get("celular"),
            "en_modulo": correo in ya_en_modulo,
        })
    out.sort(key=lambda c: c["en_modulo"])  # los disponibles primero
    return out


@router.post("")
def crear_usuario(req: UsuarioPayload, request: Request,
                  db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    # Regla: solo el Administrador Principal otorga 'admin adicional'
    if req.es_admin_adicional and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "Solo el Administrador Principal puede conceder la casilla de 'Admin Adicional'.")
    # Regla: crear otro Administrador Principal también es cosa del Principal
    # (si un admin adicional necesita poder pleno, se le da la casilla).
    if req.rol == RolUsuario.ADMIN and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "Solo el Administrador Principal puede crear otro Administrador Principal. "
                            "Para poder pleno usa la casilla 'Admin adicional'.")
    correo = req.correo.strip().lower()
    username = (req.username or "").strip().lower() or _derivar_username(correo, db)
    # El nombre viene de IntegrApp (baseusuarios); fallback solo para tests.
    nombre = _nombre_desde_baseusuarios(correo) or (req.nombre or "").strip() or correo.split("@")[0]
    if db.scalar(select(Usuario).where(Usuario.username == username)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El usuario '{username}' ya existe.")
    if db.scalar(select(Usuario).where(func.lower(Usuario.correo) == correo)):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El correo '{correo}' ya está registrado.")
    if req.rol == RolUsuario.JEFE_AREA and req.activo:
        existente = db.scalar(select(Usuario).where(
            Usuario.rol == RolUsuario.JEFE_AREA, Usuario.area == req.area, Usuario.activo == True))  # noqa: E712
        if existente:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                f"Ya existe un Jefe de Área activo para '{req.area}' ({existente.nombre}).")
    if req.rol in (RolUsuario.REQUISICION, RolUsuario.JEFE_AREA) and not (req.area or "").strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"El rol '{req.rol.value}' exige área.")

    nuevo = Usuario(
        username=username, nombre=nombre, correo=correo,
        rol=req.rol, area=(req.area or None), activo=req.activo,
        es_aprobador_pagos=req.es_aprobador_pagos, es_admin_adicional=req.es_admin_adicional,
        es_autorizador_viajes=req.es_autorizador_viajes, permisos=_validar_permisos(req.permisos),
        created_by=admin.username,
    )
    db.add(nuevo)
    db.commit()
    db.refresh(nuevo)
    AuditService.registrar(db=db, modulo="usuarios", accion="CREAR", entidad_id=str(nuevo.id), usuario=admin,
                           ip_cliente=get_client_ip(request),
                           detalles={"username": nuevo.username, "rol": nuevo.rol.value, "area": nuevo.area})
    return _dict(nuevo)


@router.put("/{id}")
def actualizar_usuario(id: uuid.UUID, req: UsuarioUpdate, request: Request,
                       db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    u = db.get(Usuario, id)
    if not u:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado.")
    if req.es_admin_adicional and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "Solo el Administrador Principal puede conceder la casilla de 'Admin Adicional'.")
    # Regla: nombrar o degradar a un Administrador Principal es cosa del
    # Principal (un admin adicional no puede ascender a nadie ni tocar a un admin).
    if req.rol == RolUsuario.ADMIN and u.rol != RolUsuario.ADMIN and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "Solo el Administrador Principal puede nombrar otro Administrador Principal.")
    if u.rol == RolUsuario.ADMIN and req.rol is not None and req.rol != RolUsuario.ADMIN and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            "Solo el Administrador Principal cambia el rol de otro administrador.")
    if req.correo is not None:
        correo = req.correo.strip().lower()
        dup = db.scalar(select(Usuario).where(func.lower(Usuario.correo) == correo, Usuario.id != u.id))
        if dup:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El correo '{correo}' ya está registrado.")
        u.correo = correo
    # El nombre SIEMPRE se resincroniza con IntegrApp (fuente de verdad);
    # si Mongo no responde, conserva el que tenía.
    nombre_bd = _nombre_desde_baseusuarios(u.correo)
    if nombre_bd:
        u.nombre = nombre_bd
    if req.rol is not None: u.rol = req.rol
    if req.area is not None: u.area = req.area or None
    if req.activo is not None: u.activo = req.activo
    if req.es_aprobador_pagos is not None: u.es_aprobador_pagos = req.es_aprobador_pagos
    if req.es_admin_adicional is not None: u.es_admin_adicional = req.es_admin_adicional
    if req.es_autorizador_viajes is not None: u.es_autorizador_viajes = req.es_autorizador_viajes
    if req.permisos is not None: u.permisos = _validar_permisos(req.permisos)
    if u.rol in (RolUsuario.REQUISICION, RolUsuario.JEFE_AREA) and not (u.area or "").strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"El rol '{u.rol.value}' exige área.")
    if u.rol == RolUsuario.JEFE_AREA and u.activo:
        existente = db.scalar(select(Usuario).where(
            Usuario.rol == RolUsuario.JEFE_AREA, Usuario.area == u.area,
            Usuario.activo == True, Usuario.id != u.id))  # noqa: E712
        if existente:
            raise HTTPException(status.HTTP_400_BAD_REQUEST,
                                f"Ya existe un Jefe de Área activo para '{u.area}' ({existente.nombre}).")
    u.updated_by = admin.username
    db.commit()
    db.refresh(u)
    AuditService.registrar(db=db, modulo="usuarios", accion="MODIFICAR", entidad_id=str(u.id), usuario=admin,
                           ip_cliente=get_client_ip(request),
                           detalles={"username": u.username, "rol": u.rol.value, "activo": u.activo})
    return _dict(u)


@router.delete("/{id}")
def eliminar_usuario(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                     admin: Usuario = Depends(require_admin)):
    """Elimina SIN historial. Si ya participó en registros, la base lo impide
    (409): en ese caso se desactiva (PUT activo=false) para conservar trazabilidad."""
    from sqlalchemy.exc import IntegrityError
    u = db.get(Usuario, id)
    if not u:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado.")
    if u.id == admin.id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No puedes eliminar tu propio usuario.")
    if u.rol == RolUsuario.ADMIN and u.activo:
        otros = db.scalar(select(func.count()).select_from(Usuario).where(
            Usuario.rol == RolUsuario.ADMIN, Usuario.activo == True, Usuario.id != u.id))  # noqa: E712
        if not otros:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "No puedes eliminar al único administrador activo.")
    if u.rol == RolUsuario.ADMIN and admin.rol != RolUsuario.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el Administrador Principal elimina administradores.")
    detalle = {"username": u.username, "nombre": u.nombre, "rol": u.rol.value, "area": u.area, "correo": u.correo}
    # Revisión explícita de TODAS las tablas que apuntan al usuario (no depende del motor)
    for tabla in Base.metadata.sorted_tables:
        for fk in tabla.foreign_keys:
            if fk.column.table.name == "usuarios" and db.scalar(
                    select(exists().where(fk.parent == u.id))):
                raise HTTPException(status.HTTP_409_CONFLICT,
                                    "Este usuario tiene registros asociados. Desactívalo en lugar de eliminarlo.")
    try:
        db.delete(u)
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT,
                            "Este usuario tiene registros asociados. Desactívalo en lugar de eliminarlo.")
    AuditService.registrar(db=db, modulo="usuarios", accion="ELIMINAR", entidad_id=str(id), usuario=admin,
                           ip_cliente=get_client_ip(request), detalles=detalle)
    return {"eliminado": True, "username": detalle["username"]}


# ── Importación masiva con vista previa ───────────────────────────────────

_COL_MAP = {
    "nombre completo": "nombre", "usuario": "username", "correo": "correo", "rol": "rol",
    "área": "area", "area": "area",
    "aprobador pagos urgentes": "es_aprobador_pagos", "admin adicional": "es_admin_adicional",
    "autorizador de viajes": "es_autorizador_viajes",
}
# campo → lista de encabezados posibles en el archivo (original usa los mismos alias)
_CAMPO_A_COLUMNAS = {
    campo: [es for es, c in _COL_MAP.items() if c == campo] for campo in set(_COL_MAP.values())
}


@router.post("/importar/vista-previa")
async def previsualizar_importacion(archivo: UploadFile = File(...),
                                    admin: Usuario = Depends(require_admin),
                                    db: Session = Depends(get_db_fin)):
    """Lee Excel/CSV y valida fila por fila ANTES de guardar nada."""
    contenido = await archivo.read()
    if len(contenido) > 5 * 1024 * 1024:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "El archivo supera 5 MB.")
    import pandas as pd
    try:
        df = pd.read_csv(io.BytesIO(contenido)) if archivo.filename.lower().endswith(".csv") \
            else pd.read_excel(io.BytesIO(contenido))
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Formato de archivo inválido: {e}")

    columnas = {str(c).lower().strip(): c for c in df.columns}
    filas = []
    for idx, row in df.iterrows():
        def g(campo, _row=row, _cols=columnas):
            v = ""
            for enc in _CAMPO_A_COLUMNAS.get(campo, [campo]):
                col = _cols.get(enc)
                if col:
                    v = _row.get(col, "")
                    break
            return "" if v is None or (isinstance(v, float) and v != v) else str(v).strip()
        nombre = g("nombre")
        username = g("username").lower()  # opcional: si viene vacío se deriva del correo
        correo = g("correo").lower()
        rol = g("rol").lower()
        area = g("area")
        errores = []
        if not nombre:
            errores.append("Nombre obligatorio.")
        if rol not in [r.value for r in RolUsuario]:
            errores.append(f"Rol '{rol}' no reconocido.")
        elif rol in ("requisicion", "jefe_area") and not area:
            errores.append(f"Área obligatoria para el rol '{rol}'.")
        if not correo:
            errores.append("Correo obligatorio (es el mismo de IntegrApp).")
        elif db.scalar(select(Usuario).where(func.lower(Usuario.correo) == correo)):
            errores.append(f"El correo '{correo}' ya está registrado.")
        if username and db.scalar(select(Usuario).where(Usuario.username == username)):
            errores.append(f"El usuario '{username}' ya está registrado.")
        filas.append({
            "fila": idx + 2,
            "valida": not errores,
            "datos": {"nombre": nombre, "username": username, "correo": correo, "rol": rol, "area": area},
            "errores": errores,
        })
    return filas


class ImportarConfirmar(BaseModel):
    filas: list[dict]


@router.post("/importar/confirmar")
def confirmar_importacion(req: ImportarConfirmar, request: Request,
                          admin: Usuario = Depends(require_admin), db: Session = Depends(get_db_fin)):
    """Crea los usuarios válidos de la vista previa (sin claves: modo portal)."""
    creados, omitidos = [], []
    vistos_user, vistos_mail = set(), set()
    jefes_en_archivo = set()
    for i, d in enumerate(req.filas, start=1):
        datos = d.get("datos", d)
        try:
            rol = RolUsuario(datos.get("rol", ""))
            username = str(datos.get("username", "")).strip().lower() or None  # se deriva si falta
            correo = str(datos.get("correo", "")).strip().lower()
            nombre = str(datos.get("nombre", "")).strip()
            area = str(datos.get("area", "")).strip() or None
        except ValueError:
            omitidos.append({"fila": i, "motivo": "Rol inválido"})
            continue
        if not (correo and nombre):
            omitidos.append({"fila": i, "usuario": username or "", "motivo": "Datos incompletos (nombre y correo obligatorios)"})
            continue
        if not username:
            username = _derivar_username(correo, db)
        # El nombre manda el de IntegrApp; el del Excel es solo fallback.
        nombre = _nombre_desde_baseusuarios(correo) or nombre or correo.split("@")[0]
        if username in vistos_user or correo in vistos_mail \
                or db.scalar(select(Usuario).where(Usuario.username == username)) \
                or db.scalar(select(Usuario).where(func.lower(Usuario.correo) == correo)):
            omitidos.append({"fila": i, "usuario": username, "motivo": "Usuario o correo duplicado."})
            continue
        if rol == RolUsuario.JEFE_AREA:
            if area in jefes_en_archivo or db.scalar(select(Usuario).where(
                    Usuario.rol == RolUsuario.JEFE_AREA, Usuario.area == area, Usuario.activo == True)):  # noqa: E712
                omitidos.append({"fila": i, "usuario": username, "motivo": f"Ya hay un jefe de área activo en '{area}'."})
                continue
            jefes_en_archivo.add(area)
        db.add(Usuario(username=username, nombre=nombre, correo=correo, rol=rol, area=area,
                       activo=True, created_by=admin.username))
        vistos_user.add(username)
        vistos_mail.add(correo)
        creados.append({"username": username, "nombre": nombre})
    db.commit()
    AuditService.registrar(db=db, modulo="usuarios", accion="IMPORTAR", usuario=admin,
                           ip_cliente=get_client_ip(request),
                           detalles={"creados": len(creados), "omitidos": len(omitidos)})
    return {"creados": creados, "omitidos": omitidos}
