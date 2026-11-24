"""Administración: usuarios (CRUD, jefe único, importar), bitácora con export,
edición/reapertura/eliminación admin."""
import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_ADMIN = "adminx.fin@integralogistica.com"
CORREO_OPER = "operx.fin@integralogistica.com"


@pytest.fixture()
def admin(sesion):
    u = Usuario(username="adminx.fin", nombre="Admin X", correo=CORREO_ADMIN,
                rol=RolUsuario.ADMIN, activo=True)
    sesion.add(u)
    sesion.commit()
    return u


@pytest.fixture()
def api(engine, admin):
    from rutas.fin_usuarios import router as ru
    from rutas.fin_auditoria import router as ra
    from rutas.fin_admin import router as radm
    from rutas.fin_traslados import router as rt
    app = FastAPI()
    for r in (ru, ra, radm, rt):
        app.include_router(r)
    return TestClient(app)


def _h(correo: str = CORREO_ADMIN) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


class TestUsuarios:
    def test_username_se_deriva_del_correo(self, api):
        # SIN username: el backend lo deriva (usuario solo pide nombre+correo+rol)
        r = api.post("/fin/usuarios", headers=_h(), json={
            "nombre": "Derivado Test",
            "correo": "derivado.test@integralogistica.com", "rol": "presidencia",
        })
        assert r.status_code == 200, r.text
        u = r.json()
        assert u["username"] == "derivado.test"

    def test_crud_y_reglas(self, api):
        # crear
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "presi.fin", "nombre": "Presi Fin",
            "correo": "presi.fin@integralogistica.com", "rol": "presidencia",
        })
        assert r.status_code == 200, r.text
        u = r.json()

        # correo duplicado → 400
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "otro", "nombre": "Otro", "correo": "PRESI.FIN@integralogistica.com", "rol": "consulta"})
        assert r.status_code == 400

        # rol jefe_area sin área → 422
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "jefe.sin.area", "nombre": "X", "correo": "x@integralogistica.com", "rol": "jefe_area"})
        assert r.status_code == 422

        # crear jefe con área
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "jefe.fin", "nombre": "Jefe Fin", "correo": "jefe.fin@integralogistica.com",
            "rol": "jefe_area", "area": "Tecnología"})
        jefe = r.json()

        # segundo jefe del MISMO área → 400
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "jefe2.fin", "nombre": "Jefe 2", "correo": "jefe2.fin@integralogistica.com",
            "rol": "jefe_area", "area": "Tecnología"})
        assert r.status_code == 400

        # desactivar y editar
        r = api.put(f"/fin/usuarios/{u['id']}", headers=_h(), json={"activo": False})
        assert r.json()["activo"] is False
        # ahora sí puede haber otro jefe... (el primero sigue activo: cambiamos su área)
        r = api.put(f"/fin/usuarios/{jefe['id']}", headers=_h(), json={"area": "Compras"})
        assert r.status_code == 200

        # eliminar sin historial
        r = api.delete(f"/fin/usuarios/{u['id']}", headers=_h())
        assert r.json()["eliminado"] is True

        # no se elimina a sí mismo
        r = api.delete("/fin/usuarios/{admin}".format(admin=""), headers=_h())  # placeholder

    def test_usuario_con_historial_no_se_elimina(self, api, sesion, admin):
        # crear jefe y un traslado suyo
        r = api.post("/fin/usuarios", headers=_h(), json={
            "username": "jefe.hist", "nombre": "Jefe Hist", "correo": "jefe.hist@integralogistica.com",
            "rol": "jefe_area", "area": "Operaciones"})
        jefe = r.json()
        r = api.post("/fin/traslados", headers={"Authorization": f"Bearer {token_modulo('jefe.hist@integralogistica.com')}"},
                     json={"lineas": [{"destinoRubro": "R", "monto": 100, "motivo": "m"}]})
        assert r.status_code == 200, r.text
        # eliminar → 409 con registros asociados
        r = api.delete(f"/fin/usuarios/{jefe['id']}", headers=_h())
        assert r.status_code == 409

    def test_no_admin_no_gestiona(self, api, sesion):
        sesion.add(Usuario(username="curioso", nombre="Curioso",
                           correo="curioso@integralogistica.com", rol=RolUsuario.CONSULTA, activo=True))
        sesion.commit()
        r = api.get("/fin/usuarios", headers=_h("curioso@integralogistica.com"))
        assert r.status_code == 403

    def test_admin_adicional_no_crea_ni_toca_admins(self, api, sesion):
        # un admin ADICIONAL (casilla) tiene poder operativo, no de gobierno
        sesion.add(Usuario(username="suplente", nombre="Suplente",
                           correo="suplente@integralogistica.com", rol=RolUsuario.TESORERIA,
                           es_admin_adicional=True, activo=True))
        sesion.commit()
        h_sup = _h("suplente@integralogistica.com")
        # sí puede crear usuarios normales
        r = api.post("/fin/usuarios", headers=h_sup, json={
            "correo": "normalcito@integralogistica.com", "rol": "consulta"})
        assert r.status_code == 200
        # NO puede crear un Administrador Principal
        r = api.post("/fin/usuarios", headers=h_sup, json={
            "correo": "intruso.admin@integralogistica.com", "rol": "admin"})
        assert r.status_code == 403
        # NO puede ascender a nadie a admin
        r = api.get("/fin/usuarios", headers=_h()).json()
        normal = [x for x in r if x["correo"] == "normalcito@integralogistica.com"][0]
        r = api.put(f"/fin/usuarios/{normal['id']}", headers=h_sup, json={"rol": "admin"})
        assert r.status_code == 403
        # y NO puede cambiarle el rol al Principal
        todos = api.get("/fin/usuarios", headers=_h()).json()
        admin_doc = [y for y in todos if y["rol"] == "admin"][0]
        r = api.put(f"/fin/usuarios/{admin_doc['id']}", headers=h_sup, json={"rol": "consulta"})
        assert r.status_code == 403

    def test_importar_excel(self, api):
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Nombre completo", "Usuario", "Correo", "Rol", "Área",
                   "Aprobador pagos urgentes", "Admin adicional", "Autorizador de viajes"])
        ws.append(["Nuevo Uno", "nuevo.uno", "nuevo.uno@integralogistica.com", "tesoreria", "", "SI", "", ""])
        ws.append(["Malo", "x", "sin-correo@@", "rolinexistente", "", "", "", ""])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        r = api.post("/fin/usuarios/importar/vista-previa", headers=_h(),
                     files={"archivo": ("usuarios.xlsx", buf,
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.status_code == 200, r.text
        filas = r.json()
        assert filas[0]["valida"] is True
        assert filas[1]["valida"] is False and len(filas[1]["errores"]) >= 1  # rol inválido (el usuario ya no se exige)

        r = api.post("/fin/usuarios/importar/confirmar", headers=_h(), json={"filas": filas})
        assert r.json()["creados"][0]["username"] == "nuevo.uno"
        assert len(r.json()["omitidos"]) == 1


class TestAuditoria:
    def test_bitacora_y_export(self, api):
        # generar un evento auditable (el admin define área explícita)
        api.post("/fin/traslados", headers={"Authorization": f"Bearer {token_modulo(CORREO_ADMIN)}"},
                 json={"area": "Tecnología", "lineas": [{"destinoRubro": "R2", "monto": 500, "motivo": "m2"}]})
        r = api.get("/fin/auditoria?modulo=traslados", headers=_h())
        assert r.status_code == 200
        assert r.json()["total"] >= 1
        assert r.json()["eventos"][0]["accion"] == "CREAR"

        r = api.get("/fin/auditoria/exportar?modulo=traslados", headers=_h())
        assert r.status_code == 200
        assert "spreadsheetml" in r.headers["content-type"]


class TestAdminRegistros:
    def test_editar_reabrir_y_eliminar(self, api):
        # traslado del admin (área explícita porque el admin no tiene área)
        r = api.post("/fin/traslados", headers=_h(),
                     json={"area": "Tecnología", "lineas": [{"destinoRubro": "R3", "monto": 700, "motivo": "m3"}]})
        assert r.status_code == 200, r.text
        tr = r.json()

        # editar campo de la lista blanca
        r = api.patch(f"/fin/admin/traslados/{tr['id']}", headers=_h(),
                      json={"campos": {"observaciones": "Editado por admin"}, "version": tr["version"],
                            "motivo": "prueba"})
        assert r.status_code == 200, r.text

        # campo fuera de lista blanca → 422
        r = api.patch(f"/fin/admin/traslados/{tr['id']}", headers=_h(),
                      json={"campos": {"consecutivo": "X"}, "motivo": "prueba"})
        assert r.status_code == 422

        # rechazar para poder reabrir
        tr = api.get(f"/fin/traslados/{tr['id']}", headers=_h()).json()
        r = api.post(f"/fin/traslados/{tr['id']}/transicion", headers=_h(),
                     json={"accion": "RECHAZAR", "version": tr["version"], "observacion": "para reabrir"})
        assert r.status_code == 200

        # reabrir
        r = api.post(f"/fin/admin/traslados/{tr['id']}/reabrir?motivo=reabierto%20de%20prueba", headers=_h())
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "PENDIENTE_PRESIDENCIA"

        # eliminar con snapshot
        r = api.delete(f"/fin/admin/traslados/{tr['id']}?motivo=borrado%20de%20prueba", headers=_h())
        assert r.status_code == 200
        # y quedó el snapshot en la bitácora
        r = api.get("/fin/auditoria?modulo=traslados&accion=ELIMINAR", headers=_h())
        assert r.json()["total"] == 1
        assert "snapshot" in r.json()["eventos"][0]["detalles"]
