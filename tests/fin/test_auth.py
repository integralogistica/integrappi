"""Autenticación del módulo: token de sesión, /fin/auth/me y rechazos."""
from datetime import datetime, timedelta, timezone

import jwt
import pytest

from conftest import token_modulo
from financiero.dependencies import crear_token_sesion
from financiero.models.usuario import RolUsuario, Usuario


CORREO = "fin.test@integralogistica.com"


@pytest.fixture()
def usuario_fin(sesion):
    u = Usuario(username="fin.test", nombre="Fin Test", correo=CORREO,
                rol=RolUsuario.JEFE_AREA, area="Tecnología", activo=True)
    sesion.add(u)
    sesion.commit()
    return u


def _auth(token: str):
    return {"Authorization": f"Bearer {token}"}


class TestSesion:
    def test_sesion_con_token_de_modulo(self, cliente, usuario_fin):
        r = cliente.post("/fin/auth/sesion", headers=_auth(token_modulo(CORREO, rol="jefe_area")))
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["token_type"] == "bearer"
        assert data["usuario"]["correo"] == CORREO
        assert data["usuario"]["rol"] == "jefe_area"
        # El token devuelto también sirve
        r2 = cliente.get("/fin/auth/me", headers=_auth(data["access_token"]))
        assert r2.status_code == 200
        assert r2.json()["es_admin"] is False

    def test_me_sin_token_401(self, cliente):
        assert cliente.get("/fin/auth/me").status_code == 401

    def test_token_basura_401(self, cliente, usuario_fin):
        r = cliente.get("/fin/auth/me", headers=_auth("no-es-un-jwt"))
        assert r.status_code == 401

    def test_token_expirado_401(self, cliente, usuario_fin):
        r = cliente.get("/fin/auth/me", headers=_auth(token_modulo(CORREO, horas=-1)))
        assert r.status_code == 401

    def test_correo_no_dado_de_alta_403(self, cliente, usuario_fin):
        r = cliente.get("/fin/auth/me", headers=_auth(token_modulo("nadie@integralogistica.com")))
        assert r.status_code == 403
        assert "No tienes acceso" in r.json()["detail"]

    def test_usuario_inactivo_403_revocacion_inmediata(self, cliente, usuario_fin, sesion):
        usuario_fin.activo = False
        sesion.commit()
        r = cliente.get("/fin/auth/me", headers=_auth(token_modulo(CORREO)))
        assert r.status_code == 403
        assert "desactivado" in r.json()["detail"]


class TestTokenPortal:
    """El JWT del login de IntegrApp (auth_source=baseusuarios)."""

    def _token_portal(self, sub="00000000-0000-0000-0000-00000000abcd", **extra):
        ahora = datetime.now(timezone.utc)
        payload = {
            "sub": sub, "usuario": "x", "perfil": "ADMIN",
            "auth_source": "baseusuarios", "iat": ahora,
            "exp": ahora + timedelta(minutes=20),
        }
        payload.update(extra)
        return jwt.encode(payload, __import__("os").environ["JWT_SECRET"], algorithm="HS256")

    def test_portal_ok(self, cliente, usuario_fin, monkeypatch):
        import financiero.dependencies as deps
        monkeypatch.setattr(deps, "_correo_desde_baseusuarios", lambda sub: {
            "correo": CORREO, "activo": True, "perfil": "COORDINADOR", "nombre": "X",
        })
        r = cliente.get("/fin/auth/me", headers=_auth(self._token_portal()))
        assert r.status_code == 200, r.text
        assert r.json()["perfil_integrapp"] == "COORDINADOR"

    def test_portal_con_claim_modulo_rechazado(self, cliente, usuario_fin, monkeypatch):
        # Filtro anti-suplantación: otros tokens firmados con el mismo secreto
        import financiero.dependencies as deps
        monkeypatch.setattr(deps, "_correo_desde_baseusuarios", lambda sub: {
            "correo": CORREO, "activo": True, "perfil": "ADMIN", "nombre": "X",
        })
        r = cliente.get("/fin/auth/me", headers=_auth(self._token_portal(modulo="registro")))
        assert r.status_code == 401

    def test_portal_usuario_integrapp_inactivo(self, cliente, usuario_fin, monkeypatch):
        import financiero.dependencies as deps
        monkeypatch.setattr(deps, "_correo_desde_baseusuarios", lambda sub: {
            "correo": CORREO, "activo": False, "perfil": "ADMIN", "nombre": "X",
        })
        r = cliente.get("/fin/auth/me", headers=_auth(self._token_portal()))
        assert r.status_code == 403

    def test_portal_sub_desconocido_401(self, cliente, usuario_fin, monkeypatch):
        import financiero.dependencies as deps
        monkeypatch.setattr(deps, "_correo_desde_baseusuarios", lambda sub: None)
        r = cliente.get("/fin/auth/me", headers=_auth(self._token_portal()))
        assert r.status_code == 401


class TestTokenSesionFactory:
    def test_crear_token_sesion_lleva_auth_source_modulo(self, usuario_fin):
        token = crear_token_sesion(usuario_fin)
        payload = jwt.decode(token, __import__("os").environ["JWT_SECRET"], algorithms=["HS256"])
        assert payload["auth_source"] == "fin_modulo"
        assert payload["sub"] == CORREO
