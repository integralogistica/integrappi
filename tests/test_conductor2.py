"""
Tests del SEGUNDO CONDUCTOR (2026-10-14): slot 2 opcional con HV completa.

Cubre:
- invitar-conductor con slot (exige slot 1, no pisa en silencio, unicidad,
  vinculacion directa de cuenta activa, solicitante de Seguridad).
- aceptar-invitacion derivando el slot por correo (links viejos sin slot).
- desvincular-conductor por slot (limpia vinculación + firma, conserva cond2*).
- _documentos_faltantes: los 6 docs del 2º SOLO si idConductor2 vinculado.
- Firma por slot: llenar cond2* NO rompe el hash del slot 1; verificar slot 2.
- sujetos_estudio: rol conductor2 + dedup con el conductor 1.
"""
import unittest
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

from rutas import conductores
from rutas import vehiculos
from Funciones import estudios_automaticos
from Funciones.claves import crear_hash


class FakeCursor:
    def __init__(self, items):
        self.items = items

    def limit(self, n):
        return self

    def sort(self, *a, **k):
        return self

    def __iter__(self):
        return iter(self.items)


class FakeColeccion:
    """Colección fake con soporte de igualdad, $regex, $ne, $or, $in y $set
    con notación punto (invitacionConductor2.expira)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    def find(self, query=None, *a, **k):
        return FakeCursor([d for d in self.documents if self._match(d, query or {})])

    def find_one(self, query=None, *a, **k):
        if query is None:
            return self.documents[0] if self.documents else None
        for d in self.documents:
            if self._match(d, query):
                return d
        return None

    @classmethod
    def _match(cls, doc, query):
        for k, v in query.items():
            if not cls._match_campo(doc, k, v):
                return False
        return True

    @classmethod
    def _match_campo(cls, doc, k, v):
        if k == "$or":
            return any(cls._match(doc, sub) for sub in v)
        if k == "_id":
            return doc.get("_id") == v
        if isinstance(v, dict):
            import re as _re
            valor = doc.get(k)
            if "$regex" in v:
                flags = _re.IGNORECASE if "i" in v.get("$options", "") else 0
                return bool(_re.match(v["$regex"], str(valor or ""), flags))
            if "$ne" in v:
                return valor != v["$ne"]
            if "$in" in v:
                return valor in v["$in"]
            if "$exists" in v:
                return (k in doc) == bool(v["$exists"])
            return False
        return doc.get(k) == v

    def insert_one(self, doc):
        doc = dict(doc)
        doc.setdefault("_id", ObjectId())
        self.documents.append(doc)

        class R:
            inserted_id = doc["_id"]
        return R()

    def update_one(self, filtro, cambio):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if self._match(d, filtro):
                if "$set" in cambio:
                    for k, v in cambio["$set"].items():
                        if "." in k:
                            raiz, hijo = k.split(".", 1)
                            d.setdefault(raiz, {})[hijo] = v
                        else:
                            d[k] = v

    def count_documents(self, query=None):
        return len([d for d in self.documents if self._match(d, query or {})])


class FakeRequest:
    def __init__(self):
        self.client = type("C", (), {"host": "127.0.0.1"})()
        self.headers = {"user-agent": "test-agent"}


class BackgroundTasksFake:
    def add_task(self, func, *args, **kwargs):
        pass


def _vehiculo_base(**extra):
    doc = {
        "placa": "XYZ789", "idUsuario": "ten-1", "estadoIntegra": "registro_incompleto",
        "idConductor": "cond-1", "invitacionConductor": {"correo": "UNO@correo.com", "estado": "aceptada"},
        "idConductor2": None, "invitacionConductor2": None,
        "condNombres": "ANA", "condCedulaCiudadania": "111",
    }
    doc.update(extra)
    return doc


class InvitarConductor2Tests(unittest.TestCase):
    def setUp(self):
        self.cuentas = FakeColeccion()
        self.vehiculos = FakeColeccion([_vehiculo_base()])

    def _patchear(self):
        return [
            patch.object(conductores, "coleccion_conductores", self.cuentas),
            patch.object(conductores, "coleccion_vehiculos", self.vehiculos),
            patch.object(conductores, "_pos_cambio_conductor"),
        ]

    def _invitar(self, **kwargs):
        import asyncio
        from rutas.conductores import InvitarConductorInput
        kwargs.setdefault("id_tenedor", "ten-1")
        datos = InvitarConductorInput(placa="XYZ789", **kwargs)
        return asyncio.run(conductores.invitar_conductor(datos, BackgroundTasksFake()))

    def test_slot2_exige_slot1(self):
        self.vehiculos.documents[0]["idConductor"] = None
        self.vehiculos.documents[0]["invitacionConductor"] = None
        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._invitar(correo_conductor="dos@correo.com", slot=2)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_slot_ocupado_no_se_pisa(self):
        """El slot 2 ocupado (vinculado O invitado) da 409, no pisa en silencio."""
        from contextlib import ExitStack
        self.vehiculos.documents[0]["idConductor2"] = "cond-2"
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._invitar(correo_conductor="otro@correo.com", slot=2)
        self.assertEqual(ctx.exception.status_code, 409)
        # Igual con solo invitación pendiente en el slot 2.
        self.vehiculos.documents[0]["idConductor2"] = None
        self.vehiculos.documents[0]["invitacionConductor2"] = {
            "correo": "PEND@correo.com", "estado": "pendiente"}
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._invitar(correo_conductor="otro@correo.com", slot=2)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_mismo_correo_en_ambos_slots_rechaza(self):
        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._invitar(correo_conductor="uno@correo.com", slot=2)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_cuenta_activa_vincula_directo_slot2(self):
        from contextlib import ExitStack
        id_cuenta = ObjectId()
        self.cuentas.documents.append({
            "_id": id_cuenta, "correo": "DOS@correo.com", "perfil": "CONDUCTOR",
            "activo": True, "correo_verificado": True, "nombre": "BETO",
        })
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patchear()]
            resultado = self._invitar(correo_conductor="dos@correo.com", slot=2)
        self.assertEqual(resultado["estado"], "vinculado")
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertEqual(veh["idConductor2"], str(id_cuenta))
        self.assertEqual(veh["invitacionConductor2"]["estado"], "aceptada")
        # Efectos post-vinculación (auditoría + downgrade) SÍ se disparan.
        self.assertTrue(mocks[-1].called)

    def test_cuenta_ya_conductor2_de_otra_placa_rechaza(self):
        from contextlib import ExitStack
        id_cuenta = ObjectId()
        self.cuentas.documents.append({
            "_id": id_cuenta, "correo": "DOS@correo.com", "perfil": "CONDUCTOR",
            "activo": True, "correo_verificado": True,
        })
        self.vehiculos.documents.append({
            "placa": "OTRA01", "idUsuario": "ten-9", "idConductor2": str(id_cuenta),
        })
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._invitar(correo_conductor="dos@correo.com", slot=2)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_stub_nuevo_slot2_deja_invitacion_pendiente(self):
        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in self._patchear():
                stack.enter_context(p)
            resultado = self._invitar(correo_conductor="nuevo2@correo.com", slot=2)
        self.assertEqual(resultado["estado"], "invitado")
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertIsNone(veh["idConductor2"])
        self.assertEqual(veh["invitacionConductor2"]["correo"], "NUEVO2@CORREO.COM")
        self.assertEqual(veh["invitacionConductor2"]["estado"], "pendiente")

    def test_solicitante_seguridad_omite_check_de_tenedor(self):
        """Seguridad invita sin ser el tenedor declarado (id_tenedor falso)."""
        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patchear()]
            resultado = self._invitar(
                id_tenedor="cualquiera", correo_conductor="seg@correo.com",
                slot=2, solicitante="Diana Seguridad")
        self.assertEqual(resultado["estado"], "invitado")
        # Auditoría con via=seguridad (4º argumento posicional).
        self.assertEqual(mocks[-1].call_args.args[3], "seguridad")


class AceptarInvitacion2Tests(unittest.TestCase):
    def setUp(self):
        self.stub_id = ObjectId()
        self.cuentas = FakeColeccion([{
            "_id": self.stub_id, "correo": "DOS@correo.com", "perfil": "CONDUCTOR",
            "nombre": "BETO", "activo": False, "correo_verificado": False,
        }])
        self.vehiculos = FakeColeccion([_vehiculo_base(
            idConductor2=None,
            invitacionConductor2={"correo": "DOS@correo.com", "estado": "pendiente"},
        )])
        politicas = FakeColeccion([{
            "_id": ObjectId(), "version": 1, "titulo": "T", "texto_html": "<p>x</p>",
            "activo": True, "declaraciones": conductores.DECLARACIONES_V2,
        }])
        self.patches = [
            patch.object(conductores, "coleccion_conductores", self.cuentas),
            patch.object(conductores, "coleccion_vehiculos", self.vehiculos),
            patch.object(conductores, "coleccion_politicas", politicas),
            patch.object(conductores, "coleccion_aceptaciones", FakeColeccion()),
            patch.object(conductores, "_pos_cambio_conductor"),
            patch.object(vehiculos, "_disparar_estudios_seguridad"),
        ]

    def _aceptar(self, **overrides):
        import asyncio
        from rutas.conductores import AceptarInvitacionInput
        ids = [d["id"] for d in conductores.DECLARACIONES_V2]
        datos = AceptarInvitacionInput(
            token="tok", placa="XYZ789", clave="secreta1", version_politica=1,
            acepta=True, declaraciones_aceptadas=ids, **overrides)
        with patch.object(conductores, "_buscar_conductor_por_token",
                          return_value=self.cuentas.documents[0]):
            from contextlib import ExitStack
            with ExitStack() as stack:
                for p in self.patches:
                    stack.enter_context(p)
                return asyncio.run(conductores.aceptar_invitacion(datos, FakeRequest()))

    def test_acepta_slot2_por_link(self):
        resultado = self._aceptar(slot=2)
        self.assertEqual(resultado["slot"], 2)
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertEqual(veh["idConductor2"], str(self.stub_id))
        self.assertEqual(veh["invitacionConductor2"]["estado"], "aceptada")
        # El slot 1 queda intacto.
        self.assertEqual(veh["idConductor"], "cond-1")

    def test_deriva_slot_por_correo_sin_parametro(self):
        """Links viejos sin slot: el correo del stub coincide con la invitación 2."""
        resultado = self._aceptar()
        self.assertEqual(resultado["slot"], 2)
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertEqual(veh["idConductor2"], str(self.stub_id))


class DesvincularConductor2Tests(unittest.TestCase):
    def setUp(self):
        self.vehiculos = FakeColeccion([_vehiculo_base(
            idConductor2="cond-2",
            invitacionConductor2={"correo": "DOS@correo.com", "estado": "aceptada"},
            cond2Nombres="BETO", cond2CedulaCiudadania="222",
            licencia2="Vehiculos/XYZ789/2026-10-14/licencia2_xyz789.pdf",
            firmaUrl2="Vehiculos/XYZ789/2026-10-14/firma2_xyz789.webp",
            firmaEvidencia2={"hash_datos": "abc", "version": 1},
        )])
        self.patches = [
            patch.object(conductores, "coleccion_conductores", FakeColeccion()),
            patch.object(conductores, "coleccion_vehiculos", self.vehiculos),
            patch.object(conductores, "_pos_cambio_conductor"),
        ]

    def _desvincular(self, **overrides):
        import asyncio
        from rutas.conductores import DesvincularConductorInput
        overrides.setdefault("id_tenedor", "ten-1")
        datos = DesvincularConductorInput(placa="XYZ789", **overrides)
        from contextlib import ExitStack
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self.patches]
            resultado = asyncio.run(conductores.desvincular_conductor(datos))
        return resultado, mocks

    def test_desvincula_slot2_limpia_firma_conserva_datos(self):
        resultado, _ = self._desvincular(slot=2)
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertIsNone(veh["idConductor2"])
        self.assertIsNone(veh["invitacionConductor2"])
        self.assertIsNone(veh["firmaUrl2"])
        self.assertIsNone(veh["firmaEvidencia2"])
        # Los datos y documentos quedan como historial.
        self.assertEqual(veh["cond2Nombres"], "BETO")
        self.assertTrue(veh["licencia2"])
        # El slot 1 queda intacto.
        self.assertEqual(veh["idConductor"], "cond-1")
        self.assertIn("Conductor 2", resultado["mensaje"])

    def test_solicitante_seguridad_desvincula_sin_ser_tenedor(self):
        resultado, mocks = self._desvincular(id_tenedor="intruso", slot=1, solicitante="Diana Seguridad")
        veh = self.vehiculos.find_one({"placa": "XYZ789"})
        self.assertIsNone(veh["idConductor"])
        self.assertEqual(mocks[-1].call_args.args[3], "seguridad")

    def test_desvincular_slot_vacio_rechaza(self):
        veh = self.vehiculos.documents[0]
        veh["idConductor2"] = None
        veh["invitacionConductor2"] = None
        from contextlib import ExitStack
        with ExitStack() as stack:
            for p in self.patches:
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                self._desvincular_interno(slot=2)
        self.assertEqual(ctx.exception.status_code, 400)

    def _desvincular_interno(self, **overrides):
        import asyncio
        from rutas.conductores import DesvincularConductorInput
        datos = DesvincularConductorInput(id_tenedor="ten-1", placa="XYZ789", **overrides)
        return asyncio.run(conductores.desvincular_conductor(datos))


def _vehiculo_docs_completos():
    """Vehículo con TODOS los documentos del gate al día (solo slot 1)."""
    llenos = {
        f: f"Vehiculos/XYZ789/x/{f}.pdf"
        for f in [
            "tarjetaPropiedad", "tarjetaPropiedadReverso", "soat",
            "revisionTecnomecanica", "documentoIdentidadConductor",
            "documentoIdentidadConductorReverso", "documentoIdentidadPropietario",
            "documentoIdentidadPropietarioReverso", "documentoIdentidadTenedor",
            "documentoIdentidadTenedorReverso", "licencia", "licenciaReverso",
            "planillaEpsArl", "condFoto", "condCertificacionBancaria",
            "tenedCertificacionBancaria", "documentoAcreditacionTenedor", "rutTenedor",
        ]
    }
    fotos = {"fotosVehiculo": [
        {"tipo": t, "ruta": f"x/{t}.webp"}
        for t in vehiculos.ANGULOS_FOTO_VEHICULO
    ]}
    doc = _vehiculo_base(**llenos, **fotos)
    return doc


class GateConductor2Tests(unittest.TestCase):
    def test_sin_conductor2_no_exige_sus_docs(self):
        faltantes = vehiculos._documentos_faltantes(_vehiculo_docs_completos())
        self.assertEqual(faltantes, [])

    def test_con_conductor2_exige_los_seis(self):
        doc = _vehiculo_docs_completos()
        doc["idConductor2"] = "cond-2"
        faltantes = vehiculos._documentos_faltantes(doc)
        self.assertEqual(
            sorted(faltantes),
            sorted(vehiculos.DOCUMENTOS_REQUERIDOS_CONDUCTOR2),
        )

    def test_docs_del_conductor2_completos_pasan(self):
        doc = _vehiculo_docs_completos()
        doc["idConductor2"] = "cond-2"
        for campo in vehiculos.DOCUMENTOS_REQUERIDOS_CONDUCTOR2:
            doc[campo] = f"Vehiculos/XYZ789/x/{campo}.pdf"
        self.assertEqual(vehiculos._documentos_faltantes(doc), [])


class FirmaSlot2Tests(unittest.TestCase):
    def _vehiculo(self, **extra):
        doc = _vehiculo_docs_completos()
        doc.update({
            "firmaUrl": "ruta/firma.webp",
            "firmaEvidencia": {"hash_datos": vehiculos._hash_datos_firmados(doc), "version": 1},
        })
        doc.update(extra)
        return doc

    def test_llenar_cond2_no_rompe_hash_slot1(self):
        """REGRESIÓN CRÍTICA: la HV del 2º es volátil para la firma del 1º."""
        doc = self._vehiculo()
        hash_antes = doc["firmaEvidencia"]["hash_datos"]
        doc["cond2Nombres"] = "BETO"
        doc["cond2CedulaCiudadania"] = "222"
        doc["licencia2"] = "ruta/licencia2.pdf"
        doc["idConductor2"] = "cond-2"
        self.assertEqual(vehiculos._hash_datos_firmados(doc), hash_antes)

    def test_hash_slot2_amarra_placa_y_datos(self):
        doc = self._vehiculo(cond2Nombres="BETO", cond2CedulaCiudadania="222")
        h1 = vehiculos._hash_datos_firmados_slot2(doc)
        # Cambiar un dato del 2º cambia SU hash…
        doc["cond2Nombres"] = "OTRO"
        self.assertNotEqual(vehiculos._hash_datos_firmados_slot2(doc), h1)
        # …pero no toca el hash del slot 1.
        self.assertEqual(vehiculos._hash_datos_firmados_slot2(
            self._vehiculo(cond2Nombres="BETO", cond2CedulaCiudadania="222")), h1)

    def test_verificar_firma_por_slot(self):
        import asyncio
        doc = self._vehiculo(
            idConductor2="cond-2", cond2Nombres="BETO", cond2CedulaCiudadania="222",
            firmaUrl2="ruta/firma2.webp",
        )
        hash2 = vehiculos._hash_datos_firmados_slot2(doc)
        firmas = FakeColeccion([
            {"placa": "XYZ789", "version": 1, "slot": 1,
             "hash_datos": doc["firmaEvidencia"]["hash_datos"], "firmado_en": 1},
            {"placa": "XYZ789", "version": 1, "slot": 2,
             "hash_datos": hash2, "firmado_en": 2},
        ])
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([doc])), \
             patch.object(vehiculos, "coleccion_firmas", firmas):
            res1 = asyncio.run(vehiculos.verificar_firma("XYZ789", slot=1))
            res2 = asyncio.run(vehiculos.verificar_firma("XYZ789", slot=2))
        self.assertTrue(res1["coincide"])
        self.assertTrue(res2["coincide"])
        # Cambiar un dato del 2º solo rompe la firma del 2º.
        doc["cond2Nombres"] = "CAMBIADO"
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([doc])), \
             patch.object(vehiculos, "coleccion_firmas", firmas):
            res1b = asyncio.run(vehiculos.verificar_firma("XYZ789", slot=1))
            res2b = asyncio.run(vehiculos.verificar_firma("XYZ789", slot=2))
        self.assertTrue(res1b["coincide"])
        self.assertFalse(res2b["coincide"])

    def test_firmas_historicas_sin_slot_leen_como_slot1(self):
        import asyncio
        doc = self._vehiculo()
        firmas = FakeColeccion([
            # Histórico SIN campo slot (todos los firmados antes de 2026-10-14).
            {"placa": "XYZ789", "version": 1,
             "hash_datos": doc["firmaEvidencia"]["hash_datos"], "firmado_en": 1},
        ])
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([doc])), \
             patch.object(vehiculos, "coleccion_firmas", firmas):
            res = asyncio.run(vehiculos.verificar_firma("XYZ789", slot=1))
            self.assertTrue(res["coincide"])
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(vehiculos.verificar_firma("XYZ789", slot=2))
        self.assertEqual(ctx.exception.status_code, 404)


class EstudiosConductor2Tests(unittest.TestCase):
    def test_conductor2_es_sujeto(self):
        sujetos = estudios_automaticos.sujetos_estudio({
            "placa": "XYZ789", "condCedulaCiudadania": "111",
            "cond2CedulaCiudadania": "222", "propDocumento": "333",
        })
        personas = [s for s in sujetos if s.get("tipo") == "persona"]
        roles = {s["cedula"]: s["roles"] for s in personas}
        self.assertIn("conductor2", roles.get("222", []))

    def test_dedup_combina_roles(self):
        """Misma persona como conductor 1 y 2 → un solo estudio, roles combinados."""
        sujetos = estudios_automaticos.sujetos_estudio({
            "placa": "XYZ789", "condCedulaCiudadania": "111",
            "cond2CedulaCiudadania": "111",
        })
        personas = [s for s in sujetos if s.get("tipo") == "persona"
                    and s["cedula"] == "111"]
        self.assertEqual(len(personas), 1)
        self.assertIn("conductor", personas[0]["roles"])
        self.assertIn("conductor2", personas[0]["roles"])

    def test_sin_cedula2_no_agrega_sujeto(self):
        sujetos = estudios_automaticos.sujetos_estudio({
            "placa": "XYZ789", "condCedulaCiudadania": "111",
        })
        personas = [s for s in sujetos if s.get("tipo") == "persona"]
        self.assertEqual(len(personas), 1)


if __name__ == "__main__":
    unittest.main()
